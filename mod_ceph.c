/* 
 * File:   mod_ceph.c
 * Author: Hoang Tien
 * 
 * Ceph plugin for lighttpd
 *      
 * Created on December 16, 2013, 3:43 PM
 */


#include "base.h"
#include "log.h"
#include "buffer.h"

#include "plugin.h"

#include "stat_cache.h"
#include "etag.h"
#include "http_chunk.h"
#include "response.h"
#include "network.h"

#include <rados/librados.h>

#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <malloc.h>

/* plugin config for all request/connections */

typedef struct {
    array *exclude_ext;
    unsigned short etags_used;
    buffer *config_file;
    buffer *pool_name;
    int block_size; //KB
} plugin_config;

typedef struct {
    PLUGIN_DATA;

    buffer *range_buf;

    plugin_config **config_storage;

    plugin_config conf;

} plugin_data;

typedef struct {
    int is_block_mode;
    size_t num_of_blocks;
    uint64_t next_block;

    buffer* object_id;
} handler_ctx;

rados_t cluster;
rados_ioctx_t ioctx;

static handler_ctx* handler_ctx_init(connection* con) {
    handler_ctx *hctx;

    hctx = calloc(1, sizeof (*hctx));
    hctx->next_block = 0;
    hctx->is_block_mode = 0;
    hctx->num_of_blocks = 0;

    hctx->object_id = buffer_init();
    buffer_append_string_buffer(hctx->object_id, con->request.http_host);
    buffer_append_string_buffer(hctx->object_id, con->request.uri);

    return hctx;
}

static void handler_ctx_free(handler_ctx* hctx) {
    if (hctx) {
        buffer_free(hctx->object_id);
        free(hctx);
    }
}

/* init the plugin data */
INIT_FUNC(mod_ceph_init) {
    plugin_data *p;

    p = calloc(1, sizeof (*p));

    p->range_buf = buffer_init();

    return p;
}

/* detroy the plugin data */
FREE_FUNC(mod_ceph_free) {
    plugin_data *p = p_d;

    UNUSED(srv);

    if (ioctx) rados_ioctx_destroy(ioctx);
    if (cluster) rados_shutdown(cluster);

    if (!p) return HANDLER_GO_ON;

    if (p->config_storage) {
        size_t i;
        for (i = 0; i < srv->config_context->used; i++) {
            plugin_config *s = p->config_storage[i];

            array_free(s->exclude_ext);
            buffer_free(s->config_file);
            buffer_free(s->pool_name);

            free(s);
        }
        free(p->config_storage);
    }
    buffer_free(p->range_buf);

    free(p);

    return HANDLER_GO_ON;
}

/* handle plugin config and check values */

SETDEFAULTS_FUNC(mod_ceph_set_defaults) {

    plugin_data *p = p_d;
    size_t i = 0;

    config_values_t cv[] = {
        { "ceph.exclude-extensions", NULL, T_CONFIG_ARRAY, T_CONFIG_SCOPE_CONNECTION}, /* 0 */
        { "ceph.etags", NULL, T_CONFIG_BOOLEAN, T_CONFIG_SCOPE_CONNECTION}, /* 1 */
        { "ceph.config-file", NULL, T_CONFIG_STRING, T_CONFIG_SCOPE_CONNECTION}, /* 2 */
        { "ceph.pool", NULL, T_CONFIG_STRING, T_CONFIG_SCOPE_CONNECTION}, /* 3 */
        { "ceph.block-size", NULL, T_CONFIG_INT, T_CONFIG_SCOPE_CONNECTION}, /* 4 */
        { NULL, NULL, T_CONFIG_UNSET, T_CONFIG_SCOPE_UNSET}
    };

    if (!p) return HANDLER_ERROR;

    p->config_storage = calloc(1, srv->config_context->used * sizeof (specific_config *));

    for (i = 0; i < srv->config_context->used; i++) {
        plugin_config *s;

        s = calloc(1, sizeof (plugin_config));
        s->exclude_ext = array_init();
        s->etags_used = 1;
        s->config_file = buffer_init();
        s->pool_name = buffer_init();
        s->block_size = 1024;

        cv[0].destination = s->exclude_ext;
        cv[1].destination = &(s->etags_used);
        cv[2].destination = s->config_file;
        cv[3].destination = s->pool_name;
        cv[4].destination = &(s->block_size);

        p->config_storage[i] = s;

        if (0 != config_insert_values_global(srv, ((data_config *) srv->config_context->data[i])->value, cv)) {
            return HANDLER_ERROR;
        }
    }

    return HANDLER_GO_ON;
}

#define PATCH(x) \
	p->conf.x = s->x;

static int mod_ceph_patch_connection(server *srv, connection *con, plugin_data *p) {
    size_t i, j;
    plugin_config *s = p->config_storage[0];

    PATCH(exclude_ext);
    PATCH(etags_used);
    PATCH(config_file);
    PATCH(pool_name);
    PATCH(block_size);

    /* skip the first, the global context */
    for (i = 1; i < srv->config_context->used; i++) {
        data_config *dc = (data_config *) srv->config_context->data[i];
        s = p->config_storage[i];

        /* condition didn't match */
        if (!config_check_cond(srv, con, dc)) continue;

        /* merge config */
        for (j = 0; j < dc->value->used; j++) {
            data_unset *du = dc->value->data[j];

            if (buffer_is_equal_string(du->key, CONST_STR_LEN("ceph.exclude-extensions"))) {
                PATCH(exclude_ext);
            } else if (buffer_is_equal_string(du->key, CONST_STR_LEN("ceph.etags"))) {
                PATCH(etags_used);
            } else if (buffer_is_equal_string(du->key, CONST_STR_LEN("ceph.config-file"))) {
                PATCH(config_file);
            } else if (buffer_is_equal_string(du->key, CONST_STR_LEN("ceph.pool"))) {
                PATCH(pool_name);
            } else if (buffer_is_equal_string(du->key, CONST_STR_LEN("ceph.block-size"))) {
                PATCH(block_size);
            }
        }
    }

    return 0;
}
#undef PATCH

static int http_response_parse_range(server *srv, connection *con, plugin_data *p, const char* object_id, uint64_t object_size) {
    int multipart = 0;
    int error;
    uint64_t start, end;
    const char *s, *minus;
    char *boundary = "fkj49sn38dcn3";
    data_string *ds;
    buffer *content_type = NULL;
    rados_completion_t comp_read;
    char* object_data;

    start = 0;
    end = object_size - 1;

    con->response.content_length = 0;

    if (NULL != (ds = (data_string *) array_get_element(con->response.headers, "Content-Type"))) {
        content_type = ds->value;
    }

    for (s = con->request.http_range, error = 0;
            !error && *s && NULL != (minus = strchr(s, '-'));) {
        char *err;
        off_t la, le;

        if (s == minus) {
            /* -<stop> */

            le = strtoll(s, &err, 10);

            if (le == 0) {
                /* RFC 2616 - 14.35.1 */

                con->http_status = 416;
                error = 1;
            } else if (*err == '\0') {
                /* end */
                s = err;

                end = object_size - 1;
                start = object_size + le;
            } else if (*err == ',') {
                multipart = 1;
                s = err + 1;

                end = object_size - 1;
                start = object_size + le;
            } else {
                error = 1;
            }

        } else if (*(minus + 1) == '\0' || *(minus + 1) == ',') {
            /* <start>- */

            la = strtoll(s, &err, 10);

            if (err == minus) {
                /* ok */

                if (*(err + 1) == '\0') {
                    s = err + 1;

                    end = object_size - 1;
                    start = la;

                } else if (*(err + 1) == ',') {
                    multipart = 1;
                    s = err + 2;

                    end = object_size - 1;
                    start = la;
                } else {
                    error = 1;
                }
            } else {
                /* error */
                error = 1;
            }
        } else {
            /* <start>-<stop> */

            la = strtoll(s, &err, 10);

            if (err == minus) {
                le = strtoll(minus + 1, &err, 10);

                /* RFC 2616 - 14.35.1 */
                if (la > le) {
                    error = 1;
                }

                if (*err == '\0') {
                    /* ok, end*/
                    s = err;

                    end = le;
                    start = la;
                } else if (*err == ',') {
                    multipart = 1;
                    s = err + 1;

                    end = le;
                    start = la;
                } else {
                    /* error */

                    error = 1;
                }
            } else {
                /* error */

                error = 1;
            }
        }

        if (!error) {
            //if (start < 0) start = 0;

            /* RFC 2616 - 14.35.1 */
            if (end > object_size - 1) end = object_size - 1;

            if (start > object_size - 1) {
                error = 1;

                con->http_status = 416;
            }
        }

        if (!error) {
            if (multipart) {
                /* write boundary-header */
                buffer *b;

                b = chunkqueue_get_append_buffer(con->write_queue);

                buffer_copy_string_len(b, CONST_STR_LEN("\r\n--"));
                buffer_append_string(b, boundary);

                /* write Content-Range */
                buffer_append_string_len(b, CONST_STR_LEN("\r\nContent-Range: bytes "));
                buffer_append_off_t(b, start);
                buffer_append_string_len(b, CONST_STR_LEN("-"));
                buffer_append_off_t(b, end);
                buffer_append_string_len(b, CONST_STR_LEN("/"));
                buffer_append_off_t(b, object_size);

                buffer_append_string_len(b, CONST_STR_LEN("\r\nContent-Type: "));
                buffer_append_string_buffer(b, content_type);

                /* write END-OF-HEADER */
                buffer_append_string_len(b, CONST_STR_LEN("\r\n\r\n"));

                con->response.content_length += b->used - 1;

            }

            /* get object data*/
            object_data = calloc(object_size, sizeof (*object_data));
            if (object_data == NULL) {
                con->http_status = 500;

                if (con->conf.log_request_handling) log_error_write(srv, __FILE__, __LINE__, "ss", "-- error on allocating object_data: ", strerror(errno));

                return 1;
            }

            if (rados_aio_create_completion(NULL, NULL, NULL, &comp_read) < 0) {
                con->http_status = 500;

                if (con->conf.log_request_handling) log_error_write(srv, __FILE__, __LINE__, "ss", "-- error on rados_aio_create_completion: ", strerror(errno));

                rados_ioctx_destroy(ioctx);
                rados_shutdown(cluster);

                return 1;
            }

            if (rados_aio_read(ioctx, object_id, comp_read, object_data, end - start + 1, start) < 0) {
                con->http_status = 500;

                if (con->conf.log_request_handling) log_error_write(srv, __FILE__, __LINE__, "ss", "-- error on rados_aio_read: ", strerror(errno));


                free(object_data);
                rados_aio_release(comp_read);
                rados_ioctx_destroy(ioctx);
                rados_shutdown(cluster);

                return 1;
            }

            rados_aio_wait_for_complete(comp_read);

            //chunkqueue_append_file(con->write_queue, con->physical.path, start, end - start + 1);
            chunkqueue_append_mem(con->write_queue, object_data, rados_aio_get_return_value(comp_read));
            con->response.content_length += end - start + 1;
        }
    }

    /* something went wrong */
    if (error) return -1;

    if (multipart) {
        /* add boundary end */
        buffer *b;

        b = chunkqueue_get_append_buffer(con->write_queue);

        buffer_copy_string_len(b, "\r\n--", 4);
        buffer_append_string(b, boundary);
        buffer_append_string_len(b, "--\r\n", 4);

        con->response.content_length += b->used - 1;

        /* set header-fields */

        buffer_copy_string_len(p->range_buf, CONST_STR_LEN("multipart/byteranges; boundary="));
        buffer_append_string(p->range_buf, boundary);

        /* overwrite content-type */
        response_header_overwrite(srv, con, CONST_STR_LEN("Content-Type"), CONST_BUF_LEN(p->range_buf));
    } else {
        /* add Content-Range-header */

        buffer_copy_string_len(p->range_buf, CONST_STR_LEN("bytes "));
        buffer_append_off_t(p->range_buf, start);
        buffer_append_string_len(p->range_buf, CONST_STR_LEN("-"));
        buffer_append_off_t(p->range_buf, end);
        buffer_append_string_len(p->range_buf, CONST_STR_LEN("/"));
        buffer_append_off_t(p->range_buf, object_size);

        response_header_insert(srv, con, CONST_STR_LEN("Content-Range"), CONST_BUF_LEN(p->range_buf));
    }

    /* ok, the file is set-up */
    return 0;
}

URIHANDLER_FUNC(mod_ceph_subrequest) {
    fprintf(stderr, "ceph subr\n");

    plugin_data *p = p_d;
    handler_ctx *hctx;
    size_t k;
    int s_len;
    buffer *mtime = NULL;
    data_string *ds;
    int allow_caching = 1;
    int ret;

    rados_completion_t comp_stat, comp_read;
    uint64_t object_size = 0;
    uint64_t block_size = 0;
    time_t object_mtime = 0;

    char* object_data;
    struct stat object_stat;

    /* someone else has done a decision for us */
//    if (con->http_status != 0) {
//        con->mode = DIRECT;
//        return HANDLER_GO_ON;
//    }
    if (con->uri.path->used == 0) {
        con->mode = DIRECT;
        return HANDLER_GO_ON;
    }
    if (con->physical.path->used == 0) {
        con->mode = DIRECT;
        return HANDLER_GO_ON;
    }

    /* someone else has handled this request */
    if (con->mode != p->id) {
        con->mode = DIRECT;
        return HANDLER_GO_ON;
    }

    /* we only handle GET, POST and HEAD */
    switch (con->request.http_method) {
        case HTTP_METHOD_GET:
        case HTTP_METHOD_POST:
        case HTTP_METHOD_HEAD:
            break;
        default:
            con->mode = DIRECT;
            return HANDLER_GO_ON;
    }

    hctx = con->plugin_ctx[p->id];
    if (hctx == NULL) {
        hctx = handler_ctx_init(con);
        con->plugin_ctx[p->id] = hctx;
    }

    if (!hctx->is_block_mode) {
        mod_ceph_patch_connection(srv, con, p);

        s_len = con->uri.path->used - 1;

        /* ignore certain extensions */
        for (k = 0; k < p->conf.exclude_ext->used; k++) {
            ds = (data_string *) p->conf.exclude_ext->data[k];

            if (ds->value->used == 0) continue;

            if (buffer_is_equal_right_len(con->physical.path, ds->value, ds->value->used - 1)) {
                con->mode = DIRECT;
                handler_ctx_free(hctx);
                return HANDLER_GO_ON;
            }
        }


        if (con->conf.log_request_handling) {
            log_error_write(srv, __FILE__, __LINE__, "s", "-- handling file as object from Ceph");
        }


        /* rados init */
        if (!cluster) {
            if (rados_create(&cluster, NULL) < 0) {
                con->http_status = 500;

                if (con->conf.log_request_handling) log_error_write(srv, __FILE__, __LINE__, "ss", "-- error on rados_create: ", strerror(errno));

                handler_ctx_free(hctx);
                con->mode = DIRECT;
                return HANDLER_FINISHED;
            }
            // fprintf(stderr, "rados_create\n");

            if (rados_conf_read_file(cluster, p->conf.config_file->ptr) < 0) {
                con->http_status = 500;

                if (con->conf.log_request_handling) log_error_write(srv, __FILE__, __LINE__, "ss", "-- error on rados_conf_read_file: ", strerror(errno));

                handler_ctx_free(hctx);
                rados_shutdown(cluster);

                con->mode = DIRECT;
                return HANDLER_FINISHED;
            }
            //fprintf(stderr, "rados_conf_read_file\n");

            if (rados_connect(cluster) < 0) {
                con->http_status = 500;

                if (con->conf.log_request_handling) log_error_write(srv, __FILE__, __LINE__, "ss", "-- error on rados_connect: ", strerror(errno));

                handler_ctx_free(hctx);
                rados_shutdown(cluster);

                con->mode = DIRECT;
                return HANDLER_FINISHED;
            }
            //fprintf(stderr, "rados_connect\n");

            if (rados_ioctx_create(cluster, p->conf.pool_name->ptr, &ioctx) < 0) {
                con->http_status = 500;

                if (con->conf.log_request_handling) log_error_write(srv, __FILE__, __LINE__, "ss", "-- error on rados_ioctx_create: ", strerror(errno));

                handler_ctx_free(hctx);
                rados_shutdown(cluster);

                con->mode = DIRECT;
                return HANDLER_FINISHED;
            }

            //fprintf(stderr, "rados_ioctx_create\n");
        }
        /*-----*/

        /* get object stats */
        if (rados_aio_create_completion(NULL, NULL, NULL, &comp_stat) < 0) {
            con->http_status = 500;

            if (con->conf.log_request_handling) log_error_write(srv, __FILE__, __LINE__, "ss", "-- error on rados_aio_create_completion: ", strerror(errno));

            handler_ctx_free(hctx);
            rados_ioctx_destroy(ioctx);
            rados_shutdown(cluster);

            con->mode = DIRECT;
            return HANDLER_FINISHED;
        }
        //fprintf(stderr, "rados_aio_create_completion\n");

        if (rados_aio_stat(ioctx, hctx->object_id->ptr, comp_stat, &object_size, &object_mtime) < 0) {
            con->http_status = 500;

            if (con->conf.log_request_handling) log_error_write(srv, __FILE__, __LINE__, "ss", "-- error on rados_aio_stat: ", strerror(errno));

            handler_ctx_free(hctx);
            rados_aio_release(comp_stat);
            rados_ioctx_destroy(ioctx);
            rados_shutdown(cluster);

            con->mode = DIRECT;
            return HANDLER_FINISHED;
        }
        //fprintf(stderr, "rados_aio_stat  %ld %ld\n", object_size, object_mtime);

        rados_aio_wait_for_complete(comp_stat);
        rados_aio_release(comp_stat);
        //fprintf(stderr, "rados_aio_wait_for_complete\n");
        /*----*/

        /* Not found */
        if (object_size == 0) {
            con->http_status = 404;

            if (con->conf.log_file_not_found) {
                log_error_write(srv, __FILE__, __LINE__, "ss",
                        "Ceph object not found: ", hctx->object_id->ptr);
            }

            handler_ctx_free(hctx);

            con->mode = DIRECT;
            return HANDLER_FINISHED;
        }


        /* mod_compress might set several data directly, don't overwrite them */

        /* set response content-type, if not set already */

        if (NULL == array_get_element(con->response.headers, "Content-Type")) {
            for (k = 0; k < con->conf.mimetypes->used; k++) {
                ds = (data_string *) con->conf.mimetypes->data[k];
                buffer *type = ds->key;

                if (type->used == 0) continue;

                /* check if the right side is the same */
                if (type->used > con->physical.path->used) continue;

                if (0 == strncasecmp(con->physical.path->ptr + con->physical.path->used - type->used, type->ptr, type->used - 1)) {
                    response_header_overwrite(srv, con, CONST_STR_LEN("Content-Type"), CONST_BUF_LEN(ds->value));
                    break;
                }
            }
            if (NULL == array_get_element(con->response.headers, "Content-Type")) {
                response_header_overwrite(srv, con, CONST_STR_LEN("Content-Type"), CONST_STR_LEN("application/octet-stream"));
                allow_caching = 0;
            }
        }


        /******************/


        if (con->conf.range_requests) {
            response_header_overwrite(srv, con, CONST_STR_LEN("Accept-Ranges"), CONST_STR_LEN("bytes"));
        }

        if (allow_caching) {
            if (p->conf.etags_used && con->etag_flags != 0) {
                if (NULL == array_get_element(con->response.headers, "ETag")) {
                    /* generate e-tag */
                    object_stat.st_size = object_size;
                    object_stat.st_mtime = object_mtime;
                    etag_create(con->physical.etag, &object_stat, con->etag_flags);

                    response_header_overwrite(srv, con, CONST_STR_LEN("ETag"), CONST_BUF_LEN(con->physical.etag));
                }
            }

            /* prepare header */
            if (NULL == (ds = (data_string *) array_get_element(con->response.headers, "Last-Modified"))) {
                mtime = strftime_cache_get(srv, object_mtime);
                response_header_overwrite(srv, con, CONST_STR_LEN("Last-Modified"), CONST_BUF_LEN(mtime));
            } else {
                mtime = ds->value;
            }

            if (HANDLER_FINISHED == http_response_handle_cachable(srv, con, mtime)) {

                handler_ctx_free(hctx);

                con->mode = DIRECT;
                return HANDLER_FINISHED;
            }
        }

        if (con->request.http_range && con->conf.range_requests) {
            int do_range_request = 1;
            /* check if we have a conditional GET */

            if (NULL != (ds = (data_string *) array_get_element(con->request.headers, "If-Range"))) {
                /* if the value is the same as our ETag, we do a Range-request,
                 * otherwise a full 200 */

                if (ds->value->ptr[0] == '"') {
                    /**
                     * client wants a ETag
                     */
                    if (!con->physical.etag) {
                        do_range_request = 0;
                    } else if (!buffer_is_equal(ds->value, con->physical.etag)) {
                        do_range_request = 0;
                    }
                } else if (!mtime) {
                    /**
                     * we don't have a Last-Modified and can match the If-Range: 
                     *
                     * sending all
                     */
                    do_range_request = 0;
                } else if (!buffer_is_equal(ds->value, mtime)) {
                    do_range_request = 0;
                }
            }

            if (do_range_request) {
                /* content prepared, I'm done */
                con->file_finished = 1;

                if (0 == http_response_parse_range(srv, con, p, hctx->object_id->ptr, object_size)) {
                    con->http_status = 206;
                }

                handler_ctx_free(hctx);

                con->mode = DIRECT;
                return HANDLER_FINISHED;
            }
        }


        if (object_size <= p->conf.block_size * 1024) {
            block_size = object_size;
            hctx->num_of_blocks = 1;

        } else {
            block_size = p->conf.block_size * 1024;
            hctx->is_block_mode = 1;
            hctx->num_of_blocks = (object_size % (p->conf.block_size * 1024)) ?
                    (object_size / (p->conf.block_size * 1024)) + 1 : (object_size / (p->conf.block_size * 1024));
        }
      
        fprintf(stderr,"%ld\n",object_size);
    }

    if (hctx->is_block_mode) {
        fprintf(stderr, "Block mode\n");
        block_size = p->conf.block_size * 1024;
    }


    if (rados_aio_create_completion(NULL, NULL, NULL, &comp_read) < 0) {
        con->http_status = 500;

        if (con->conf.log_request_handling) log_error_write(srv, __FILE__, __LINE__, "ss", "-- error on rados_aio_create_completion: ", strerror(errno));

        handler_ctx_free(hctx);
        rados_ioctx_destroy(ioctx);
        rados_shutdown(cluster);

        con->mode = DIRECT;
        return HANDLER_FINISHED;
    }

    object_data = calloc(block_size, sizeof (*object_data));
    if (object_data == NULL) {
        con->http_status = 500;

        if (con->conf.log_request_handling) log_error_write(srv, __FILE__, __LINE__, "ss", "-- error on allocating object_data: ", strerror(errno));

        handler_ctx_free(hctx);
        rados_aio_release(comp_read);

        con->mode = DIRECT;
        return HANDLER_FINISHED;
    }


    fprintf(stderr, "NBlock %d Offset %d\n", hctx->num_of_blocks, hctx->next_block * block_size);
    if (rados_aio_read(ioctx, hctx->object_id->ptr, comp_read, object_data, block_size, hctx->next_block * block_size) < 0) {
        con->http_status = 500;

        if (con->conf.log_request_handling) log_error_write(srv, __FILE__, __LINE__, "ss", "-- error on rados_aio_read: ", strerror(errno));


        free(object_data);
        handler_ctx_free(hctx);
        rados_aio_release(comp_read);
        rados_ioctx_destroy(ioctx);
        rados_shutdown(cluster);

        con->mode = DIRECT;
        return HANDLER_FINISHED;
    }

    hctx->next_block++;
    rados_aio_wait_for_complete(comp_read);
    /*----End get object data*/


    /* if we are still here, prepare body */

    /* we add it here for all requests
     * the HEAD request will drop it afterwards again
     */

    //http_chunk_append_file(srv, con, con->physical.path, 0, sce->st.st_size);
    con->response.transfer_encoding = HTTP_TRANSFER_ENCODING_CHUNKED;
    http_chunk_append_mem(srv, con, object_data, rados_aio_get_return_value(comp_read)+1);


    if (hctx->num_of_blocks == hctx->next_block) {
        fprintf(stderr, "end\n");
        //http_chunk_append_mem(srv, con, object_data, 0);
        con->file_finished = 1;
        con->http_status = 200;
        con->is_writable=1;
        fprintf(stderr, "finish\n");
        con->mode = DIRECT;
        ret = HANDLER_FINISHED;
        handler_ctx_free(hctx);
    } else {
        fprintf(stderr, "not end\n");
        con->file_finished = 0;
        con->is_writable=1;
        con->http_status = 200;
        ret = HANDLER_COMEBACK;
    }

    //        network_write_chunkqueue(srv, con, con->output_queue);
    //    network_write_chunkqueue(srv, con, con->write_queue);


    free(object_data);
    rados_aio_release(comp_read);
    //con->mode = DIRECT;

    return ret;
}

URIHANDLER_FUNC(mod_ceph_uri_clean) {
    //fprintf(stderr, "uri clean ceph\n");
    plugin_data *p = p_d;

    mod_ceph_patch_connection(srv, con, p);
    con->mode = p->id;


    return HANDLER_GO_ON;
}


/* this function is called at dlopen() time and inits the callbacks */

int mod_ceph_plugin_init(plugin * p);

int mod_ceph_plugin_init(plugin * p) {
    p->version = LIGHTTPD_VERSION_ID;
    p->name = buffer_init_string("ceph");

    p->init = mod_ceph_init;
    p->handle_uri_clean = mod_ceph_uri_clean;
    p->handle_subrequest = mod_ceph_subrequest;
    p->set_defaults = mod_ceph_set_defaults;
    p->cleanup = mod_ceph_free;

    p->data = NULL;

    return 0;
}
