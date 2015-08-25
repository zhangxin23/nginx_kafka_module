#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#include <librdkafka/rdkafka.h>

#define KAFKA_TOPIC_MAXLEN      256
#define KAFKA_BROKER_MAXLEN     512

static char g_broker_list[KAFKA_BROKER_MAXLEN * 2];

static ngx_int_t ngx_http_kafka_init_worker(ngx_cycle_t *cycle);
static void ngx_http_kafka_exit_worker(ngx_cycle_t *cycle);

static void *ngx_http_kafka_create_main_conf(ngx_conf_t *cf);
static void *ngx_http_kafka_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_set_kafka_broker_list(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_set_kafka_topic(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_kafka_handler(ngx_http_request_t *r);
static void ngx_http_kafka_post_callback_handler(ngx_http_request_t *r);

typedef enum {
    ngx_str_push = 0,
    ngx_str_pop = 1
} ngx_str_op;

static void ngx_str_helper(ngx_str_t *str, ngx_str_op op);

typedef struct {
    ngx_array_t	     *meta_brokers;
    rd_kafka_t       *rk;
    rd_kafka_conf_t  *rkc;

    size_t         broker_size;
    size_t         nbrokers;
    ngx_str_t     *brokers;
} ngx_http_kafka_main_conf_t;

typedef struct {
    ngx_log_t  *log;
    ngx_str_t   topic;
    ngx_str_t   broker;

    rd_kafka_topic_t *rkt;
    rd_kafka_topic_conf_t  *rktc;
} ngx_http_kafka_loc_conf_t;

static ngx_command_t ngx_http_kafka_commands[] = {
    {
        ngx_string("kafka.broker.list"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_1MORE,
        ngx_http_set_kafka_broker_list,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_kafka_main_conf_t, meta_brokers),
        NULL },
    {
        ngx_string("kafka.topic"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_http_set_kafka_topic,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_kafka_loc_conf_t, topic),
        NULL },
    ngx_null_command
};

static ngx_http_module_t ngx_http_kafka_module_ctx = {
    NULL,
    NULL,

    ngx_http_kafka_create_main_conf,      
    NULL,

    NULL,
    NULL,

    ngx_http_kafka_create_loc_conf,
    NULL,
};

ngx_module_t ngx_http_kafka_module = {
    NGX_MODULE_V1,
    &ngx_http_kafka_module_ctx,
    ngx_http_kafka_commands,
    NGX_HTTP_MODULE,

    NULL,
    NULL,
    ngx_http_kafka_init_worker,
    NULL,
    NULL,
    ngx_http_kafka_exit_worker,
    NULL,

    NGX_MODULE_V1_PADDING
};

ngx_int_t ngx_str_equal(ngx_str_t *s1, ngx_str_t *s2)
{
    if (s1->len != s1->len) {
        return 0;
    }
    if (ngx_memcmp(s1->data, s2->data, s1->len) != 0) {
        return 0;
    }
    return 1;
}

void *ngx_http_kafka_create_main_conf(ngx_conf_t *cf)
{
    ngx_http_kafka_main_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_kafka_main_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }

    conf->rk = NULL;
    conf->rkc = NULL;

    conf->broker_size = 0;
    conf->nbrokers = 0;
    conf->brokers = NULL;

    return conf;
}

void *ngx_http_kafka_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_kafka_loc_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_kafka_loc_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }
    conf->log = cf->log;
    ngx_str_null(&conf->topic);
    ngx_str_null(&conf->broker);

    return conf;
}

void kafka_callback_handler(rd_kafka_t *rk, void *msg, size_t len, int err, void *opaque, void *msg_opaque)
{
    if (err != 0) {
        ngx_log_error(NGX_LOG_ERR, (ngx_log_t *)msg_opaque, 0, rd_kafka_err2str(err));
    }
}

char *ngx_http_set_kafka_broker_list(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t	*value;
    ngx_uint_t	i;
    char 		*ptr;
    memset(g_broker_list, 0, sizeof(g_broker_list));
    ptr = g_broker_list;

    for (i = 1; i < cf->args->nelts; i++) {
        value = cf->args->elts;
        ngx_str_t url = value[i];
        memcpy(ptr, url.data, url.len);
        ptr += url.len;
        *ptr++ = ',';
    }
    *(ptr - 1) = '\0';

    return NGX_CONF_OK;
}

char *ngx_http_set_kafka_topic(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t   *clcf;
    ngx_http_kafka_loc_conf_t  *local_conf;

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    if (clcf == NULL) {
        return NGX_CONF_ERROR;
    }
    clcf->handler = ngx_http_kafka_handler;

    if (ngx_conf_set_str_slot(cf, cmd, conf) != NGX_CONF_OK) {
        return NGX_CONF_ERROR;
    }

    local_conf = conf;

    local_conf->rktc = rd_kafka_topic_conf_new();

    return NGX_CONF_OK;
}

static ngx_int_t ngx_http_kafka_handler(ngx_http_request_t *r)
{
    ngx_int_t  rv;

    if (!(r->method & NGX_HTTP_POST)) {
        return NGX_HTTP_NOT_ALLOWED;
    }

    rv = ngx_http_read_client_request_body(r, ngx_http_kafka_post_callback_handler);
    if (rv >= NGX_HTTP_SPECIAL_RESPONSE) {
        return rv;
    }

    return NGX_DONE;
}

static void ngx_http_kafka_post_callback_handler(ngx_http_request_t *r)
{
    static const char rc[] = "{\"msg\":\"SUCCESS\",\"code\":0}\n";

    int          nbufs;
    u_char    *msg;
    size_t      len;
    ngx_buf_t         *buf;
    ngx_chain_t      out;
    ngx_chain_t      *cl, *in;
    ngx_http_request_body_t         *body;
    ngx_http_kafka_main_conf_t    *main_conf;
    ngx_http_kafka_loc_conf_t       *local_conf;

    body = r->request_body;
    if (body == NULL || body->bufs == NULL) {
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
    }

    main_conf = NULL;

    len = 0;
    nbufs = 0;
    in = body->bufs;
    for (cl = in; cl != NULL; cl = cl->next) {
        nbufs++;
        len += (size_t)(cl->buf->last - cl->buf->pos);
    }

    if (nbufs == 0) {
        goto end;
    }

    if (nbufs == 1 && ngx_buf_in_memory(in->buf)) {
        msg = in->buf->pos;
    } else {
        if ((msg = ngx_pnalloc(r->pool, len)) == NULL) {
            ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
            return;
        }

        for (cl = in; cl != NULL; cl = cl->next) {
            if (ngx_buf_in_memory(cl->buf)) {
                msg = ngx_copy(msg, cl->buf->pos, cl->buf->last - cl->buf->pos);
            } else {
                ngx_log_error(NGX_LOG_NOTICE, r->connection->log, 0,
                        "ngx_http_kafka_handler cannot handler in-file-post-buf");
                goto end;
            }
        }
        msg -= len;
    }

    main_conf = ngx_http_get_module_main_conf(r, ngx_http_kafka_module);
    local_conf = ngx_http_get_module_loc_conf(r, ngx_http_kafka_module);
    if (local_conf->rkt == NULL) {
        ngx_str_helper(&local_conf->topic, ngx_str_push);
        local_conf->rkt = rd_kafka_topic_new(main_conf->rk,
                (const char *)local_conf->topic.data, local_conf->rktc);
        ngx_str_helper(&local_conf->topic, ngx_str_pop);
    }

    rd_kafka_produce(local_conf->rkt, RD_KAFKA_PARTITION_UA, 
            RD_KAFKA_MSG_F_COPY, (void *)msg, len, NULL, 0, local_conf->log);

end:
    buf = ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
    out.buf = buf;
    out.next = NULL;
    buf->pos = (u_char *)rc;
    buf->last = (u_char *)rc + sizeof(rc) - 1;
    buf->memory = 1;
    buf->last_buf = 1;

    ngx_str_set(&(r->headers_out.content_type), "text/html");
    r->headers_out.status = NGX_HTTP_OK;
    r->headers_out.content_length_n = sizeof(rc) - 1;
    ngx_http_send_header(r);
    ngx_http_output_filter(r, &out);
    ngx_http_finalize_request(r, NGX_OK);

    if (main_conf != NULL) {
        rd_kafka_poll(main_conf->rk, 0);
    }
}

ngx_int_t ngx_http_kafka_init_worker(ngx_cycle_t *cycle)
{
    ngx_http_kafka_main_conf_t  *main_conf;

    main_conf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_kafka_module);

    main_conf->rkc = rd_kafka_conf_new();
    rd_kafka_conf_set_dr_cb(main_conf->rkc, kafka_callback_handler);
    rd_kafka_conf_set(main_conf->rkc, "metadata.broker.list", g_broker_list, NULL, 0);
    main_conf->rk = rd_kafka_new(RD_KAFKA_PRODUCER, main_conf->rkc, NULL, 0);

    return 0;
}

void ngx_http_kafka_exit_worker(ngx_cycle_t *cycle)
{
    ngx_http_kafka_main_conf_t  *main_conf;

    main_conf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_kafka_module);

    rd_kafka_destroy(main_conf->rk);
}

void ngx_str_helper(ngx_str_t *str, ngx_str_op op)
{
    static char backup;

    switch (op) {
        case ngx_str_push:
            backup = str->data[str->len];
            str->data[str->len] = 0;
            break;
        case ngx_str_pop:
            str->data[str->len] = backup;
            break;
        default:
            ngx_abort();
    }
}
