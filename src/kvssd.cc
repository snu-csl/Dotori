#include "libforestdb/forestdb.h"
#include "log_message.h"

#include "filemgr_ops_kvssd.h"
#include "kadi.h"

#include "kvssdmgr.h"

#define stringify(name) # name
#define kvs_errstr(name) (errortable[name])
const char* errortable[] = {
  stringify(KVS_SUCCESS),
  stringify(KVS_ERR_BUFFER_SMALL),
  stringify(KVS_ERR_DEV_CAPAPCITY),
  stringify(KVS_ERR_DEV_NOT_EXIST),
  stringify(KVS_ERR_KS_CAPACITY),
  stringify(KVS_ERR_KS_EXIST),
  stringify(KVS_ERR_KS_INDEX),
  stringify(KVS_ERR_KS_NAME),
  stringify(KVS_ERR_KS_NOT_EXIST),
  stringify(KVS_ERR_KS_NOT_OPEN),
  stringify(KVS_ERR_KS_OPEN),
  stringify(KVS_ERR_ITERATOR_FILTER_INVALID),
  stringify(KVS_ERR_ITERATOR_MAX),
  stringify(KVS_ERR_ITERATOR_NOT_EXIST),
  stringify(KVS_ERR_ITERATOR_OPEN),
  stringify(KVS_ERR_KEY_LENGTH_INVALID),
  stringify(KVS_ERR_KEY_NOT_EXIST),
  stringify(KVS_ERR_OPTION_INVALID),
  stringify(KVS_ERR_PARAM_INVALID),
  stringify(KVS_ERR_SYS_IO),
  stringify(KVS_ERR_VALUE_LENGTH_INVALID),
  stringify(KVS_ERR_VALUE_OFFSET_INVALID),
  stringify(KVS_ERR_VALUE_OFFSET_MISALIGNED),
  stringify(KVS_ERR_VALUE_UPDATE_NOT_ALLOWED),
  stringify(KVS_ERR_DEV_NOT_OPENED),
};


struct aio_cmd_ctx {
    enum aio_event_type type;
    int index;
    struct nvme_passthru_kv_cmd cmd;
    void (*cb)(struct nvme_passthru_kv_cmd, void*);
    void *args;
    bool log;
    uint16_t log_num;
    key *k;
};


uint32_t N_AIO = 1;
uint32_t QD = 64;

int g_fd = -1;
int g_nsid = -1;

std::string MILESTONE_K = "@milestone";

void set_n_aio(uint32_t n) {
    N_AIO = n;
}

void set_qd(uint32_t qd) {
    QD = qd;
}

uint16_t _klen_from_cmd(struct nvme_passthru_kv_cmd *cmd) {
    return cmd->cdw11 + 1;
}

void vernum_to_prefix(uint64_t vernum, unsigned char *out_key, uint32_t klen)
{
    for(unsigned int i = 0; i < klen; i++) {
        out_key[i] = vernum >> (8 * (klen - 1 - i)) & 0xFF;
    }
}

uint8_t initialized = 0;

thread_local char readBuffer[MAX_VALUE_SIZE];

atomic_uint8_t aio_w_shutdown;
pthread_t *aio_w_pt;

/*
 * The async I/O routines in this file are a (poorly)
 * adapted version of the nice async I/O code in https://github.com/BLepers/KVell.
 */

struct aio_w {
    uint32_t id;
    uint32_t qd;

    struct aio_event **events;
    // Number of requests enqueued or in the process of being enqueued
    volatile size_t buffered_callbacks_idx;               
    // Number of requests fully enqueued
    volatile size_t sent_callbacks;                      
    // Number of requests fully submitted and processed on disk
    volatile size_t processed_callbacks;                  
    // Maximum number of enqueued requests
    size_t max_pending_callbacks;                         

    volatile size_t sent_io;
    volatile size_t processed_io;
    size_t max_pending_io;
    size_t ios_sent_to_disk;  

    struct nvme_aioctx kvssd_aioctx;
    struct aio_cmd_ctx *cmd_ctxs;
    int fd, ns, efd;
    fd_set rfds;
} *workers;

bool _buffer_check(struct aio_w *w, size_t sent) {
    if(sent - __atomic_load_n(&w->processed_io,  __ATOMIC_SEQ_CST) 
       >= w->max_pending_io) {
        printf("Sent %lu ios, processed %lu (> %lu waiting), "
                "IO buffer is too full!\n", 
                sent, w->processed_io, w->max_pending_io);
        return true;
    }
    return false;
}

int _io_pending(struct aio_w *w) {
    return __atomic_load_n(&w->sent_io,  __ATOMIC_SEQ_CST)
           - w->processed_io;
}

/*
 * We choose to use the raw KVSSD NVMe commands for sync I/O here as they
 * are significantly faster than using Samsung's KVAPI library due
 * to the library's design for async I/O throughput.
 */

void _build_cmd(struct aio_event *event, struct nvme_passthru_kv_cmd *cmd,
                int reqid, int ns, struct nvme_aioctx aioctx)
{ 
    memset(cmd, 0, sizeof(struct nvme_passthru_kv_cmd));
    enum nvme_kv_delete_option option = DELETE_OPTION_NOTHING;

    switch(event->type) {
        case AIO_STORE:
        case LOG_WRITE:
            cmd->opcode = nvme_cmd_kv_store;
            break;
        case AIO_RETRIEVE:
        case AIO_RETRIEVE_KEY:
        case LOG_READ:
        case RECOVERY_READ:
            cmd->opcode = nvme_cmd_kv_retrieve;
            break;
		case AIO_DELETE:
		case AIO_DELETE_LOG:
		case AIO_DELETE_VNUM:
            cmd->cdw4 = option;
			cmd->opcode = nvme_cmd_kv_delete;
			break;
        case DELETE_WITH_CHECK:
            option = DELETE_OPTION_CHECK_KEY_EXIST;
            cmd->cdw4 = option;
            cmd->opcode = nvme_cmd_kv_delete;
            break;
		case LAST:
			return;
        default:
			assert(0);
            return;
    }

    if(event->key->len() > 16) {
        cmd->key_addr = (__u64)event->key->data();
    } else {
        memcpy(cmd->key, event->key->data(), event->key->len());
    }

    cmd->key_length = event->key->len();
    cmd->nsid = ns;
    cmd->data_addr = (__u64) event->buf;
    cmd->data_length = event->len;
    cmd->cdw10 = event->len >>  2;
    cmd->cdw11 = event->key->len() -1;
    cmd->ctxid = aioctx.ctxid;
    cmd->reqid = reqid;
    cmd->cdw3 = 1;
}

void _response_cb(struct nvme_passthru_kv_cmd cmd, void *voidargs)
{
    struct callback_args *kvssd_cb_args;
    kvssd_cb_args = (struct callback_args*)voidargs;

    assert(cmd.result == 0);

    free(kvssd_cb_args);
}

void _clear_ctx(aio_cmd_ctx *ctx)
{
    ctx->k = NULL;
}

void _enqueue_io(struct aio_w *w) {
    size_t pending = __atomic_load_n(&w->sent_io,  __ATOMIC_SEQ_CST)
                     - w->processed_io;

    w->ios_sent_to_disk = 0;
    if(pending == 0) {
        return;
	}

    for(size_t i = 0; i < pending; i++) {
        int ret, reqid = (w->processed_io + i) % w->max_pending_io;
        struct aio_cmd_ctx *ctx = &w->cmd_ctxs[reqid]; 
        assert(ctx->index == reqid);
        ret = ioctl(w->fd, NVME_IOCTL_AIO_CMD, &ctx->cmd);
        if(ret) {
            assert(0);
        }
        w->ios_sent_to_disk++;
    }
}

void _cb(struct aio_w *w, struct aio_cmd_ctx ctx)
{
    void (*cb)(struct nvme_passthru_kv_cmd, void*);

    cb = ctx.cb;
    cb(ctx.cmd, ctx.args);
    delete ctx.k;
}

void _complete_io(struct aio_w *w) {
    int nr_changedfds, read_s, check_nr;
    int efd = w->efd;
    uint64_t eftd_ctx = 0;
    fd_set rfds = w->rfds;
    struct nvme_aioevents aioevents;
    int completed = 0;

    if(w->ios_sent_to_disk == 0) {
        return;
    }

again:
    nr_changedfds = select(efd + 1, &rfds, NULL, NULL, NULL);
    if(nr_changedfds >= 0) {
        read_s = read(efd, &eftd_ctx, sizeof(uint64_t));

        while (eftd_ctx) {
            check_nr = eftd_ctx;

            //if (check_nr > MAX_AIO_EVENTS) {
            //    check_nr = MAX_AIO_EVENTS;
            //}

            if (check_nr > w->max_pending_io) {
                assert(0);
            }

            aioevents.nr = check_nr;
            aioevents.ctxid = w->kvssd_aioctx.ctxid;

            if (ioctl(w->fd, NVME_IOCTL_GET_AIOEVENT, &aioevents) < 0) {
                fprintf(stderr, "fail to read IOEVETS %s\n", strerror(errno));
                assert(0);
            }

            eftd_ctx -= check_nr;

            int prev_reqid = -1;
            for (int i = 0; i < aioevents.nr; i++) {
                /* found request */
                int reqid = aioevents.events[i].reqid;
                assert(reqid != prev_reqid);
                prev_reqid = reqid;
                struct aio_cmd_ctx *ctx = &w->cmd_ctxs[reqid];

                assert(ctx->cmd.result == 0);
                ctx->cmd.data_length = aioevents.events[i].result;
                _cb(w, *ctx);
                __sync_fetch_and_add(&w->processed_io, 1);
                _clear_ctx(ctx);
            }

            completed += aioevents.nr;
            if(completed < w->ios_sent_to_disk) {
                goto again;
            }
        }
    } 
}

void _store_async(struct aio_w *w, struct aio_event *e) {
    int sent = __sync_fetch_and_add(&w->sent_io, 1);
    int buffer_idx = sent % w->max_pending_io;
    struct aio_cmd_ctx *ctx = &w->cmd_ctxs[buffer_idx];
    assert(ctx->index == buffer_idx);

    assert(e->len <= VALUE_RETRIEVE_LENGTH);

    _build_cmd(e, &ctx->cmd, buffer_idx, w->ns, w->kvssd_aioctx);

    assert(e->key);
    ctx->k = e->key;
    ctx->type = AIO_STORE;
    ctx->args = e->args;
    ctx->log = false;
    ctx->log_num = UINT16_MAX;
    ctx->type = e->type;
    ctx->cmd.opcode = nvme_cmd_kv_store;
    ctx->cb = e->cb ? e->cb : _response_cb;

    if(_buffer_check(w, sent)) {
        assert(0);
    }

    free(e);
    return;
}

void _retrieve_async(struct aio_w *w, struct aio_event *e) {
    int sent = __sync_fetch_and_add(&w->sent_io, 1);
    int buffer_idx = sent % w->max_pending_io;
    struct aio_cmd_ctx *ctx = &w->cmd_ctxs[buffer_idx];

    _build_cmd(e, &ctx->cmd, buffer_idx, w->ns, w->kvssd_aioctx);

    ctx->k = e->key;
    ctx->type = AIO_RETRIEVE;
    ctx->cb = e->cb;
    ctx->args = e->args;
    ctx->log = false;
    ctx->log_num = UINT16_MAX;
    ctx->type = e->type;
    ctx->cmd.opcode = nvme_cmd_kv_retrieve;

    if(_buffer_check(w, sent)) {
        assert(0);
    }
    
    free(e);
    return;
}

void _delete_async(struct aio_w *w, struct aio_event *e) {
    int sent = __sync_fetch_and_add(&w->sent_io, 1);
    int buffer_idx = sent % w->max_pending_io;
    struct aio_cmd_ctx *ctx = &w->cmd_ctxs[buffer_idx];

    assert(e->len <= 4096);

    _build_cmd(e, &ctx->cmd, buffer_idx, w->ns, w->kvssd_aioctx);

    ctx->k = e->key;
    ctx->type = AIO_DELETE;
    ctx->cb = e->cb;
    ctx->args = e->args;
    ctx->log = false;
    ctx->log_num = UINT16_MAX;
    ctx->type = e->type;
    ctx->cmd.opcode = nvme_cmd_kv_delete;

    if(_buffer_check(w, sent)) {
        assert(0);
    }
    
    free(e);
    return;
}

void _dequeue_requests(struct aio_w *w) {
    size_t sent_callbacks = 
    __atomic_load_n(&w->sent_callbacks,  __ATOMIC_SEQ_CST);

	size_t pending = sent_callbacks - w->processed_callbacks;
    if(pending == 0) {
        return;
    }

    for(size_t i = 0; i < pending; i++) {
        int idx = w->processed_callbacks % w->max_pending_callbacks;
        struct aio_event *e = w->events[idx];
        enum aio_event_type type = e->type;

        switch(type) {
            case AIO_STORE:
                _store_async(w, e);
                break;
            case AIO_RETRIEVE:
                _retrieve_async(w, e);
                break;
            case AIO_DELETE:
                _delete_async(w, e);
                break;
            default:
                assert(0);
        }

        w->events[idx] = NULL;
        __sync_fetch_and_add(&w->processed_callbacks, 1);
    }

    return;
}

static size_t _get_event(struct aio_w *w) {
	size_t next_buffer = __sync_fetch_and_add(&w->buffered_callbacks_idx, 1);
	while(1) {
		volatile size_t pending = next_buffer - 
        __atomic_load_n(&w->processed_callbacks,  __ATOMIC_SEQ_CST);
		if(pending >= w->max_pending_callbacks) { // Queue is full, wait
			usleep(2);
		} else {
			break;
		}
	}
	return next_buffer % w->max_pending_callbacks;
}

static size_t _submit_event(struct aio_w *w, int buffer_idx) {
	while(1) {
		if(__atomic_load_n(&w->sent_callbacks,  __ATOMIC_SEQ_CST)
           % w->max_pending_callbacks != buffer_idx) { 
            // Somebody else is enqueuing a request, wait!
			usleep(1); // TODO nop10
		} else {
			break;
		}
	}
	return __sync_fetch_and_add(&w->sent_callbacks, 1);
}

void _enqueue_event(struct aio_w *w, struct aio_event *e) {
    size_t buffer_idx = _get_event(w);
    w->events[buffer_idx] = e;
    _submit_event(w, buffer_idx);
}

static void* aio_w_t(void* voidargs) {
    struct aio_w *w = (struct aio_w*) voidargs;

    while(1) {
        while(_io_pending(w)) {
            _enqueue_io(w);
			_complete_io(w);
        }

        volatile size_t pending = 
        __atomic_load_n(&w->sent_callbacks,  __ATOMIC_SEQ_CST)
        - w->processed_callbacks;

        while(!pending && !_io_pending(w)) {
            usleep(1);
            pending = __atomic_load_n(&w->sent_callbacks,  __ATOMIC_SEQ_CST) 
                      - w->processed_callbacks;

            if(!pending && !_io_pending(w)) {
                if(aio_w_shutdown.load()) {
                    return 0;
                }
            }
        }

        _dequeue_requests(w);
    }

    return 0;
}

fdb_status kvssdmgr_init(const char *dev_path, const char *config_file_path) {
    if(initialized) {
        return FDB_RESULT_SUCCESS;
    }

    memset(readBuffer, 0x0, MAX_VALUE_SIZE);

    aio_w_shutdown.store(0);
    aio_w_pt = (pthread_t*)malloc(sizeof(pthread_t) * N_AIO);
    workers = (aio_w*) calloc(N_AIO, sizeof(*workers));
    for(int i = 0; i < N_AIO; i++) {
        struct aio_w *w = &workers[i];
        memset(w, 0x0, sizeof(*w));
        w->id = i;
        w->qd = QD;

        w->fd = open(dev_path, O_RDONLY);
        if (w->fd < 0) {
            fprintf(stderr, "fail to open device %s.\n", dev_path);
            assert(0);
        }
        g_fd = w->fd;

        w->ns = ioctl(w->fd, NVME_IOCTL_ID);
        if ((unsigned) w->ns == (unsigned) -1) {
            fprintf(stderr, "fail to get nsid for %s.\n", dev_path);
            assert(0);
        }
        g_nsid = w->ns;

        w->efd = eventfd(0, EFD_NONBLOCK);
        if (w->efd < 0) {
            fprintf(stderr, "fail to create an event.\n");
            assert(0);
        }

        int ret;
        w->kvssd_aioctx.eventfd = w->efd;
        if ((ret = ioctl(w->fd, NVME_IOCTL_SET_AIOCTX, &w->kvssd_aioctx)) < 0) {
            fprintf(stderr, "fail to set_aioctx %s.\n", strerror(ret));
            assert(0);
        }

        FD_ZERO(&w->rfds);
        FD_SET(w->efd, &w->rfds);

        w->max_pending_callbacks = w->qd;
        w->events = 
        (struct aio_event**)calloc(w->max_pending_callbacks, sizeof(*w->events));

        w->max_pending_io = w->qd;
        w->cmd_ctxs = 
        (struct aio_cmd_ctx*)calloc(w->max_pending_io, sizeof(*w->cmd_ctxs));
        for(int i = 0; i < w->max_pending_io; i++) {
            memset(&w->cmd_ctxs[i], 0x0, sizeof(w->cmd_ctxs[i]));
            memset(&w->cmd_ctxs[i].cmd, 0x0, sizeof(w->cmd_ctxs[i].cmd));
            
            w->cmd_ctxs[i].index = i;
        }

        pthread_create(&aio_w_pt[i], NULL, aio_w_t, w);
        pthread_setname_np(aio_w_pt[i], "WORKER");
    }

    initialized = 1;

    return FDB_RESULT_SUCCESS;
}

ssize_t kvssd_store(key *key, void *val, uint32_t vlen, 
                    struct callback_args* cb_args) {
    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
            "Storing %s", key->print());

    int ret;
    struct aio_event *e = (struct aio_event*) malloc(sizeof(*e));;
    e->type = AIO_STORE;
    e->len = vlen;
    e->key = key;

    assert(vlen <= MAX_VALUE_SIZE);

    if(!cb_args) {
        struct nvme_passthru_kv_cmd cmd;
        struct nvme_aioctx dummy;

        e->buf = val;
        _build_cmd(e, &cmd, 0, g_nsid, dummy);

        ret = ioctl(g_fd, NVME_IOCTL_IO_KV_CMD, &cmd);
        if(ret) {
            printf("Write failed for key %s (ret %d %s)\n",
                    key->print(), ret, kvs_errstr(ret));
            fflush(stdout);
            assert(0);
            free(e);
            return 0;
        } else {
            free(e);
            delete key;
            return vlen;
        }
    } else {
        assert(vlen <= 4096);
        auto w = key->vnum() % N_AIO;

        e->buf = malloc(vlen);
        memcpy(e->buf, val, vlen);
        e->args = cb_args;
        e->cb = cb_args->cb;

        char buf[128];
        pthread_getname_np(pthread_self(), buf, 128);
        if(strcmp(buf, "WORKER")) {
            _enqueue_event(&workers[w], e); 
        } else {
            _store_async(&workers[w], e);
        }

        return FDB_RESULT_SUCCESS;
    }
}

int kvssd_flush() {
    return 0;
}

bool _special_key(key *key) {
    if(!strcmp((char*) key->data(), MILESTONE_K.c_str()) ||
       !strcmp((char*) key->data(), "UDATA") ||
       !strcmp((char*) key->data(), "DATA")  ||
       !strncmp((char*) key->data(), "@Supe", 5)) {
        return true;
    } else {
        return false;
    }
}

ssize_t kvssd_retrieve(key *key, void *buf, struct callback_args* cb_args) {
    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
            "Retrieving %s", key->print());

    struct aio_event *e = (struct aio_event*) malloc(sizeof(*e));;
    e->type = AIO_RETRIEVE;
    e->key = key;
    e->buf = buf;

    if(cb_args) {
        auto w = key->vnum() % N_AIO;

        e->len = LOG_RETRIEVE_LENGTH;
        e->args = cb_args;
        e->cb = cb_args->cb;

        _enqueue_event(&workers[w], e); 

        return 0;
    } else {
        struct nvme_passthru_kv_cmd cmd;
        struct nvme_aioctx dummy;
        int ret = 1;

        if(_special_key(key)) {
            e->len = 4096;
        } else {
            e->len = VALUE_RETRIEVE_LENGTH;
        }

        _build_cmd(e, &cmd, 0, g_nsid, dummy);
        ret = ioctl(g_fd, NVME_IOCTL_IO_KV_CMD, &cmd);

        if(ret) {
            fflush(stdout);
            free(e);
            return 0;
        } else {
            free(e);
            return cmd.result;
        }
    }
}

fdb_status kvssd_delete(key *key, struct callback_args* cb_args) {
    struct aio_event *e = (struct aio_event*) malloc(sizeof(*e));;
    e->key = key;
    e->len = 0;
    e->buf = NULL;

    if(cb_args) {
        auto w = key->vnum() % N_AIO;

        e->type = AIO_DELETE;
        e->args = cb_args;
        e->cb = cb_args->cb;

        _enqueue_event(&workers[w], e); 

        return FDB_RESULT_SUCCESS;
    } else {
        struct nvme_passthru_kv_cmd cmd;
        struct nvme_aioctx dummy;
        e->type = DELETE_WITH_CHECK;

        _build_cmd(e, &cmd, 0, g_nsid, dummy);
        int ret = ioctl(g_fd, NVME_IOCTL_IO_KV_CMD, &cmd);

        free(e);
        if(ret && ret != 0x310) {
            printf("Delete failed for key %s (ret %d %s)\n",
                    key->print(), ret, kvs_errstr(ret));
            fflush(stdout);
            return FDB_RESULT_WRITE_FAIL;
        } else if(ret == 0x310) {
            return FDB_RESULT_KEY_NOT_EXIST;
        } else {
            return FDB_RESULT_SUCCESS;
        }
    }
}

fdb_status kvssdmgr_close()
{
    aio_w_shutdown.store(1);
    for(int i = 0; i < N_AIO; i++) {
        pthread_join(aio_w_pt[i], NULL);

        free(workers[i].events);
        free(workers[i].cmd_ctxs);
    }
    free(workers);
    free(aio_w_pt);

    fdb_log(NULL, FDB_LOG_INFO, FDB_RESULT_SUCCESS, "KVSSD closed");

    initialized = 0;

    return FDB_RESULT_SUCCESS;
}

fdb_status kvssd_get_utilization(uint32_t *dev_util) {
    return FDB_RESULT_SUCCESS;
}

fdb_status kvssd_get_capacity(uint64_t *dev_capa) {
    return FDB_RESULT_SUCCESS;
}


struct filemgr_ops_kvssd samsung_kvssd_ops = {
        kvssdmgr_init,
        kvssd_store,
        kvssd_retrieve,
        kvssd_delete,
        kvssd_get_utilization,
        kvssd_get_capacity,
        kvssd_flush,
        kvssdmgr_close,
};

struct filemgr_ops_kvssd * get_samsung_kvssd_filemgr_ops()
{
    return &samsung_kvssd_ops;
}
