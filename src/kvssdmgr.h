#ifndef FORESTDB_KVSSDMGR_H
#define FORESTDB_KVSSDMGR_H

#define SAMSUNG_API

#include <sys/ioctl.h>
#include <sys/eventfd.h>
#include <kvs_api.h>
#include <stdlib.h>
#include <unistd.h>

#include "atomic.h"
#include "key.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TSTAMP_LENGTH sizeof(uint64_t)
extern uint32_t VALUE_RETRIEVE_LENGTH;
extern uint32_t LOG_RETRIEVE_LENGTH;
extern uint32_t MAX_ENTRIES_PER_LOG_WRITE;

void set_n_aio(uint32_t n);
void set_qd(uint32_t qd);

#define MAX_VALUE_SIZE 2097152
#define nvme_cmd_kv_store 0x81
#define nvme_cmd_kv_retrieve 0x90

extern std::string MILESTONE_K;

#ifndef __KVSSD_EMU
#include "linux_nvme_ioctl.h"
struct callback_args {
    void (*cb)(struct nvme_passthru_kv_cmd, void*);
    void* args;

    /*
     * Is this callback being called from inside
     * another callback?
     */

    bool nested = false;
};

enum aio_event_type {
    AIO_STORE = 0,
    AIO_RETRIEVE,
    AIO_RETRIEVE_KEY,
    AIO_DELETE,
    AIO_DELETE_VNUM,
    AIO_DELETE_LOG,
    FILEMGR_READ,
    AIO_NODE_COLLECT,
    LOG_WRITE,
    LOG_READ,
    RECOVERY_READ,
    DELETE_WITH_CHECK,
    LAST
};

struct aio_event {
    enum aio_event_type type;
    key *key;
    uint64_t len;
    unsigned char prefix[4];
    void *buf;
    void (*cb)(nvme_passthru_kv_cmd, void*);
    void *args;
};

/*
 * Count the number of completed I/O after
 * sending some
 */

extern atomic_uint64_t responses;
typedef void (*io_cb)(struct nvme_passthru_kv_cmd, void*);
#else
#include "/home/carl/KVSSD/PDK/core/include/kvs_api.h"
struct callback_args {
    void (*cb)(kvs_postprocess_context *ctx);
    void* args;

    /*
     * Is this callback being called from inside
     * another callback?
     */

    bool nested = false;
};
#endif

#ifdef __cplusplus
}
#endif

#endif //FORESTDB_KVSSDMGR_H
