#ifndef _FILEMGR_OPS_KVSSD
#define _FILEMGR_OPS_KVSSD

#include "common.h"
#include "libforestdb/fdb_errors.h"
#include "libforestdb/fdb_types.h"
#include "arch.h"
#include "key.h"

#ifdef __cplusplus
extern "C" {
#endif

// Note: Please try to ensure that the following filemgr ops also have
// equivalent test/filemgr_anomalous_ops.h/cc test apis for failure testing
struct filemgr_ops_kvssd {
    fdb_status (*open)(const char *pathname,  const char *config_file_path);
    ssize_t (*store)(key *key, void *value, uint32_t vlen, 
                     struct callback_args* cb_args);
    ssize_t (*retrieve)(key *key, void *value, struct callback_args* cb_args);
    fdb_status (*del)(key *key, struct callback_args* cb_args);
    fdb_status (*get_utilization)(uint32_t *dev_util);
    fdb_status (*get_capacity)(uint64_t *dev_capa);
    int (*flush)();
    fdb_status (*close)();
};

struct filemgr_ops_kvssd * get_filemgr_ops_kvssd();

#ifdef __cplusplus
}
#endif

#endif
