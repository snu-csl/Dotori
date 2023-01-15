#include "libforestdb/forestdb.h"
#include "log_message.h"
#include "filemgr_ops_kvssd.h"

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

std::string MILESTONE_K = "@milestone";
uint8_t initialized = 0;

kvs_device_handle dev_hd;
kvs_key_space_handle ks_hd;

thread_local char readBuffer[MAX_VALUE_SIZE];

fdb_status kvssdmgr_init(const char *dev_path, const char *config_file_path) {
    if(initialized) {
        return FDB_RESULT_SUCCESS;
    }

    kvs_result ret;

    ret = kvs_open_device((char*) dev_path, &dev_hd);
    if(ret != KVS_SUCCESS) {
        if(ret == KVS_ERR_SYS_IO) {
            initialized = 1;
            fdb_log(NULL, FDB_LOG_DEBUG,
                    FDB_RESULT_OPEN_FAIL, 
                    "KVSSD %s was already open", 
                    dev_path);
            return FDB_RESULT_SUCCESS;
        }
        fdb_log(NULL, FDB_LOG_FATAL,
                FDB_RESULT_WRITE_FAIL, 
                "Failed to open KVSSD %s error %d (%s)", 
                dev_path, ret, kvs_errstr(ret));
        return FDB_RESULT_OPEN_FAIL;
    }

    kvs_key_space_name ks_name;
    kvs_option_key_space option = { KVS_KEY_ORDER_NONE };
    ks_name.name = (char*) "dotori";
    ks_name.name_len = strlen(ks_name.name);
    ret = kvs_create_key_space(dev_hd, &ks_name, 0, option);
    if (ret != KVS_SUCCESS) {
        kvs_close_device(dev_hd);

        fdb_log(NULL, FDB_LOG_FATAL,
                FDB_RESULT_WRITE_FAIL, 
                "Failed to create KVSSD keyspace %s error %d (%s)", 

                ks_name.name, ret, kvs_errstr(ret));
        return FDB_RESULT_OPEN_FAIL;
    }

    ret = kvs_open_key_space(dev_hd, ks_name.name, &ks_hd);
    if(ret != KVS_SUCCESS) {
        fdb_log(NULL, FDB_LOG_FATAL, 
                FDB_RESULT_WRITE_FAIL,
                "Failed to open KVSSD keyspace %s error %d (%s).\n", 
                ks_name.name, ret, kvs_errstr(ret));
        kvs_delete_key_space(dev_hd, &ks_name);
        kvs_close_device(dev_hd);
        return FDB_RESULT_OPEN_FAIL;
    }

    memset(readBuffer, 0x0, MAX_VALUE_SIZE);

    initialized = 1;

    return FDB_RESULT_SUCCESS;
}

ssize_t kvssd_store(key *key, void *val, uint32_t vlen, 
                    struct callback_args* cb_args) {
    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
            "Storing %s len %u", key->print(), vlen);

    kvs_result ret;
    kvs_option_store option;
    option.st_type = KVS_STORE_POST;
    uint8_t klen = key->len();

    kvs_key *kvskey = (kvs_key*) malloc(sizeof(kvs_key));
    kvs_value *kvsvalue = (kvs_value*) malloc(sizeof(kvs_value));

    kvskey->key = malloc(klen);
    memset(kvskey->key, 0x0, klen);
    memcpy(kvskey->key, key->data(), klen);
    kvskey->length = klen;

    kvsvalue->length = vlen;
    kvsvalue->actual_value_size = 0;
    kvsvalue->offset = 0;

    if(!cb_args) {
        kvsvalue->value = val;
        ret = kvs_store_kvp(ks_hd, kvskey, kvsvalue, &option);
        free(kvskey->key);
        free(kvskey);
		free(kvsvalue);
	} else {
        kvsvalue->value = malloc(vlen);
        memcpy(kvsvalue->value, val, vlen);
		ret = kvs_store_kvp_async(ks_hd, kvskey, kvsvalue, 
								  &option, cb_args, NULL, 
								  cb_args->cb);
	}

    if(ret != KVS_SUCCESS) {
        fdb_log(NULL, FDB_LOG_ERROR,
                FDB_RESULT_WRITE_FAIL,
                "Store %s failed with %s", 
                (char*) key->data(), 
                kvs_errstr(ret));
        delete key;
        return FDB_RESULT_WRITE_FAIL;
    }

    delete key;
    return vlen;
}

int kvssd_flush() {
    return 0;
}

ssize_t kvssd_retrieve(key *key, void *buf, 
                       struct callback_args* cb_args) {
    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
            "Retrieving %s", key->print());

    kvs_result ret;
    kvs_option_retrieve option;
    option.kvs_retrieve_delete = false;
    
    uint8_t klen = key->len();

    kvs_key *kvskey = (kvs_key*) malloc(sizeof(kvs_key));
    kvs_value *kvsvalue = (kvs_value*) malloc(sizeof(kvs_value));

    kvskey->key = malloc(klen);
    memset(kvskey->key, 0x0, klen);
    memcpy(kvskey->key, key->data(), klen);
    kvskey->length = klen;

    kvsvalue->length = VALUE_RETRIEVE_LENGTH;
    kvsvalue->actual_value_size = 0;
    kvsvalue->offset = 0;

    if(cb_args) {
        char* value = (char*) malloc(VALUE_RETRIEVE_LENGTH);
        kvsvalue->value = value;
        ret = kvs_retrieve_kvp_async(ks_hd, kvskey, &option, 
                                     cb_args, NULL, kvsvalue,
                                     cb_args->cb);
        if(ret != KVS_SUCCESS) {
            fdb_log(NULL, FDB_LOG_ERROR,
                    FDB_RESULT_WRITE_FAIL,
                    "Async retrieve for %s failed with %s", 
                    (char*) key->data(), 
                    kvs_errstr(ret));
            return FDB_RESULT_READ_FAIL;
        }

        delete key;
        return FDB_RESULT_SUCCESS;
    } else {
        kvsvalue->value = readBuffer;
        ret = kvs_retrieve_kvp(ks_hd, kvskey, &option, kvsvalue);
        if(ret != KVS_SUCCESS) {
            fdb_log(NULL, FDB_LOG_ERROR,
                    FDB_RESULT_WRITE_FAIL,
                    "Retrieve for %s failed with %s", 
                    (char*) key->data(), 
                    kvs_errstr(ret));
            free(kvskey->key);
            free(kvskey);
            free(kvsvalue);
            return 0;
        }

        auto ret_len = kvsvalue->actual_value_size;
        memcpy(buf, kvsvalue->value, ret_len);

        free(kvskey->key);
        free(kvskey);
        free(kvsvalue);

        return ret_len;
    }
}

fdb_status kvssd_delete(key *key, struct callback_args* cb_args) {
    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
            "Deleting %s", key->print());

    kvs_result ret;
    kvs_option_delete option = {false};

    uint8_t klen = key->len();

    kvs_key *kvskey = (kvs_key*) malloc(sizeof(kvs_key));

    kvskey->key = malloc(klen);
    memset(kvskey->key, 0x0, klen);
    memcpy(kvskey->key, key->data(), klen);
    kvskey->length = klen;

    if(cb_args) {
        ret = kvs_delete_kvp_async(ks_hd, kvskey, &option,
                                   cb_args, NULL, cb_args->cb);
        if(ret != KVS_SUCCESS) {
            fdb_log(NULL, FDB_LOG_ERROR,
                    FDB_RESULT_WRITE_FAIL,
                    "Delete for %s failed with %s", 
                    (char*) key->data(), 
                    kvs_errstr(ret));
            return FDB_RESULT_WRITE_FAIL;
        }
    } else {
        ret = kvs_delete_kvp(ks_hd, kvskey, &option);
        if(ret != KVS_SUCCESS) {
            fdb_log(NULL, FDB_LOG_ERROR,
                    FDB_RESULT_WRITE_FAIL,
                    "Delete for %s failed with %s", 
                    (char*) key->data(), 
                    kvs_errstr(ret));
            return FDB_RESULT_WRITE_FAIL;
        }
    }

    delete key;
    return FDB_RESULT_SUCCESS;
}

/*
 * For the emulator, we don't close here incase we need to open the store again
 * within the same process (closing the device wipes the emulator).
 */

fdb_status kvssdmgr_close()
{
    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS, "KVSSD emulator closed");
    initialized = 0;
    return FDB_RESULT_SUCCESS;
}

fdb_status kvssd_get_utilization(uint32_t *dev_util) {
    int ret;

    if (dev_hd == NULL || dev_util == NULL) {
        fdb_log(NULL, FDB_LOG_ERROR,
                FDB_RESULT_INVALID_CONFIG,
                "Parameter invalid in get_kvssd_utilization");
        return FDB_RESULT_INVALID_CONFIG;
    }

    uint32_t util;

    ret = kvs_get_device_utilization(dev_hd, &util);
    if (ret == KVS_SUCCESS) {
        *dev_util = util;
    } else {
        fdb_log(NULL, FDB_LOG_ERROR,
                FDB_RESULT_READ_FAIL,
                "Failed to get KVSSD utilization %s", kvs_errstr(ret));
        return FDB_RESULT_READ_FAIL;
    }

    return FDB_RESULT_SUCCESS;
}

fdb_status kvssd_get_capacity(uint64_t *dev_capa) {
    int ret;

    if (dev_hd == NULL || dev_capa == NULL) {
        fdb_log(NULL, FDB_LOG_ERROR,
                FDB_RESULT_INVALID_CONFIG,
                "Parameter invalid in get_kvssd_capacity");
        return FDB_RESULT_INVALID_CONFIG;
    }
    
    uint64_t capa;

    ret = kvs_get_device_capacity(dev_hd, &capa);
    if (ret == KVS_SUCCESS) {
        *dev_capa = capa;
    } else {
        fdb_log(NULL, FDB_LOG_ERROR,
                FDB_RESULT_READ_FAIL,
                "Failed to get KVSSD capacity %s", kvs_errstr(ret));
        return FDB_RESULT_READ_FAIL;
    }

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
