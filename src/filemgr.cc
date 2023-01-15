/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <openssl/sha.h>
#if !defined(WIN32) && !defined(_WIN32)
#include <sys/time.h>
#endif
#include <time.h>
#include <string>
#include <map>
#include <set>
#include <algorithm>

#include "filemgr.h"
#include "filemgr_ops.h"
#include "filemgr_ops_kvssd.h"
#include "hash_functions.h"
#include "blockcache.h"
#include "timing.h"
#include "wal.h"
#include "list.h"
#include "fdb_internal.h"
#include "time_utils.h"
#include "encryption.h"
#include "version.h"
#include "kv_data_cache.h"
#include "btree_kv.h"
#include "libforestdb/forestdb.h"
#include "limits.h"
#include "kvssdmgr.h"

#include "memleak.h"

uint32_t log_meta_length = 16 + TSTAMP_LENGTH;
uint32_t log_entry_size = sizeof(uint64_t) + sizeof(uint64_t);

uint32_t VALUE_RETRIEVE_LENGTH;
uint32_t LOG_RETRIEVE_LENGTH;
uint32_t MAX_ENTRIES_PER_LOG_WRITE;

thread_local bool check_lat = 0;
uint32_t MAX_OUTSTANDING_REQUESTS = 64;

#ifdef __KVSSD_EMU
static void _ioctx_free_func(kvs_postprocess_context* ioctx, bool free_key, bool free_val) {
    if(free_key) {
        if(ioctx->key && ioctx->key->key) free(ioctx->key->key);
    }

    if(free_val) {
        if(ioctx->value && ioctx->value->value) free(ioctx->value->value);
    }

    if(ioctx->key) free(ioctx->key);
    if(ioctx->value) free(ioctx->value);
}
#endif

fdb_status _delete(struct filemgr *file, key *k, 
                   struct callback_args *cb) {
    fdb_status ret = file->ops_kvssd->del(k, cb);
    if(!cb) {
        //delete k;
    }
    return ret;
}

fdb_status del(struct filemgr *file, std::string str, 
            struct callback_args *cb) {
    key *k = new str_key(str);
    return _delete(file, k, cb);
}

fdb_status del(struct filemgr *file, uint64_t vernum,
            struct callback_args *cb) {
    key *k = new vnum_key(vernum);
    return _delete(file, k, cb);
}

fdb_status del(struct filemgr *file, uint64_t vernum, 
            uint32_t log_num, struct callback_args *cb) {
    key *k = new log_key(vernum, log_num);
    return _delete(file, k, cb);
}

ssize_t _store(struct filemgr *file, key *k, 
               void *buf, uint32_t vlen, struct callback_args *cb) {
    ssize_t ret = file->ops_kvssd->store(k, buf, vlen, cb);
    if(!cb) {
        //delete k;
    }
    return ret;
}

ssize_t store(struct filemgr *file, std::string str, 
              void* buf, uint32_t vlen, struct callback_args *cb) {
    key *k = new str_key(str);
    return _store(file, k, buf, vlen, cb);
}

ssize_t store(struct filemgr *file, uint64_t vernum,
              void* buf, uint32_t vlen, struct callback_args *cb) {
    key *k = new vnum_key(vernum);
    return _store(file, k, buf, vlen, cb);
}

ssize_t store(struct filemgr *file, uint64_t vernum, uint32_t log_num,
              void* buf, uint32_t vlen, struct callback_args *cb) {
    key *k = new log_key(vernum, log_num);
    return _store(file, k, buf, vlen, cb);
}

ssize_t _retrieve(struct filemgr *file, key *k, 
                  void *buf, struct callback_args *cb) {
    ssize_t ret = file->ops_kvssd->retrieve(k, buf, cb);
    if(!cb) {
        delete k;
    }
    return ret;
}

ssize_t retrieve(struct filemgr *file, std::string str, 
                 void* buf, struct callback_args *cb) {
    key *k = new str_key(str);
    return _retrieve(file, k, buf, cb);
}

ssize_t retrieve(struct filemgr *file, uint64_t vernum,
                 void* buf, struct callback_args *cb) {
    key *k = new vnum_key(vernum);
    return _retrieve(file, k, buf, cb);
}

ssize_t retrieve(struct filemgr *file, uint64_t vernum, uint32_t log_num,
                 void* buf, struct callback_args *cb) {
    key *k = new log_key(vernum, log_num);
    return _retrieve(file, k, buf, cb);
}

uint32_t _log_num_from_key(char* key) {
    return *(uint32_t*)(key + strlen("LOGG") + sizeof(uint64_t));
}

uint64_t _log_bid_from_key(char* key) {
    return *(uint64_t*)(key + strlen("LOGG"));
}

uint64_t get_timestamp()
{
    auto t0 = std::chrono::high_resolution_clock::now();        
    return t0.time_since_epoch().count();
}

static void _signal_flush(struct filemgr *file)
{
    std::lock_guard<std::mutex> lock(file->flush_lock);
    file->ready = true;
    file->flush_cond.notify_all();
}

static void put_buf(void *buf)
{
    free(buf);
    return;
}

static void * get_buf()
{
    return malloc(VALUE_RETRIEVE_LENGTH);
}

void put_large_buf(void *buf)
{
    put_buf(buf);
    return;
}

void * get_large_buf()
{
    void *ret;
    ret = malloc(LOG_RETRIEVE_LENGTH);
    return ret;
}

#ifdef __DEBUG
#ifndef __DEBUG_FILEMGR
    #undef DBG
    #undef DBGCMD
    #undef DBGSW
    #define DBG(...)
    #define DBGCMD(...)
    #define DBGSW(n, ...)
#endif
#endif

// NBUCKET must be power of 2
#define NBUCKET (1024)

// global static variables
#ifdef SPIN_INITIALIZER
static spin_t initial_lock = SPIN_INITIALIZER;
#else
static volatile unsigned int initial_lock_status = 0;
static spin_t initial_lock;
#endif

#define METASIZE_ALIGN_UNIT (16)
#ifdef METASIZE_ALIGN_UNIT
#define _metasize_align(size) \
         (((( (size + sizeof(metasize_t)) + (METASIZE_ALIGN_UNIT-1)) \
             / METASIZE_ALIGN_UNIT) * METASIZE_ALIGN_UNIT) - sizeof(metasize_t))
#else
#define _metasize_align(size) (size)
#endif

union {
    uint32_t i;
    unsigned char c[4];
} prefix;

static volatile uint8_t filemgr_initialized = 0;
extern volatile uint8_t bgflusher_initialized;
static struct filemgr_config global_config;
static struct hash hash;
static spin_t filemgr_openlock;

struct temp_buf_item{
    void *addr;
    struct list_elem le;
};
static struct list temp_buf;
static spin_t temp_buf_lock;
// larger buffers
static struct list temp_buf_kvssd;
static spin_t kvssd_temp_buf_lock;

static bool lazy_file_deletion_enabled = false;
static register_file_removal_func register_file_removal = NULL;
static check_file_removal_func is_file_removed = NULL;

static struct sb_ops sb_ops;

static void spin_init_wrap(void *lock) {
    spin_init((spin_t*)lock);
}

static void spin_destroy_wrap(void *lock) {
    spin_destroy((spin_t*)lock);
}

static void spin_lock_wrap(void *lock) {
    spin_lock((spin_t*)lock);
}

static void spin_unlock_wrap(void *lock) {
    spin_unlock((spin_t*)lock);
}

static void mutex_init_wrap(void *lock) {
    mutex_init((mutex_t*)lock);
}

static void mutex_destroy_wrap(void *lock) {
    mutex_destroy((mutex_t*)lock);
}

static void mutex_lock_wrap(void *lock) {
    mutex_lock((mutex_t*)lock);
}

static void mutex_unlock_wrap(void *lock) {
    mutex_unlock((mutex_t*)lock);
}

INLINE int _cmp_uint64_t(void *key1, void *key2, void *aux)
{
    (void) aux;
    uint64_t a,b;
    a = deref64(key1);
    b = deref64(key2);

#ifdef __BIT_CMP
    return _CMP_U64(a, b);
#else
    if (a < b) {
        return -1;
    } else if (a > b) {
        return 1;
    } else {
        return 0;
    }
#endif
}

INLINE int _cmp_binary64(void *key1, void *key2, void *aux)
{
    (void) aux;
#ifdef __BIT_CMP
    uint64_t a,b;
    a = _endian_encode(deref64(key1));
    b = _endian_encode(deref64(key2));
    return _CMP_U64(a, b);
#else
    return memcmp(key1, key2, 8);
#endif
}

static int _kvs_stat_cmp(struct avl_node *a, struct avl_node *b, void *aux)
{
    struct kvs_node *aa, *bb;
    aa = _get_entry(a, struct kvs_node, avl_id);
    bb = _get_entry(b, struct kvs_node, avl_id);

    if (aa->id < bb->id) {
        return -1;
    } else if (aa->id > bb->id) {
        return 1;
    } else {
        return 0;
    }
}

static int _block_is_overlapped(void *pbid1, void *pis_writer1,
                                void *pbid2, void *pis_writer2,
                                void *aux)
{
    (void)aux;
    bid_t bid1, is_writer1, bid2, is_writer2;
    bid1 = *(bid_t*)pbid1;
    is_writer1 = *(bid_t*)pis_writer1;
    bid2 = *(bid_t*)pbid2;
    is_writer2 = *(bid_t*)pis_writer2;

    if (bid1 != bid2) {
        // not overlapped
        return 0;
    } else {
        // overlapped
        if (!is_writer1 && !is_writer2) {
            // both are readers
            return 0;
        } else {
            return 1;
        }
    }
}

static int _addr_is_overlapped(void *addr1, void *addris_writer1,
                                void *addr2, void *addris_writer2,
                                void *aux)
{
    (void)aux;
    bid_t is_writer1, is_writer2;
    is_writer1 = *(bid_t*)addris_writer1;
    is_writer2 = *(bid_t*)addris_writer2;

    if (addr1 != addr2) {
        // not overlapped
        return 0;
    } else {
        // overlapped
        if (!is_writer1 && !is_writer2) {
            // both are readers
            return 0;
        } else {
            return 1;
        }
    }
}

static void _log_errno_str(struct filemgr_ops *ops,
                           err_log_callback *log_callback,
                           fdb_status io_error,
                           const char *what,
                           const char *filename)
{
    if (io_error < 0) {
        char errno_msg[512];
        ops->get_errno_str(errno_msg, 512);
        fdb_log(log_callback, FDB_LOG_ERROR, io_error,
                "Error in %s on a database file '%s', %s", what, filename, errno_msg);
    }
}

static uint32_t _file_hash(struct hash *hash, struct hash_elem *e)
{
    struct filemgr *file = _get_entry(e, struct filemgr, e);
    int len = strlen(file->filename);

    return get_checksum(reinterpret_cast<const uint8_t*>(file->filename), len) &
                        ((unsigned)(NBUCKET-1));
}

static int _file_cmp(struct hash_elem *a, struct hash_elem *b)
{
    struct filemgr *aa, *bb;
    aa = _get_entry(a, struct filemgr, e);
    bb = _get_entry(b, struct filemgr, e);
    return strcmp(aa->filename, bb->filename);
}

void filemgr_init(struct filemgr_config *config)
{
    // global initialization
    // initialized only once at first time
    if (!filemgr_initialized) {
#ifndef SPIN_INITIALIZER
        // Note that only Windows passes through this routine
        if (InterlockedCompareExchange(&initial_lock_status, 1, 0) == 0) {
            // atomically initialize spin lock only once
            spin_init(&initial_lock);
            initial_lock_status = 2;
        } else {
            // the others ... wait until initializing 'initial_lock' is done
            while (initial_lock_status != 2) {
                Sleep(1);
            }
        }
#endif

        spin_lock(&initial_lock);
        if (!filemgr_initialized) {
            memset(&sb_ops, 0x0, sizeof(sb_ops));
            global_config = *config;

            bcache_config bconfig;
            bconfig.do_not_cache_doc_blocks = global_config.do_not_cache_doc_blocks;
            kvcache_config kcconfig;
            if (global_config.ncacheblock > 0) {
                bcache_init(global_config.ncacheblock,
                            global_config.index_blocksize,
                            bconfig);
            }

            if(global_config.kv_cache_size > 0) {
                kcconfig.blocksize = config->blocksize;
                kvcache_init(global_config.kv_cache_size, kcconfig);
            }

            hash_init(&hash, NBUCKET, _file_hash, _file_cmp);

            // initialize temp buffer
            list_init(&temp_buf);
            spin_init(&temp_buf_lock);

            if(config->kvssd) {
                /*
                    Initialize larger temp buffers for the KVSSD
                    as single writes can be much larger than a page
                */
                list_init(&temp_buf_kvssd);
                spin_init(&kvssd_temp_buf_lock);
            }
            // initialize global lock
            spin_init(&filemgr_openlock);

            // set the initialize flag
            filemgr_initialized = 1;
        }
        spin_unlock(&initial_lock);
    }
}

void filemgr_set_lazy_file_deletion(bool enable,
                                    register_file_removal_func regis_func,
                                    check_file_removal_func check_func)
{
    lazy_file_deletion_enabled = enable;
    register_file_removal = regis_func;
    is_file_removed = check_func;
}

void filemgr_set_sb_operation(struct sb_ops ops)
{
    sb_ops = ops;
}

// Read a block from the file, decrypting if necessary.
ssize_t filemgr_read_block(struct filemgr *file, void *buf, bid_t bid) {
    ssize_t result = file->ops->pread(file->fd, buf, file->blocksize,
                                      file->blocksize*bid);
    if (file->encryption.ops && result > 0) {
        if (result != (ssize_t)file->blocksize)
            return FDB_RESULT_READ_FAIL;
        fdb_status status = fdb_decrypt_block(&file->encryption, buf, result, bid);
        if (status != FDB_RESULT_SUCCESS)
            return status;
    }
    return result;
}

// Read a block from the file, decrypting if necessary.
ssize_t filemgr_read_block(struct filemgr *file, void *_key, void *buf, bid_t bid) {
    ssize_t result;

    if (file->config->kvssd) {
        if (_key != NULL) {
            std::string keyBuf = (char *) _key + std::to_string(bid);
            result = retrieve(file, keyBuf, buf, NULL);
            if (result != (ssize_t)file->blocksize) {
                return FDB_RESULT_READ_FAIL;
            }
        } else { // only use vernum as key
            result = retrieve(file, bid, buf, NULL);
            if (result != file->blocksize) {
                return FDB_RESULT_READ_FAIL;
            }
        }

        if (file->encryption.ops && result > 0) {
            if (result != (ssize_t)file->blocksize) {
                return FDB_RESULT_READ_FAIL;
            }

            fdb_status status = fdb_decrypt_block(&file->encryption, buf, result, bid);
            if (status != FDB_RESULT_SUCCESS) {
                return status;
            }
        }

        return file->blocksize;
    } else {
        ssize_t result = file->ops->pread(file->fd, buf, file->blocksize,
                                          file->blocksize*bid);
        if (file->encryption.ops && result > 0) {
            if (result != (ssize_t)file->blocksize) {

                return FDB_RESULT_READ_FAIL;
            }

            fdb_status status = fdb_decrypt_block(&file->encryption, buf, result, bid);
            if (status != FDB_RESULT_SUCCESS) {
                if(filemgr_get_file_status(file) == FILE_COMPACT_OLD) {
                }
                return status;
            }
        }
        if(filemgr_get_file_status(file) == FILE_COMPACT_OLD) {
        }
        return result;
    }
}

// Write consecutive block(s) to the file, encrypting if necessary.
ssize_t filemgr_write_blocks(struct filemgr *file, void *buf, unsigned num_blocks, bid_t start_bid) {
    size_t blocksize = file->blocksize;
    cs_off_t offset = start_bid * blocksize;
    size_t nbytes = num_blocks * blocksize;

    if (file->encryption.ops == NULL) {
        return file->ops->pwrite(file->fd, buf, nbytes, offset);
    } else {
        uint8_t *encrypted_buf;
        if (nbytes > 4096)
            encrypted_buf = (uint8_t*)malloc(nbytes);
        else
            encrypted_buf = alca(uint8_t, nbytes); // most common case (writing single block)
        if (!encrypted_buf)
            return FDB_RESULT_ALLOC_FAIL;
        fdb_status status = fdb_encrypt_blocks(&file->encryption,
                                               encrypted_buf,
                                               buf,
                                               blocksize,
                                               num_blocks,
                                               start_bid);
        if (nbytes > 4096)
            free(encrypted_buf);
        if (status != FDB_RESULT_SUCCESS)
            return status;
        return file->ops->pwrite(file->fd, encrypted_buf, nbytes, offset);
    }
}

// Write consecutive block(s) to the file, encrypting if necessary.
ssize_t filemgr_write_blocks_kvssd(struct filemgr *file, void *_key, void *buf, unsigned num_blocks, bid_t start_bid) {
    size_t blocksize = file->blocksize;
    ssize_t result;
    size_t nbytes = num_blocks * blocksize;
    size_t klen;

    if (file->encryption.ops == NULL) {
        if (_key != NULL && ((char *)_key)[0] == '@') { // only write super blocks differently
            std::string keyBuf = (char *) _key + std::to_string(start_bid);

            result = store(file, keyBuf, buf, num_blocks * blocksize, NULL);
            klen = keyBuf.length();

            atomic_add_uint64_t(&file->data_in_kvssd, 
                                VALUE_RETRIEVE_LENGTH - 1024 + sizeof(uint64_t));
            atomic_add_uint64_t(&file->written_to_kvssd, nbytes + klen);
        } else {
            result = store(file, start_bid, buf, num_blocks * blocksize, NULL);
            klen = sizeof(start_bid);

            atomic_add_uint64_t(&file->data_in_kvssd, nbytes + klen);
            atomic_add_uint64_t(&file->written_to_kvssd, nbytes + klen);
        }

    } else {
        uint8_t *encrypted_buf;
        if (nbytes > 4096)
            encrypted_buf = (uint8_t*)malloc(nbytes);
        else
            encrypted_buf = alca(uint8_t, nbytes); // most common case (writing single block)
        if (!encrypted_buf)
            return FDB_RESULT_ALLOC_FAIL;
        fdb_status status = fdb_encrypt_blocks(&file->encryption,
                                               encrypted_buf,
                                               buf,
                                               blocksize,
                                               num_blocks,
                                               start_bid);
        if (nbytes > 4096)
            free(encrypted_buf);
        if (status != FDB_RESULT_SUCCESS)
            return status;
        result = store(file, start_bid, encrypted_buf, nbytes, NULL);
        return result;
    }

    return result;
}

// Write consecutive block(s) to the file, encrypting if necessary.
ssize_t filemgr_write_pair_kvssd(struct filemgr *file, void *val, size_t vlen, bid_t vernum) 
{
    size_t nbytes = vlen;

    if (file->encryption.ops == NULL) {
        return store(file, vernum, val, vlen, NULL);
    } else {
        uint8_t *encrypted_buf;
        if (nbytes > 4096)
            encrypted_buf = (uint8_t*)malloc(nbytes);
        else
            encrypted_buf = alca(uint8_t, nbytes); // most common case (writing single block)
        if (!encrypted_buf)
            return FDB_RESULT_ALLOC_FAIL;
        fdb_status status = fdb_encrypt_blocks(&file->encryption,
                                               encrypted_buf,
                                               val,
                                               vlen,
                                               1,
                                               vernum);
        if (nbytes > 4096)
            free(encrypted_buf);
        if (status != FDB_RESULT_SUCCESS)
            return status;
        return store(file, vernum, encrypted_buf, nbytes, NULL);
    }
}


INLINE struct bnode *_fetch_bnode(void *addr)
{
    struct bnode *node = NULL;

    node = (struct bnode *)addr;

    if (!(node->flag & BNODE_MASK_METADATA)) {
        // no metadata
        node->data = (uint8_t *)addr + sizeof(struct bnode);
    } else {
        // metadata
        metasize_t metasize;
        memcpy(&metasize, (uint8_t *)addr + sizeof(struct bnode), sizeof(metasize_t));
        metasize = _endian_decode(metasize);
        node->data = (uint8_t *)addr + sizeof(struct bnode) + sizeof(metasize_t) +
                     _metasize_align(metasize);
    }

    return node;
}

struct bnode* _init_bnode(uint64_t bid, void *addr)
{
    struct bnode* node;

    node = (struct bnode *)addr;
    node->kvsize = 8 << 8 | 8;
    node->nentry = 0;
    node->level = 100;
    node->flag = 0;
    node->bid = bid;
    node->data = (uint8_t *)addr + sizeof(struct bnode);

    return node;
}

/*
 * In us
 */

struct lat_tracking {
    uint64_t first_read_sub;
    uint64_t last_read_sub;
    uint64_t *read_lats;
    uint64_t entered_reduce;
    uint64_t waited_for_lock;
    uint64_t left_reduce;
};

struct collect_logs_cb_args
{
    struct filemgr *file;
    atomic_uint32_t *valid;
    uint32_t log_num;
    std::unordered_map<std::string, uint64_t> *pairs;
    std::set<std::string> *expired_pairs;
    std::unordered_map<std::string, uint64_t> *ret;
    uint64_t *timestamps;
    uint16_t *level;
    atomic_uint16_t *outstanding_reads;
    atomic_uint16_t *complete;
    uint64_t bid;
    void (*cb)(std::unordered_map<std::string, uint64_t>*, void*, void*, bool);
    void *voidargs;
    std::string *latest_log;
    struct lat_tracking *lat;
};

void _compact_logs_cb(std::unordered_map<std::string, uint64_t> *log, 
        void *voidargs,
        void* buf,
        bool nested);

void __filemgr_collect_logs_cb(struct collect_logs_cb_args *args,
                               uint64_t len, char *buf, 
                               bool nested) {
    struct filemgr *file;
    uint64_t vernum;
    uint32_t log_num;
    uint64_t bid;
    uint32_t max_logs;
    std::string *latest_log;
    uint64_t *timestamps;
    uint64_t timestamp;
    bool entered = false;
    bool last = false;

    file = args->file; 
    log_num = args->log_num;
    bid = args->bid;
    max_logs = file->config->max_logs_per_node + 1;
    latest_log = args->latest_log;
    timestamps = args->timestamps;

    uint64_t ts = get_monotonic_ts_us();
    args->lat->read_lats[log_num] = ts - args->lat->read_lats[log_num];

    if(atomic_decr_uint16_t(args->outstanding_reads) == 0) {
        last = true;
    }

    if(len == 0) {
        goto end;
    }

    timestamp = *(uint64_t*)((uint8_t*)buf + (len - sizeof(uint64_t)));
    timestamps[log_num] = timestamp;

    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
            "Got timestamp %lu for bid %lu log %u", timestamp, bid, log_num);

    for(unsigned int j = 0; j < len / log_entry_size; j++) {
        std::string key_from_log = std::string(buf + (j * 16), 8);
        vernum = *(uint64_t*)(buf + (j * 16) + 8);

        if(args->log_num == 0 && j == 0) {
            assert(!strncmp(key_from_log.c_str(), "LEVEL___", 8));
            (*args->level) = *(uint16_t*)(buf + (j * 16) + 8);
            //printf("got %s %u\n", key_from_log.c_str(), (*args->level)); 
            continue;
        }

        if(!strncmp("LEVEL___", key_from_log.c_str(), 8)) {
            continue;
        }

        fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                "Got %s %lu in bid %lu log_num %u", 
                key_from_log.c_str(), vernum,  
                bid, log_num);

        if(vernum == BLK_NOT_FOUND) {
            args->expired_pairs[log_num].insert(key_from_log);
            continue;
        }

        auto entry = args->pairs[log_num].find(key_from_log);
        if(entry != args->pairs[log_num].end()) {
            entry->second = vernum;
        } else {
            args->pairs[log_num].insert({key_from_log, vernum});
        }

    }

end:
    if(last) {
        entered = true;
        args->lat->entered_reduce = get_monotonic_ts_us();

        std::set<std::string> exp = std::set<std::string>();
        for(unsigned int i = 0; i < max_logs; i++) {

            /*
             * Logs were read before being deleted
             */

            if(timestamps[i] < timestamps[0]) {
                fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                        "log %u was too old bid %lu (%lu %lu)", 
                        i, bid, timestamps[i], timestamps[0]);
                continue;
            }

            atomic_incr_uint32_t(args->valid);

            for(auto &it: args->expired_pairs[i]) {
                exp.insert(it);
            }
        }

        for(unsigned int i = 0; i < max_logs; i++) {
            if(timestamps[i] < timestamps[0]) {
                continue;
            }

            for(auto &it: args->pairs[i]) {
                if(exp.count(it.first) == 0) {
                    auto entry = args->ret->find(it.first);
                    if(entry != args->ret->end()) {
                        entry->second = it.second;
                    } else {
                        args->ret->insert({it.first, it.second});
                    }
                } else {
                    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                            "%s skipped in merge", it.first.c_str()); 
                }
            }
        }

        if(latest_log && !latest_log->empty()) {
            (*args->level) = *(uint16_t*)(latest_log->c_str() + 8);

            for(unsigned int j = 1; j < latest_log->length() / log_entry_size; j++) {
                std::string key_from_log = latest_log->substr(j * 16, 8); 
                uint64_t val_from_log = *(uint64_t*)(latest_log->c_str() + (j * 16) + 8);

                if(!strncmp("LEVEL___", key_from_log.c_str(), 8)) {
                    assert(0);
                    continue;
                }

                if(val_from_log == BLK_NOT_FOUND) {
                    args->ret->erase(key_from_log);
                    continue;
                    assert(0);
                }

                (*args->ret)[key_from_log] = val_from_log;
            } 
        }

        uint64_t t1, t2;
        t1 = get_monotonic_ts_us();
        mutex_lock(&file->logs_per_node_lock);
        t2 = get_monotonic_ts_us();
        args->lat->waited_for_lock = t2 - t1;
        //assert(args->valid->load() == file->logs_per_node->find(bid)->second);
        file->logs_per_node->insert({bid, atomic_get_uint32_t(args->valid)});
        mutex_unlock(&file->logs_per_node_lock);
        fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                "%u valid for bid %lu\n", atomic_get_uint32_t(args->valid), bid);
        args->lat->left_reduce = get_monotonic_ts_us();
    }

    if(args->cb) {
        atomic_decr_uint16_t(args->complete);
    }

    if(last && args->cb) {
        while(atomic_get_uint16_t(args->complete) > 0) {
            usleep(1);
        }
    }

    if(args->cb && last) {
        fflush(stdout);
        args->cb(args->ret, args->voidargs, buf, nested);
        put_large_buf(buf);
        delete args->ret;
        delete args->outstanding_reads;
        delete args->valid;
        delete[] args->pairs;
        delete[] args->expired_pairs;
        delete latest_log;
        delete args->complete;
        free(args->lat->read_lats);
        free(args->lat);
        free(timestamps);
    } else if(args->cb) {
        put_large_buf(buf);
    }

    if(!args->cb) {
        atomic_decr_uint16_t(args->complete);
    }

    free(args);
}

#ifndef __KVSSD_EMU
void _filemgr_collect_logs_cb(struct nvme_passthru_kv_cmd cmd, 
                              void *voidargs)
{
    uint64_t len;
    char *buf;
    struct callback_args* cb_args;
    struct collect_logs_cb_args *args;

    len = cmd.data_length;
    buf = (char*)((void*) cmd.data_addr);
    cb_args = (struct callback_args*)voidargs;
    args = (struct collect_logs_cb_args*)cb_args->args;

    __filemgr_collect_logs_cb(args, len, buf, cb_args->nested);
    free(cb_args);
}
#else
void _filemgr_collect_logs_cb(kvs_postprocess_context* ioctx) 
{
    uint64_t len;
    char *buf;
    struct callback_args* cb_args;
    struct collect_logs_cb_args *args;

    len = ioctx->value->actual_value_size;
    buf = (char*)(ioctx->value->value);
    cb_args = (struct callback_args*)ioctx->private1;
    args = (struct collect_logs_cb_args*)cb_args->args;

    __filemgr_collect_logs_cb(args, len, buf, cb_args->nested);
    free(cb_args);
    _ioctx_free_func(ioctx, true, true);
}
#endif

struct filemgr_collect_node_cb_args
{
    struct filemgr *file;
    struct bnode* node;
    uint16_t *level;
    uint64_t offset;
    struct filemgr_read_cb_args *fmgr_cb_args;
};

std::unordered_map<std::string, uint64_t> buf_to_ret(void *buf, uint16_t *level) {
    std::unordered_map<std::string, uint64_t> ret;
    struct bnode *bnode = btree_get_bnode(buf);
    char* data = (char*)bnode->data;
    uint64_t vernum;

    for(int i = 0; i < bnode->nentry; i++) {
        std::string k = std::string(data + (i * 16), 8);
        vernum = *(uint64_t*)(data + (i * 16) + 8);
        ret.insert({k, vernum});
    }

    *level = bnode->level;

    return ret;
}

std::unordered_map<std::string, uint64_t> 
_filemgr_collect_logs(struct filemgr *file, uint64_t bid, 
                      uint16_t *level, std::string *latest_log,
                      void (*cb)(std::unordered_map<std::string, uint64_t>*, void*, void*, bool), 
                      void *voidargs,
                      bool nested)
{
    void *buf;
    atomic_uint32_t *valid;
    uint32_t max_logs;
    atomic_uint16_t *outstanding_reads;
    atomic_uint16_t *complete;
    uint64_t *timestamps;
    int cached;

    max_logs = file->config->max_logs_per_node + 1;
    auto pairs = new std::unordered_map<std::string, uint64_t>[max_logs];
    auto expired_pairs = new std::set<std::string>[max_logs];
    auto ret = new std::unordered_map<std::string, uint64_t>();
    timestamps = (uint64_t*)malloc(sizeof(uint64_t) * max_logs);
    void *bufs[max_logs];

    for(unsigned int i = 0; i < max_logs; i++) {
        timestamps[i] = 0;
    }

    valid = new atomic_uint32_t();
    outstanding_reads = new atomic_uint16_t();
    complete = new atomic_uint16_t();

    atomic_init_uint32_t(valid, 0);
    atomic_init_uint16_t(outstanding_reads, max_logs);
    atomic_init_uint16_t(complete, max_logs);

    if(latest_log && !latest_log->empty()) {
        assert(latest_log->length() > 16);
    }

    //bool all_cached = true;

    struct lat_tracking *lat;
    struct lat_tracking lat_static;

    if(!cb) {
        lat = &lat_static;
    } else {
        lat = (struct lat_tracking*)malloc(sizeof(*lat));
    }
    memset(lat, 0x0, sizeof(*lat));
    lat->read_lats = (uint64_t*)malloc(sizeof(uint64_t) * max_logs);

    for(uint32_t i = 0; i < max_logs; i++) {
        struct callback_args *kvssd_cb_args;
        struct collect_logs_cb_args *collect_args;

        kvssd_cb_args = 
            (struct callback_args*)malloc(sizeof(struct callback_args));
        kvssd_cb_args->cb = _filemgr_collect_logs_cb;
        
        collect_args = (struct collect_logs_cb_args*)malloc(sizeof(*collect_args)); 
        collect_args->file = file;
        collect_args->valid = valid;
        collect_args->log_num = i;
        collect_args->level = level;
        collect_args->outstanding_reads = outstanding_reads;
        collect_args->complete = complete;
        collect_args->pairs = pairs;
        collect_args->expired_pairs = expired_pairs;
        collect_args->ret = ret;
        collect_args->bid = bid;
        collect_args->cb = cb;
        collect_args->voidargs = voidargs;
        collect_args->latest_log = latest_log;
        collect_args->timestamps = timestamps;
        collect_args->lat = lat;
        collect_args->lat->read_lats[i] = get_monotonic_ts_us();

        kvssd_cb_args->args = collect_args;

        buf = get_large_buf();
        bufs[i] = buf;

        if(nested) assert(0);

        cached = kvcache_read_log(file, bid, buf, i);
        if(cached) {
#ifndef __KVSSD_EMU
            struct nvme_passthru_kv_cmd dummy;
            memset(&dummy, 0x0, sizeof(dummy));
            dummy.data_length = cached;
            dummy.data_addr = (uint64_t) buf;
            dummy.result = 0;
            kvssd_cb_args->nested = false;
            _filemgr_collect_logs_cb(dummy, kvssd_cb_args);
#else
            _filemgr_collect_logs_cb(NULL);
#endif
        } else if(nested) {
            assert(0);
        } else {
            retrieve(file, bid, i, buf, kvssd_cb_args);
        }

        if(i == 0) {
            lat->first_read_sub = get_monotonic_ts_us();
        } else if(i == max_logs - 1) {
            lat->last_read_sub = get_monotonic_ts_us();
        }
    }

done:
    if(!cb) {
        while(atomic_get_uint16_t(complete) > 0) {
            usleep(1);
        }

        for(uint16_t i = 0; i < max_logs; i++) {
            put_large_buf(bufs[i]);
        }

        delete outstanding_reads;
        delete valid;
        delete[] pairs;
        delete[] expired_pairs;
        delete complete;
        free(timestamps);
    
        if(lat->left_reduce - lat->first_read_sub > 100000000) {
            printf("\nTID %lu *** COLLECTION TOOK %luus. %lu TO SUBMIT ALL.\n"
                   "%lu FROM LAST SUBMISSION TO ENTER REDUCE.\n"
                   "%lu TIME SPENT REDUCING. WAITED %lu FOR LOCK.\n"
                   "READ_LATS: %lu %lu %lu %lu %lu\n",
                   pthread_self(),
                   lat->left_reduce - lat->first_read_sub,
                   lat->last_read_sub - lat->first_read_sub,
                   lat->entered_reduce - lat->last_read_sub,
                   lat->left_reduce - lat->entered_reduce,
                   lat->waited_for_lock,
                   lat->read_lats[0], lat->read_lats[1], lat->read_lats[2],
                   lat->read_lats[3], lat->read_lats[4]);
            check_lat = 1;
        }
        free(lat->read_lats);

        auto tmp = std::unordered_map<std::string, uint64_t>(*ret);
        delete ret;
        return tmp;
    } else {
        return std::unordered_map<std::string, uint64_t>();
    }
}

void filemgr_clear_logs(struct filemgr *file) {
    file->outstanding_logs->clear();
}

/*
 * We do not want write_log to block on log compaction, and instead
 * make it asynchronous so other log writes can proceed in the meantime.
 * The process is as follows : logs are collected asynchronously with
 * retrieves in _filemgr_collect_logs, and each log deducts an atomic counter
 * in the callback. When a callback deducts the counter and it equals 0, it
 * is the last log to be collected. The log is then compacted and passed back
 * to filemgr_write_log.
 */

struct _filemgr_write_log_args {
    struct filemgr *file;
    uint64_t bid;
    std::string *log;
    bool sync;
    atomic_uint64_t *count;
};

struct _compact_logs_args {
    struct filemgr *file;
    atomic_uint64_t *log_write_count; // how many logs to be written in filemgr_write_log
    uint64_t bid;
    std::string *ret;
    std::string *log;
    uint16_t *level;
};

void _compact_logs_cb(std::unordered_map<std::string, uint64_t> *log, 
                      void *voidargs,
                      void* buf,
                      bool nested)
{
    struct filemgr *file;
    uint64_t bid;
    struct _compact_logs_args *args;
    uint16_t level;
    std::string *ret;
    uint64_t vernum;
    std::string latest_log;

    args = (struct _compact_logs_args*)voidargs;

    file = args->file;
    bid = args->bid;
    ret = args->ret;
    level = *args->level;
    ret->append("LEVEL___", 8);

    uint64_t tmp_level = (uint64_t)level;
    ret->append((char*)&tmp_level, 8);
    assert(level < 5);

    for(auto i: *log) {
        ret->append(i.first);
        vernum = i.second;
        assert(vernum != BLK_NOT_FOUND);
        ret->append((char*)&vernum, 8);
    }

    assert(ret->length() > 16);

    mutex_lock(&file->logs_per_node_lock);
    (*file->logs_per_node)[bid] = 0;
    mutex_unlock(&file->logs_per_node_lock);

    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
            "In repack CB for %lu\n", bid);

    /*
     * Because we are continually adding logs to data_in_kvssd in filemgr_write_log,
     * we remove all the log data from data_in_kvssd for this node here. This means
     * that there is a brief period of time before the logs are re-written where
     * data_in_kvssd is lower than reality. However, when measuring space amplification
     * we take the highest seen value throughout and entire benchmark, not
     * the lowest.
     */

    atomic_sub_uint64_t(&file->data_in_kvssd, 
                        1024 * file->config->max_logs_per_node);

    filemgr_write_log(file, bid, ret, true, NULL, true);

    delete args->level;
    delete args->log;
    atomic_decr_uint64_t(args->log_write_count);
    free(args);
}

std::string _compact_log(struct filemgr *file, uint64_t bid, atomic_uint64_t *count, bool sync)
{
    uint16_t *level;
    struct _compact_logs_args *args;
    auto ret = new std::string();

    level = new uint16_t();
    args = (struct _compact_logs_args*)malloc(sizeof(*args));

    args->file = file;
    args->log_write_count = count;
    args->bid = bid;
    args->ret = ret;
    args->log = NULL;
    args->level = level;

    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
            "Compacting %lu\n", bid);

    char _buf[LOG_RETRIEVE_LENGTH];
    if(bcache_read(file, bid, _buf)) {
        fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                "%lu was cached\n", bid);
        auto reduced = buf_to_ret(_buf, args->level);
        _compact_logs_cb(&reduced, args, NULL, false);
    } else {
        auto log_map = _filemgr_collect_logs(file, bid, level, NULL, _compact_logs_cb, args, false);
    }
    
    return std::string();
}

struct _write_log_cb_args {
    struct filemgr *file;
    uint64_t bid; 
    std::string *log;
    uint32_t log_num;
    atomic_uint64_t *count;
};

void __write_log_cb(struct _write_log_cb_args *args) {
    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
            "Write log CB for bid %lu log_num %u",
            args->bid, args->log_num);
    delete args->log;
    if(args->count) {
        atomic_decr_uint64_t(args->count);
    }
}

#ifndef __KVSSD_EMU
void _write_log_cb(struct nvme_passthru_kv_cmd cmd, void* voidargs)
{
    struct callback_args *kvssd_cb_args;
    struct _write_log_cb_args *args;

    kvssd_cb_args = (struct callback_args*)voidargs;
    args = (struct _write_log_cb_args*)kvssd_cb_args->args;

    __write_log_cb(args);

    put_large_buf((void*) cmd.data_addr);
    free(args);
    free(kvssd_cb_args);
}
#else
void _write_log_cb(kvs_postprocess_context* ioctx) 
{
    struct callback_args *cb_args;
    struct _write_log_cb_args *args;

    cb_args = (struct callback_args*)ioctx->private1;
    args = (struct _write_log_cb_args*)cb_args->args;

    __write_log_cb(args);

    _ioctx_free_func(ioctx, true, true);
    free(cb_args);
    free(args);
}
#endif

void filemgr_write_log(struct filemgr *file, uint64_t bid, std::string *log, 
                       bool sync, atomic_uint64_t *count, bool purge)
{
    int ksize = file->config->bnode_ksize;
    int vsize = file->config->bnode_vsize;
    int kvsize = ksize + vsize;

    uint32_t nlog = log->size() / kvsize;
    char *ch_log = const_cast<char*>(log->c_str());
    std::string log_slice;
    void *buf;

    uint32_t nbytes = 0;
    uint32_t log_num;
    uint64_t timestamp;
    uint64_t vlen;

    if(nlog == 0) {
        assert(0);
    }

    while (nlog > 0) {
        int n = 0;
        if ((purge && ((nlog - 1) > MAX_ENTRIES_PER_LOG_WRITE)) ||
            (nlog > MAX_ENTRIES_PER_LOG_WRITE)) {

            auto nlogs = nlog % MAX_ENTRIES_PER_LOG_WRITE == 0 ?
                         nlog / MAX_ENTRIES_PER_LOG_WRITE :
                         (nlog / MAX_ENTRIES_PER_LOG_WRITE) + 1;

			if(nlogs / MAX_ENTRIES_PER_LOG_WRITE > 
			   file->config->max_logs_per_node){
                printf("nlog was %u which is %u logs\n",
                        nlog, nlogs);
				assert(0);
			}

            if(purge) {
                printf("Log too big on compact for bid %lu! Inside log:\n",
                        bid);
                for(int i = 0; i < log->size() / 16; i++) {
                    std::string entry = std::string(log->c_str() + (i * 16), 8);
                    printf("%s\n", entry.c_str()); 
                }
                assert(0);
            }

            n = MAX_ENTRIES_PER_LOG_WRITE;
            nlog -= MAX_ENTRIES_PER_LOG_WRITE;
            atomic_incr_uint64_t(count);
        } else {
            n = nlog;
            nlog = 0;
        }

        /*
         * Key : offset + log number
         * Value : KV pairs + timestamp
         */

        /*
         * logs_per_node is modified here, during a WAL flush, 
         * or at the end of a log compaction.
         * Log writes and log compactions happens in order in the flush thread,
         * and neither can happen when the WAL is flushing, but we still take
         * the log_lock here because logs_per_node can be updated by
         * _filemgr_collect_logs_cb.
         */

        mutex_lock(&file->logs_per_node_lock);
        auto i = file->logs_per_node->find(bid);
        if(i == file->logs_per_node->end()) {
            log_num = 0;
            file->logs_per_node->insert({bid, 1});
            atomic_add_uint64_t(&file->data_in_kvssd, 1024);
            atomic_add_uint64_t(&file->written_to_kvssd, 1024);
        } else {
            log_num = i->second;
            file->logs_per_node->find(bid)->second++;

            if(file->logs_per_node->find(bid)->second >
                   file->config->max_logs_per_node + 1) {
                fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                        "Too many logs bid %lu\n", bid);
            }

            assert(file->logs_per_node->find(bid)->second <= 
                   file->config->max_logs_per_node + 1);

            atomic_add_uint64_t(&file->data_in_kvssd, 1024);
            atomic_add_uint64_t(&file->written_to_kvssd, 1024);
        }

        mutex_unlock(&file->logs_per_node_lock);

        /*
         * Queue this index node for a compaction. to_repack is only
         * written to here when filemgr_write_logs is called from 
         * _log_flush_thread, and is read in filemgr_compact_logs which
         * is only called after filemgr_write_logs.
         */

        if(log_num == file->config->max_logs_per_node) {
            if(log->length() <= 16) {
                printf("log_num == file->config->max_logs_per_node %s", log->c_str());
            }
            assert(std::find(file->to_repack->begin(), 
                  file->to_repack->end(), bid) == file->to_repack->end());
            file->to_repack->push_back(bid);
        }

        timestamp = get_timestamp();
        log_slice = std::string(ch_log, (n * log_entry_size));
        log_slice.append((char*)&timestamp, 8);
        assert(log_slice.length() == ((n * log_entry_size) + 8));
        if(log_num == 0) {
            assert(!strncmp(ch_log, "LEVEL___", 8));
        }

        vlen = log_slice.length();
        assert(vlen <= LOG_RETRIEVE_LENGTH);
        buf = get_large_buf();
        memcpy(buf, log_slice.c_str(), vlen);

        fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                "Writing log %u len %u in %s for %lu with ts %lu. Inside:", 
                log_num, vlen,
                purge ? "compaction" : "normal write", bid, timestamp);

        if(file->config->kv_cache_size > 0) {
            //kvcache_wipe_log(file, bid, log_num); 
            //kvcache_write_log(file, bid, buf, vlen, log_num, KVCACHE_REQ_CLEAN); 
        }

        if(sync) {
            store(file, bid, log_num, buf, vlen, NULL);

            if(!purge) {
                atomic_decr_uint64_t(count);
            } else {
                int ret = file->ops_kvssd->flush();

                if(ret) {
                    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                            "Flushed %d in compaction cb", ret);
                }
            }
        } else {
            struct callback_args *kvssd_cb_args;
            kvssd_cb_args = (struct callback_args*)malloc(sizeof(*kvssd_cb_args));
            struct _write_log_cb_args *args;
            args = (struct _write_log_cb_args*)malloc(sizeof(*args));

            args->file = file;
            args->bid = bid;
            args->log = new std::string(ch_log, vlen - sizeof(uint64_t));
            args->log_num = log_num;
            args->count = count;

            kvssd_cb_args->cb = _write_log_cb;
            kvssd_cb_args->args = args;
            kvssd_cb_args->nested = purge ? false : false;

            store(file, bid, log_num, buf, vlen, kvssd_cb_args);
        }

        free(buf);

        nbytes += LOG_KLEN;
        ch_log += (n * kvsize);
    }

    if(purge) {
        for(int i = 1; i < file->config->max_logs_per_node + 1; i++) {
            //kvcache_wipe_log(file, bid, i); 
        }
        //file->log_del_lock.lock();
        //file->logs_to_remove->push_back(bid);
        //file->log_del_lock.unlock();
    }

    delete log;
}

/*
 * Will also clear the logs once all of the logs have been written
 */

void filemgr_write_logs(struct filemgr *file, bool sync,
                        std::unordered_map<uint64_t, std::string> to_flush)
{
    atomic_uint64_t *count;

    assert(atomic_get_uint8_t(&file->flushing));

    count = new atomic_uint64_t();
    atomic_init_uint64_t(count, to_flush.size());

    if(to_flush.size() > 0) {
        for(auto i: to_flush) {
            filemgr_write_log(file, i.first, new std::string(i.second), 
                              sync, count, false);
        }
    } 

    uint64_t sleep_count = 0;
    while(atomic_get_uint64_t(count) > 0) {
        usleep(1);
        sleep_count++;

        if(sleep_count == 100) {
            file->ops_kvssd->flush();
            sleep_count = 0;
        }
    }

    file->ops_kvssd->flush();
    delete count;
}

void filemgr_compact_logs(struct filemgr *file, bool sync)
{
    atomic_uint64_t *count;

    count = new atomic_uint64_t();
    atomic_init_uint64_t(count, file->to_repack->size());

    for(auto i : *file->to_repack) {
        fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                "Sending repack for %lu\n", i);
        _compact_log(file, i, count, sync);
    }

    int ret = file->ops_kvssd->flush();
    if(ret) {
        fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                "Flushed %d in compact logs!\n", ret);
    }

    uint64_t sleep_count = 0;
    while(atomic_get_uint64_t(count) > 0) {
        usleep(1);
        sleep_count++;

        if(sleep_count == 100) {
            file->ops_kvssd->flush();
            sleep_count = 0;
        }
    }

    delete count;

    file->to_repack->clear();
}

int filemgr_is_writable(struct filemgr *file, bid_t bid)
{
    if (sb_bmp_exists(file->sb) && sb_ops.is_writable) {
        // block reusing is enabled
        return sb_ops.is_writable(file, bid);
    } else {
        uint64_t pos;
        if(file->config->kvssd) {
            pos = bid;

            uint64_t global_commitid, global_vernum;
            uint64_t bid_commitid, bid_vernum;
            filemgr_get_commitid_and_vernum(file, file->header.vernum, &global_commitid, &global_vernum);
            filemgr_get_commitid_and_vernum(file, bid, &bid_commitid, &bid_vernum);

            bool ret = 
            (bid_commitid >= global_commitid && bid_vernum <= global_vernum);

            return ret;
        } else {
            pos = bid * file->blocksize;

            return (pos < atomic_get_uint64_t(&file->pos) &&
                    pos >= atomic_get_uint64_t(&file->last_commit));
        }
        // Note that we don't need to grab file->lock here because
        // 1) both file->pos and file->last_commit are only incremented.
        // 2) file->last_commit is updated using the value of file->pos,
        //    and always equal to or smaller than file->pos.
    }
}

uint64_t filemgr_get_sb_bmp_revnum(struct filemgr *file)
{
    if (file->sb && sb_ops.get_bmp_revnum) {
        return sb_ops.get_bmp_revnum(file);
    } else {
        return 0;
    }
}

static fdb_status _filemgr_read_header(struct filemgr *file,
                                       err_log_callback *log_callback)
{
    uint8_t marker[BLK_MARKER_SIZE];
    filemgr_magic_t magic = ver_get_latest_magic();
    filemgr_header_len_t len;
    uint8_t *buf;
    uint32_t crc, crc_file;
    bool check_crc32_open_rule = false;
    fdb_status status = FDB_RESULT_SUCCESS;
    bid_t hdr_bid, hdr_bid_local;
    size_t min_filesize = 0;

    // get temp buffer
    buf = (uint8_t *) get_buf();

    // If a header is found crc_mode can change to reflect the file
    if (file->crc_mode == CRC32) {
        check_crc32_open_rule = true;
    }

    if(file->config->kvssd) {
        magic = FILEMGR_MAGIC_003;
        retrieve(file, MILESTONE_K, &hdr_bid, NULL);
    } else {
        hdr_bid = atomic_get_uint64_t(&file->pos) / file->blocksize - 1;
    }
    hdr_bid_local = hdr_bid;

    if (file->sb) {
        // superblock exists .. file size does not start from zero.
        if(file->config->kvssd) {
            min_filesize = file->sb->config->num_sb;
        } else {
            min_filesize = file->sb->config->num_sb * file->blocksize;
        }
        bid_t sb_last_hdr_bid = atomic_get_uint64_t(&file->sb->last_hdr_bid);
        if (sb_last_hdr_bid != BLK_NOT_FOUND) {
            hdr_bid = hdr_bid_local = sb_last_hdr_bid;
        }
        // if header info does not exist in superblock,
        // get DB header at the end of the file.
    }

    if (atomic_get_uint64_t(&file->pos) > min_filesize) {
        if(!file->config->kvssd) {
            // Crash Recovery Test 1: unaligned last block write
            uint64_t remain = atomic_get_uint64_t(&file->pos) % file->blocksize;
            if (remain) {
                atomic_sub_uint64_t(&file->pos, remain);
                atomic_store_uint64_t(&file->last_commit, atomic_get_uint64_t(&file->pos));
                const char *msg = "Crash Detected: %" _F64 " non-block aligned bytes discarded "
                    "from a database file '%s'\n";
                DBG(msg, remain, file->filename);
                fdb_log(log_callback, FDB_LOG_WARNING,
                        FDB_RESULT_READ_FAIL /* Need to add a better error code*/,
                        msg, remain, file->filename);
            }
        }

        size_t block_counter = 0;
        do {
            if (hdr_bid_local > file->pos) {
                // Handling EOF scenario
                status = FDB_RESULT_NO_DB_HEADERS;
                const char *msg = "Unable to read block from file '%s' as EOF "
                    "reached\n";
                fdb_log(log_callback, FDB_LOG_ERROR, status,
                        msg, file->filename);
                break;
            }

            ssize_t rv = filemgr_read_block(file, NULL, buf, hdr_bid_local);
            if (rv != (ssize_t)file->blocksize) {
                status = (fdb_status) rv;
                const char *msg = "Unable to read a database file '%s' with "
                                  "blocksize %u\n";
                DBG(msg, file->filename, file->blocksize);
                fdb_log(log_callback, FDB_LOG_ERROR, status,
                        msg, file->filename, file->blocksize);
                break;
            }
            ++block_counter;
            memcpy(marker, buf + file->blocksize - BLK_MARKER_SIZE,
                   BLK_MARKER_SIZE);

            if (marker[0] == BLK_MARKER_DBHEADER) {
                // possible need for byte conversions here
                memcpy(&magic,
                       buf + file->blocksize - BLK_MARKER_SIZE - sizeof(magic),
                       sizeof(magic));
                magic = _endian_decode(magic);

                if (ver_is_valid_magic(magic)) {

                    memcpy(&len,
                           buf + file->blocksize - BLK_MARKER_SIZE -
                           sizeof(magic) - sizeof(len),
                           sizeof(len));
                    len = _endian_decode(len);

                    memcpy(&crc_file, buf + len - sizeof(crc), sizeof(crc));
                    crc_file = _endian_decode(crc_file);

                    // crc check and detect the crc_mode
                    if (detect_and_check_crc(reinterpret_cast<const uint8_t*>(buf),
                                             len - sizeof(crc),
                                             crc_file,
                                             &file->crc_mode)) {
                        // crc mode is detected and known.
                        // check the rules of opening legacy CRC
                        if (check_crc32_open_rule && file->crc_mode != CRC32) {
                            const char *msg = "Open of CRC32C file"
                                              " with forced CRC32\n";
                            status = FDB_RESULT_INVALID_ARGS;
                            DBG(msg);
                            fdb_log(log_callback, FDB_LOG_WARNING, status, msg);
                            break;
                        } else {
                            status = FDB_RESULT_SUCCESS;

                            file->header.data = (void *)malloc(file->blocksize);

                            memcpy(file->header.data, buf, len);
                            memcpy(&file->header.revnum, buf + len,
                                   sizeof(filemgr_header_revnum_t));
                            memcpy((void *) &file->header.seqnum,
                                    buf + len + sizeof(filemgr_header_revnum_t),
                                    sizeof(fdb_seqnum_t));

                            if (ver_superblock_support(magic)) {
                                // last_writable_bmp_revnum should be same with
                                // the current bmp_revnum (since it indicates the
                                // 'bmp_revnum' of 'sb->cur_alloc_bid').
                                atomic_store_uint64_t(&file->last_writable_bmp_revnum,
                                                      filemgr_get_sb_bmp_revnum(file));
                            }

                            file->header.revnum =
                                _endian_decode(file->header.revnum);
                            file->header.seqnum =
                                _endian_decode(file->header.seqnum.load());

                            if(file->config->kvssd &&
                                file->header.revnum != file->header.vernum >> 32) {
                                file->header.vernum = 0;
                            }

                            file->header.size = len;
                            atomic_store_uint64_t(&file->header.bid, hdr_bid_local);
                            memset(&file->header.stat, 0x0, sizeof(file->header.stat));

                            // release temp buffer
                            put_buf(buf);
                        }

                        file->version = magic;
                        return status;
                    } else {
                        status = FDB_RESULT_CHECKSUM_ERROR;
                        uint32_t crc32 = 0, crc32c = 0;
                        crc32 = get_checksum(reinterpret_cast<const uint8_t*>(buf),
                                             len - sizeof(crc),
                                             CRC32);
#ifdef _CRC32C
                        crc32c = get_checksum(reinterpret_cast<const uint8_t*>(buf),
                                              len - sizeof(crc),
                                              CRC32C);
#endif
                        const char *msg = "Crash Detected: CRC on disk %u != (%u | %u) "
                            "in a database file '%s'\n";
                        DBG(msg, crc_file, crc32, crc32c, file->filename);
                        fdb_log(log_callback, FDB_LOG_WARNING, status,
                                msg, crc_file, crc32, crc32c,
                                file->filename);
                    }
                } else {
                    status = FDB_RESULT_FILE_CORRUPTION;
                    const char *msg = "Crash Detected: Wrong Magic %" _F64
                                      " in a database file '%s'\n";
                    fdb_log(log_callback, FDB_LOG_WARNING, status,
                            msg, magic, file->filename);
                }
            } else {
                status = FDB_RESULT_NO_DB_HEADERS;
                if (block_counter == 1) {
                    const char *msg = "Crash Detected: Last Block not DBHEADER %0.01x "
                                      "in a database file '%s'\n";
                    DBG(msg, marker[0], file->filename);
                    fdb_log(log_callback, FDB_LOG_WARNING, status,
                            msg, marker[0], file->filename);
                }
            }

            atomic_store_uint64_t(&file->last_commit, hdr_bid_local * file->blocksize);
            // traverse headers in a circular manner
            if (hdr_bid_local) {
                hdr_bid_local--;
            } else {
                hdr_bid_local = atomic_get_uint64_t(&file->pos) / file->blocksize - 1;
            }
        } while (hdr_bid_local != hdr_bid);
    }

    // release temp buffer
    put_buf(buf);

    file->header.size = 0;
    file->header.revnum = 0;
    file->header.seqnum = 0;
    file->header.data = NULL;
    atomic_store_uint64_t(&file->header.bid, 0);
    memset(&file->header.stat, 0x0, sizeof(file->header.stat));
    file->version = magic;
    return status;
}

size_t filemgr_get_ref_count(struct filemgr *file)
{
    size_t ret = 0;
    spin_lock(&file->lock);
    ret = atomic_get_uint32_t(&file->ref_count);
    spin_unlock(&file->lock);
    return ret;
}

uint64_t filemgr_get_bcache_used_space(void)
{
    uint64_t bcache_free_space = 0;
    if (global_config.ncacheblock) { // If buffer cache is indeed configured
        bcache_free_space = bcache_get_num_free_blocks();
        bcache_free_space = (global_config.ncacheblock - bcache_free_space)
                          * global_config.blocksize;
    }
    return bcache_free_space;
}

uint64_t filemgr_get_kvcache_used_space(void)
{
    return kvcache_get_used_space();
}

struct filemgr_prefetch_args {
    struct filemgr *file;
    uint64_t duration;
    err_log_callback *log_callback;
    void *aux;
};

static void *_filemgr_prefetch_thread(void *voidargs)
{
    struct filemgr_prefetch_args *args = (struct filemgr_prefetch_args*)voidargs;
    uint8_t *buf = alca(uint8_t, args->file->blocksize);
    uint64_t cur_pos = 0, i;
    uint64_t bcache_free_space;
    bid_t bid;
    bool terminate = false;
    struct timeval begin, cur, gap;

    spin_lock(&args->file->lock);
    cur_pos = atomic_get_uint64_t(&args->file->last_commit);
    spin_unlock(&args->file->lock);
    if (cur_pos < FILEMGR_PREFETCH_UNIT) {
        terminate = true;
    } else {
        cur_pos -= FILEMGR_PREFETCH_UNIT;
    }
    // read backwards from the end of the file, in the unit of FILEMGR_PREFETCH_UNIT
    gettimeofday(&begin, NULL);
    while (!terminate) {
        for (i = cur_pos;
             i < cur_pos + FILEMGR_PREFETCH_UNIT;
             i += args->file->blocksize) {

            gettimeofday(&cur, NULL);
            gap = _utime_gap(begin, cur);
            bcache_free_space = bcache_get_num_free_blocks();
            bcache_free_space *= args->file->blocksize;

            if (atomic_get_uint8_t(&args->file->prefetch_status)
                == FILEMGR_PREFETCH_ABORT ||
                gap.tv_sec >= (int64_t)args->duration ||
                bcache_free_space < FILEMGR_PREFETCH_UNIT) {
                // terminate thread when
                // 1. got abort signal
                // 2. time out
                // 3. not enough free space in block cache
                terminate = true;
                break;
            } else {
                bid = i / args->file->blocksize;
                if (filemgr_read(args->file, bid, buf, NULL, NULL, true)
                        != FDB_RESULT_SUCCESS) {
                    // 4. read failure
                    fdb_log(args->log_callback, FDB_LOG_ERROR, FDB_RESULT_READ_FAIL,
                            "Prefetch thread failed to read a block with block id %" _F64
                            " from a database file '%s'", bid, args->file->filename);
                    terminate = true;
                    break;
                }
            }
        }

        if (cur_pos >= FILEMGR_PREFETCH_UNIT) {
            cur_pos -= FILEMGR_PREFETCH_UNIT;
        } else {
            // remaining space is less than FILEMGR_PREFETCH_UNIT
            terminate = true;
        }
    }

    atomic_cas_uint8_t(&args->file->prefetch_status, FILEMGR_PREFETCH_RUNNING,
                       FILEMGR_PREFETCH_IDLE);
    free(args);
    return NULL;
}

// prefetch the given DB file
void filemgr_prefetch(struct filemgr *file,
                      struct filemgr_config *config,
                      err_log_callback *log_callback)
{
    uint64_t bcache_free_space;

    bcache_free_space = bcache_get_num_free_blocks();
    bcache_free_space *= file->blocksize;

    // block cache should have free space larger than FILEMGR_PREFETCH_UNIT
    spin_lock(&file->lock);
    if (atomic_get_uint64_t(&file->last_commit) > 0 &&
        bcache_free_space >= FILEMGR_PREFETCH_UNIT) {
        // invoke prefetch thread
        struct filemgr_prefetch_args *args;
        args = (struct filemgr_prefetch_args *)
               calloc(1, sizeof(struct filemgr_prefetch_args));
        args->file = file;
        args->duration = config->prefetch_duration;
        args->log_callback = log_callback;

        if (atomic_cas_uint8_t(&file->prefetch_status, FILEMGR_PREFETCH_IDLE,
                               FILEMGR_PREFETCH_RUNNING)) {
            thread_create(&file->prefetch_tid, _filemgr_prefetch_thread, args);
        }
    }
    spin_unlock(&file->lock);
}

fdb_status filemgr_does_file_exist(char *filename) {
    struct filemgr_ops *ops = get_filemgr_ops();
    int fd = ops->open(filename, O_RDONLY, 0444);
    if (fd < 0) {
        return (fdb_status) fd;
    }
    ops->close(fd);
    return FDB_RESULT_SUCCESS;
}

static fdb_status _filemgr_load_sb(struct filemgr *file,
                                   err_log_callback *log_callback)
{
    fdb_status status = FDB_RESULT_SUCCESS;
    struct sb_config sconfig;

    if (sb_ops.init && sb_ops.get_default_config && sb_ops.read_latest) {
        sconfig = sb_ops.get_default_config();
        if (filemgr_get_pos(file)) {
            // existing file
            status = sb_ops.read_latest(file, sconfig, log_callback);
        } else {
            // new file
            status = sb_ops.init(file, sconfig, log_callback);
        }
    }

    return status;
}

static filemgr* get_instance_UNLOCKED(const char *filename)
{
    if (!filename) {
        return NULL;
    }

    struct filemgr query;
    struct hash_elem *e = NULL;
    struct filemgr *file = NULL;

    query.filename = (char*)filename;
    e = hash_find(&hash, &query.e);
    if (e) {
        file = _get_entry(e, struct filemgr, e);
    }
    return file;
}

struct filemgr* filemgr_get_instance(const char* filename)
{
    spin_lock(&filemgr_openlock);
    struct filemgr *file = get_instance_UNLOCKED(filename);
    spin_unlock(&filemgr_openlock);

    return file;
}

double _space_amp(struct filemgr *file)
{
    fdb_file_info info;

    size_t datasize = _kvs_stat_get_sum(file, KVS_STAT_DATASIZE);
    size_t ret = datasize;
    ret += wal_get_datasize(file);
    ret += atomic_get_uint64_t(&file->logs_occupied_space);

    size_t wal_docs = wal_get_num_docs(file);
    size_t wal_deletes = wal_get_num_deletes(file);
    size_t wal_n_inserts = wal_docs - wal_deletes;

    size_t ndocs = _kvs_stat_get_sum(file, KVS_STAT_NDOCS);

    if (ndocs + wal_n_inserts < wal_deletes) {
        info.doc_count = 0;
    } else {
        if (ndocs) {
            info.doc_count = ndocs + wal_n_inserts - wal_deletes;
        } else {
            info.doc_count = wal_n_inserts;
        }
    }

    size_t ndeletes = _kvs_stat_get_sum(file, KVS_STAT_NDELETES);
    if (ndeletes) { // not accurate since some ndeletes may be wal_deletes
        info.deleted_count = ndeletes + wal_deletes;
    } else { // this is accurate since it reflects only wal_ndeletes
        info.deleted_count = wal_deletes;
    }

    info.space_used = ret; 
    
    return (double) info.space_used / info.file_size;
}

void __pair_delete_cb(struct filemgr *file) {
    atomic_sub_uint64_t(&file->data_in_kvssd, VALUE_RETRIEVE_LENGTH - 1024 + sizeof(uint64_t));
}

#ifndef __KVSSD_EMU
void _pair_delete_cb(struct nvme_passthru_kv_cmd cmd, void *voidargs)
{
    struct callback_args *kvssd_cb_args;
    struct filemgr *file;

    kvssd_cb_args = (struct callback_args*)voidargs;
    file = (struct filemgr*)kvssd_cb_args->args;

    __pair_delete_cb(file);

    assert(cmd.result == 0);
    free(kvssd_cb_args);
}
#else
void _pair_delete_cb(kvs_postprocess_context* ioctx) 
{
    struct callback_args *cb_args;
    struct filemgr *file;

    cb_args = (struct callback_args*)ioctx->private1;
    file = (struct filemgr*)cb_args->args;

    __pair_delete_cb(file);
    _ioctx_free_func(ioctx, true, true);
    free(cb_args);
}
#endif

std::mutex deleted_lock;
std::unordered_set<uint64_t> deleted;
void *_pair_delete_thread(void* voidargs) {
    struct filemgr *file;
    uint64_t vernum = BLK_NOT_FOUND;
    struct callback_args *kvssd_cb_args;

    file = (struct filemgr*)voidargs;

    while(1) {
        file->open_snapshots_lock.lock();
        if(file->open_snapshots->size() > 0) {
            file->open_snapshots_lock.unlock();
            sleep(1);
            continue;
        }
        file->open_snapshots_lock.unlock();

        file->pairs_to_remove->wait_dequeue_timed(vernum, 
                                                  std::chrono::seconds(1));

        if(vernum == BLK_NOT_FOUND) {
            fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                    "Delete queue empty");
            goto shutdown;
        } else if(0 && file->pairs_to_remove->size_approx() == 0) {
            atomic_store_uint8_t(&file->deletes_done, 1);
            goto shutdown;
        }

       fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                "Sending delete for %lu (%lu %lu)", 
                vernum, vernum >> 32, vernum & 0xFFFFFFFF);

        kvssd_cb_args =
        (struct callback_args*)malloc(sizeof(struct callback_args));
        kvssd_cb_args->args = (void*) file;
        kvssd_cb_args->cb = _pair_delete_cb;
        del(file, vernum, kvssd_cb_args);
        vernum = BLK_NOT_FOUND;

        _kvs_stat_update_attr(file, 0,
                KVS_STAT_DATASIZE, -2000);

shutdown:
        if(atomic_get_uint8_t(&file->flush_shutdown)) {
            if(file->pairs_to_remove->size_approx() > 0) {
                continue;
            }
            return NULL;
        }
    }

    return NULL;
}

#include "btree.h"
void * _log_flush_thread(void *voidargs) {
    struct filemgr *file;
    bool just_flushed;

    file = (struct filemgr*)voidargs;
    just_flushed = false;

start:
    std::unique_lock<std::mutex> lock(file->flush_lock);
    while(!file->ready) {
        file->flush_cond.wait_for(lock, std::chrono::seconds(1));

        if(atomic_get_uint8_t(&file->flush_shutdown)) {
            atomic_store_uint8_t(&file->flushing, 0);
            return NULL;
        }
    }

    assert(atomic_get_uint8_t(&file->flushing) == 0);
    atomic_store_uint8_t(&file->flushing, 1);

    mutex_lock(&file->outstanding_log_lock);
    auto logs_to_flush = *file->outstanding_logs;
    filemgr_clear_logs(file);
    mutex_unlock(&file->outstanding_log_lock);
    file->log_flushing = true;
    filemgr_write_logs(file, false, logs_to_flush);
    filemgr_compact_logs(file, false);
    file->log_flushing = false;
    file->ready = false;

    atomic_store_uint8_t(&file->flushing, 0);
    lock.unlock();

    if(atomic_get_uint8_t(&file->flush_shutdown)) {
        return NULL;
    }

    goto start;

    return NULL;
}

filemgr_open_result filemgr_open(char *filename, struct filemgr_ops *ops,
        struct filemgr_config *config,
        err_log_callback *log_callback)
{
    struct filemgr *file = NULL;
    struct filemgr query;
    struct hash_elem *e = NULL;
    bool create = config->options & FILEMGR_CREATE;
    int file_flag = 0x0;
    int fd = -1;
    fdb_status status;
    filemgr_open_result result = {NULL, FDB_RESULT_OPEN_FAIL};

    filemgr_init(config);

    if (config->encryption_key.algorithm != FDB_ENCRYPTION_NONE 
            && global_config.ncacheblock <= 0
            && global_config.kv_cache_size <= 0) {
        // cannot use encryption without a block cache
        result.rv = FDB_RESULT_CRYPTO_ERROR;
        return result;
    }

    // check whether file is already opened or not
    query.filename = filename;
    spin_lock(&filemgr_openlock);
    e = hash_find(&hash, &query.e);

    if (e) {
        // already opened (return existing structure)
        file = _get_entry(e, struct filemgr, e);

        if (atomic_incr_uint32_t(&file->ref_count) > 1 &&
                atomic_get_uint8_t(&file->status) != FILE_CLOSED) {
            file->ops_kvssd->open("/dev/nvme0n1", NULL);
            spin_unlock(&filemgr_openlock);
            result.file = file;
            result.rv = FDB_RESULT_SUCCESS;
            return result;
        }

        spin_lock(&file->lock);

        if (atomic_get_uint8_t(&file->status) == FILE_CLOSED) { // if file was closed before
            file_flag = O_RDWR;
            if (create) {
                file_flag |= O_CREAT;
            }
            *file->config = *config;
            file->config->blocksize = global_config.blocksize;
            file->config->index_blocksize = global_config.index_blocksize;
            file->config->ncacheblock = global_config.ncacheblock;
            file_flag |= config->flag;
            file->fd = file->ops->open(file->filename, file_flag, 0666);
            if(file->config->kvssd) {
                file->ops_kvssd->open("/dev/nvme0n1", NULL);
            }
            if (file->fd < 0) {
                if (file->fd == FDB_RESULT_NO_SUCH_FILE) {
                    // A database file was manually deleted by the user.
                    // Clean up global hash table, WAL index, and buffer cache.
                    // Then, retry it with a create option below IFF it is not
                    // a read-only open attempt
                    struct hash_elem *ret;
                    spin_unlock(&file->lock);
                    ret = hash_remove(&hash, &file->e);
                    fdb_assert(ret, 0, 0);
                    filemgr_free_func(&file->e);
                    // free(file);
                    // file->ops_kvssd->close();
                    if (!create) {
                        _log_errno_str(ops, log_callback,
                                FDB_RESULT_NO_SUCH_FILE, "OPEN", filename);
                        spin_unlock(&filemgr_openlock);
                        result.rv = FDB_RESULT_NO_SUCH_FILE;
                        return result;
                    }
                } else {
                    _log_errno_str(file->ops, log_callback,
                            (fdb_status)file->fd, "OPEN", filename);
                    atomic_decr_uint32_t(&file->ref_count);
                    spin_unlock(&file->lock);
                    spin_unlock(&filemgr_openlock);
                    result.rv = file->fd;
                    return result;
                }
            } else { // Reopening the closed file is succeed.
                atomic_store_uint8_t(&file->status, FILE_NORMAL);
                if (config->options & FILEMGR_SYNC) {
                    file->fflags |= FILEMGR_SYNC;
                } else {
                    file->fflags &= ~FILEMGR_SYNC;
                }

                spin_unlock(&file->lock);
                spin_unlock(&filemgr_openlock);

                result.file = file;
                result.rv = FDB_RESULT_SUCCESS;
                return result;
            }
        } else { // file is already opened.

            if (config->options & FILEMGR_SYNC) {
                file->fflags |= FILEMGR_SYNC;
            } else {
                file->fflags &= ~FILEMGR_SYNC;
            }

            spin_unlock(&file->lock);
            spin_unlock(&filemgr_openlock);
            result.file = file;
            result.rv = FDB_RESULT_SUCCESS;
            return result;
        }
    }

    file_flag = O_RDWR;
    if (create) {
        file_flag |= O_CREAT;
    }
    file_flag |= config->flag;
    fd = ops->open(filename, file_flag, 0666);
    if (fd < 0) {
        _log_errno_str(ops, log_callback, (fdb_status)fd, "OPEN", filename);
        spin_unlock(&filemgr_openlock);
        result.rv = fd;
        return result;
    }
    file = (struct filemgr*)calloc(1, sizeof(struct filemgr));
    file->filename_len = strlen(filename);
    file->filename = (char*)malloc(file->filename_len + 1);
    strcpy(file->filename, filename);

#ifndef __KVSSD_EMU
    set_n_aio(global_config.num_aio_workers);
    set_qd(global_config.max_outstanding);
#endif

    if(config->kvssd) {
        file->ops_kvssd = get_filemgr_ops_kvssd();
#ifdef __KVSSD_EMU
        file->ops_kvssd->open(config->kvssd_dev_path, 
                              config->kvssd_emu_config_file);
#else
        file->ops_kvssd->open("/dev/nvme0n1", NULL);
#endif
    }

    atomic_init_uint32_t(&file->ref_count, 1);
    file->stale_list = NULL;

    status = fdb_init_encryptor(&file->encryption, &config->encryption_key);
    if (status != FDB_RESULT_SUCCESS) {
        ops->close(fd);
        free(file);
        spin_unlock(&filemgr_openlock);
        result.rv = status;
        return result;
    }

    file->wal = (struct wal *)calloc(1, sizeof(struct wal));
    file->wal->flag = 0;

    file->ops = ops;
    file->blocksize = global_config.blocksize;
    file->index_blocksize = global_config.index_blocksize;
    atomic_init_uint8_t(&file->status, FILE_NORMAL);
    file->config = (struct filemgr_config*)malloc(sizeof(struct filemgr_config));
    *file->config = *config;
    file->config->blocksize = global_config.blocksize;
    file->config->index_blocksize = global_config.index_blocksize;
    file->config->ncacheblock = global_config.ncacheblock;
    file->old_filename = NULL;
    file->new_filename = NULL;
    file->fd = fd;
    file->config->kvssd = global_config.kvssd;
    file->config->kv_cache_size = global_config.kv_cache_size;
    file->config->split_threshold = global_config.split_threshold;
    file->config->max_log_threshold = global_config.max_log_threshold;
    file->config->max_outstanding = global_config.max_outstanding;
    file->config->kvssd_retrieve_length = global_config.kvssd_retrieve_length;
    file->config->num_aio_workers = global_config.num_aio_workers;
    file->config->max_logs_per_node = global_config.max_logs_per_node;

    VALUE_RETRIEVE_LENGTH = file->config->kvssd_retrieve_length + 1024;
    if(VALUE_RETRIEVE_LENGTH > MAX_VALUE_SIZE) {
        VALUE_RETRIEVE_LENGTH = MAX_VALUE_SIZE;
    }
    LOG_RETRIEVE_LENGTH = global_config.index_blocksize;
    MAX_ENTRIES_PER_LOG_WRITE = (LOG_RETRIEVE_LENGTH - log_meta_length) / log_entry_size;
    MAX_OUTSTANDING_REQUESTS = file->config->max_outstanding;

    fdb_log(NULL, FDB_LOG_INFO, FDB_RESULT_SUCCESS,
            "Number of AIO workers: %u\n", file->config->num_aio_workers);
    fdb_log(NULL, FDB_LOG_INFO, FDB_RESULT_SUCCESS,
            "Number of max outstanding requests: %u\n", MAX_OUTSTANDING_REQUESTS);

    cs_off_t offset = 0;
    if(config->kvssd) {
        retrieve(file, MILESTONE_K, &offset, NULL);
        filemgr_set_vernum(file, offset);
        atomic_store_uint64_t(&file->pos, offset + 1);
    } else {
        offset = file->ops->goto_eof(file->fd);
    }

    if (offset < 0) {
        _log_errno_str(file->ops, log_callback, (fdb_status) offset, "SEEK_END", filename);
        file->ops->close(file->fd);
        free(file->wal);
        free(file->filename);
        free(file->config);
        free(file);
        spin_unlock(&filemgr_openlock);
        result.rv = (fdb_status) offset;
        return result;
    }
    atomic_init_uint64_t(&file->last_commit, offset);
    atomic_init_uint64_t(&file->last_writable_bmp_revnum, 0);
    atomic_init_uint64_t(&file->pos, offset);
    atomic_init_uint32_t(&file->throttling_delay, 0);
    atomic_init_uint64_t(&file->num_invalidated_blocks, 0);
    atomic_init_uint8_t(&file->io_in_prog, 0);
    atomic_init_uint64_t(&file->btree_handle_idx, 0);

    file->max_pending_handles = MAX_OUTSTANDING_REQUESTS;
    atomic_init_uint64_t(&file->busy_handles_idx, 0);
    atomic_init_uint64_t(&file->finished_handles, 0);
    atomic_init_uint64_t(&file->sent_handles, 1);

    file->open_snapshots = new std::unordered_set<uint64_t>();
    file->sync_mode = 0;

    atomic_init_uint8_t(&file->flushing, 0);

    file->writes_this_commit = 0;

    atomic_init_uint8_t(&file->flush_shutdown, 0);
    file->outstanding_logs = new std::unordered_map<uint64_t, std::string>();
    file->entries_per_node = new std::unordered_map<uint64_t, int16_t>();
    file->to_repack = new std::vector<uint64_t>();

    file->logs_to_remove = new std::deque<uint64_t>();
    file->pairs_to_remove = 
    new BlockingConcurrentQueue<uint64_t>(128);
 
    thread_create(&file->flush_thread, _log_flush_thread, (void*) file);
    thread_create(&file->pair_del_thread, _pair_delete_thread, (void*) file);

    void *buf = malloc(sizeof(uint64_t));
    memset(buf, 0x0, sizeof(uint64_t));
    retrieve(file, "DATA", buf, NULL);
    atomic_store_uint64_t(&file->data_in_kvssd, *(uint64_t*) buf);
    memset(buf, 0x0, sizeof(uint64_t));
    retrieve(file, "UDATA", buf, NULL);
    atomic_store_uint64_t(&file->user_data , *(uint64_t*) buf);
    free(buf);

    mutex_init(&file->entries_lock);
    mutex_init(&file->logs_per_node_lock);
    mutex_init(&file->outstanding_log_lock);
    prefix.i = -1;
    file->logs_per_node = new std::map<uint64_t, uint64_t>();
    file->this_flush_leaves_updated = new std::unordered_set<uint64_t>();

#ifdef _LATENCY_STATS
    for (int i = 0; i < FDB_LATENCY_NUM_STATS; ++i) {
        filemgr_init_latency_stat(&file->lat_stats[i]);
    }
#endif // _LATENCY_STATS

    file->bcache = NULL;
    file->kvcache = NULL;
    file->in_place_compaction = false;
    file->kv_header = NULL;
    atomic_init_uint8_t(&file->prefetch_status, FILEMGR_PREFETCH_IDLE);

    atomic_init_uint64_t(&file->header.bid, 0);
    _init_op_stats(&file->header.op_stat);

    spin_init(&file->lock);
    file->stale_list = (struct list*)calloc(1, sizeof(struct list));
    list_init(file->stale_list);
    avl_init(&file->stale_info_tree, NULL);
    avl_init(&file->mergetree, NULL);
    file->stale_info_tree_loaded = false;

    filemgr_dirty_update_init(file);

    spin_init(&file->fhandle_idx_lock);
    avl_init(&file->fhandle_idx, NULL);

#ifdef __FILEMGR_DATA_PARTIAL_LOCK
    struct plock_ops pops;
    struct plock_config pconfig;

    pops.init_user = mutex_init_wrap;
    pops.lock_user = mutex_lock_wrap;
    pops.unlock_user = mutex_unlock_wrap;
    pops.destroy_user = mutex_destroy_wrap;
    pops.init_internal = spin_init_wrap;
    pops.lock_internal = spin_lock_wrap;
    pops.unlock_internal = spin_unlock_wrap;
    pops.destroy_internal = spin_destroy_wrap;
    pops.is_overlapped = _block_is_overlapped;
    (void)_block_is_overlapped;
    (void)_addr_is_overlapped;

    memset(&pconfig, 0x0, sizeof(pconfig));
    pconfig.ops = &pops;
    pconfig.sizeof_lock_internal = sizeof(spin_t);
    pconfig.sizeof_lock_user = sizeof(mutex_t);
    pconfig.sizeof_range = sizeof(bid_t);
    pconfig.aux = NULL;
    plock_init(&file->plock, &pconfig);
#elif defined(__FILEMGR_DATA_MUTEX_LOCK)
    int i;
    for (i=0;i<DLOCK_MAX;++i) {
        mutex_init(&file->data_mutex[i]);
    }
#else
    int i;
    for (i=0;i<DLOCK_MAX;++i) {
        spin_init(&file->data_spinlock[i]);
    }
#endif //__FILEMGR_DATA_PARTIAL_LOCK

    mutex_init(&file->writer_lock.mutex);
    file->writer_lock.locked = false;

    // Note: CRC must be initialized before superblock loading
    // initialize CRC mode
    if (file->config && file->config->options & FILEMGR_CREATE_CRC32) {
        file->crc_mode = CRC32;
    } else {
        file->crc_mode = CRC_DEFAULT;
    }

    do { // repeat until both superblock and DB header are correctly read
        // init or load superblock
        status = _filemgr_load_sb(file, log_callback);
        // we can tolerate SB_READ_FAIL for old version file
        if (status != FDB_RESULT_SB_READ_FAIL &&
            status != FDB_RESULT_SUCCESS) {
            _log_errno_str(file->ops, log_callback, status, "READ", file->filename);
            file->ops->close(file->fd);
            free(file->stale_list);
            free(file->wal);
            free(file->filename);
            free(file->config);
            free(file);
            spin_unlock(&filemgr_openlock);
            result.rv = status;
            return result;
        }

        // read header
        status = _filemgr_read_header(file, log_callback);
        if (file->sb && status == FDB_RESULT_NO_DB_HEADERS) {
            // this happens when user created & closed a file without any mutations,
            // thus there is no other data but superblocks.
            // we can tolerate this case.
        } else if (status != FDB_RESULT_SUCCESS) {
            _log_errno_str(file->ops, log_callback, status, "READ", filename);
            file->ops->close(file->fd);
            if (file->sb) {
                sb_ops.release(file);
            }
            free(file->stale_list);
            free(file->wal);
            free(file->filename);
            free(file->config);
            free(file);
            spin_unlock(&filemgr_openlock);
            result.rv = status;
            return result;
        }

        if (file->sb &&
            file->header.revnum != atomic_get_uint64_t(&file->sb->last_hdr_revnum)) {
            // superblock exists but the corresponding DB header does not match.
            // read another candidate.
            continue;
        }

        break;
    } while (true);

    // initialize WAL
    if (!wal_is_initialized(file)) {
        wal_init(file, FDB_WAL_NBUCKET);
    }

    // init global transaction for the file
    file->global_txn.wrapper = (struct wal_txn_wrapper*)
                               malloc(sizeof(struct wal_txn_wrapper));
    file->global_txn.wrapper->txn = &file->global_txn;
    file->global_txn.handle = NULL;
    if (atomic_get_uint64_t(&file->pos)) {
        file->global_txn.prev_hdr_bid =
            (atomic_get_uint64_t(&file->pos) / file->blocksize) - 1;
    } else {
        file->global_txn.prev_hdr_bid = BLK_NOT_FOUND;
    }
    file->global_txn.prev_revnum = 0;
    file->global_txn.items = (struct list *)malloc(sizeof(struct list));
    list_init(file->global_txn.items);
    file->global_txn.isolation = FDB_ISOLATION_READ_COMMITTED;
    wal_add_transaction(file, &file->global_txn);

    hash_insert(&hash, &file->e);
    if (config->prefetch_duration > 0) {
        filemgr_prefetch(file, config, log_callback);
    }

    spin_unlock(&filemgr_openlock);

    if (config->options & FILEMGR_SYNC) {
        file->fflags |= FILEMGR_SYNC;
    } else {
        file->fflags &= ~FILEMGR_SYNC;
    }

    result.file = file;
    result.rv = FDB_RESULT_SUCCESS;
    fdb_log(log_callback, FDB_LOG_INFO, FDB_RESULT_SUCCESS,
            "Forestdb opened database file %s",
            filename);

    return result;
}

uint64_t filemgr_update_header(struct filemgr *file,
                               void *buf,
                               size_t len,
                               bool inc_revnum)
{
    uint64_t ret;

    spin_lock(&file->lock);

    if (file->header.data == NULL) {
        file->header.data = (void *)malloc(file->blocksize);
    }
    memcpy(file->header.data, buf, len);
    file->header.size = len;
    if (inc_revnum) {
        ++(file->header.revnum);

        if(file->config->kvssd) {
            filemgr_set_vernum(file, file->header.revnum << 32);
        }

    }
    ret = file->header.revnum;

    spin_unlock(&file->lock);

    return ret;
}

filemgr_header_revnum_t filemgr_get_header_revnum(struct filemgr *file)
{
    filemgr_header_revnum_t ret;
    spin_lock(&file->lock);
    ret = file->header.revnum;
    spin_unlock(&file->lock);
    return ret;
}

// 'filemgr_get_seqnum', 'filemgr_set_seqnum',
// 'filemgr_get_walflush_revnum', 'filemgr_set_walflush_revnum'
// have to be protected by 'filemgr_mutex_lock' & 'filemgr_mutex_unlock'.
fdb_seqnum_t filemgr_get_seqnum(struct filemgr *file)
{
    return file->header.seqnum;
}

void filemgr_set_seqnum(struct filemgr *file, fdb_seqnum_t seqnum)
{
    file->header.seqnum = seqnum;
}

fdb_seqnum_t filemgr_assign_new_seqnum(struct filemgr *file)
{
    return atomic_incr_uint64_t(&file->header.seqnum);
}

fdb_seqnum_t filemgr_get_current_vernum(struct filemgr *file)
{
   return (file->header.revnum << 32) | file->header.vernum;
}

fdb_seqnum_t filemgr_assign_new_vernum(struct filemgr *file)
{
    spin_lock(&file->lock);
    uint64_t revnum = file->header.revnum << 32;
    uint64_t vernum = atomic_incr_uint64_t(&file->header.vernum);
    spin_unlock(&file->lock);

    return revnum | vernum;
}

void filemgr_set_vernum(struct filemgr *file, fdb_seqnum_t vernum)
{
   file->header.vernum = vernum;
}

void filemgr_get_commitid_and_vernum(struct filemgr *file, uint64_t vernum,
                                     uint64_t *commitid, uint64_t *seqnum) {
    *commitid = vernum >> 32;
    *seqnum = vernum & 0xFFFFFFFF;
}

void* filemgr_get_header(struct filemgr *file, void *buf, size_t *len,
                         bid_t *header_bid, fdb_seqnum_t *seqnum,
                         filemgr_header_revnum_t *header_revnum)
{
    spin_lock(&file->lock);

    if (file->header.size > 0) {
        if (buf == NULL) {
            buf = (void*)malloc(file->header.size);
        }
        memcpy(buf, file->header.data, file->header.size);
    }

    if (len) {
        *len = file->header.size;
    }
    if (header_bid) {
        *header_bid = filemgr_get_header_bid(file);
    }
    if (seqnum) {
        *seqnum = file->header.seqnum;
    }
    if (header_revnum) {
        *header_revnum = file->header.revnum;
    }

    spin_unlock(&file->lock);

    return buf;
}

fdb_status filemgr_fetch_header(struct filemgr *file, uint64_t bid,
                                void *buf, size_t *len, fdb_seqnum_t *seqnum,
                                filemgr_header_revnum_t *header_revnum,
                                uint64_t *deltasize, uint64_t *version,
                                uint64_t *sb_bmp_revnum,
                                err_log_callback *log_callback)
{
    uint8_t *_buf;
    uint8_t marker[BLK_MARKER_SIZE];
    filemgr_header_len_t hdr_len;
    uint64_t _deltasize, _bmp_revnum;
    filemgr_magic_t magic;
    fdb_status status = FDB_RESULT_SUCCESS;

    *len = 0;

    if (!bid || bid == BLK_NOT_FOUND) {
        // No other header available
        return FDB_RESULT_SUCCESS;
    }

    _buf = (uint8_t *)get_buf();

    status = filemgr_read(file, (bid_t)bid, _buf, NULL, log_callback, true);

    if (status != FDB_RESULT_SUCCESS) {
        fdb_log(log_callback, FDB_LOG_WARNING, status,
                "Failed to read a database header with block id %" _F64 " in "
                "a database file '%s'", bid, file->filename);
        put_buf(_buf);
        return status;
    }
    memcpy(marker, _buf + file->blocksize - BLK_MARKER_SIZE,
            BLK_MARKER_SIZE);

    if (marker[0] != BLK_MARKER_DBHEADER) {
        // Comment this warning log as of now because the circular block reuse
        // can cause false alarms as a previous stale header block can be reclaimed
        // and reused for incoming writes.
        /*
        fdb_log(log_callback, FDB_RESULT_FILE_CORRUPTION,
                "A block marker of the database header block id %" _F64 " in "
                "a database file '%s' does NOT match BLK_MARKER_DBHEADER!",
                bid, file->filename);
        */
        put_buf(_buf);
        return FDB_RESULT_READ_FAIL;
    }
    memcpy(&magic,
            _buf + file->blocksize - BLK_MARKER_SIZE - sizeof(magic),
            sizeof(magic));
    magic = _endian_decode(magic);
    if (!ver_is_valid_magic(magic)) {
        fdb_log(log_callback, FDB_LOG_WARNING, FDB_RESULT_FILE_CORRUPTION,
                "A block magic value of %" _F64 " in the database header block"
                "id %" _F64 " in a database file '%s'"
                "does NOT match FILEMGR_MAGIC %" _F64 "!",
                magic, bid, file->filename, ver_get_latest_magic());
        put_buf(_buf);
        return FDB_RESULT_FILE_CORRUPTION;
    }
    memcpy(&hdr_len,
            _buf + file->blocksize - BLK_MARKER_SIZE - sizeof(magic) -
            sizeof(hdr_len), sizeof(hdr_len));
    hdr_len = _endian_decode(hdr_len);

    memcpy(buf, _buf, hdr_len);
    *len = hdr_len;
    *version = magic;

    if (header_revnum) {
        // copy the DB header revnum
        filemgr_header_revnum_t _revnum;
        memcpy(&_revnum, _buf + hdr_len, sizeof(_revnum));
        *header_revnum = _endian_decode(_revnum);
    }
    if (seqnum) {
        // copy default KVS's seqnum
        fdb_seqnum_t _seqnum;
        memcpy(&_seqnum, _buf + hdr_len + sizeof(filemgr_header_revnum_t),
               sizeof(_seqnum));
        *seqnum = _endian_decode(_seqnum);
    }

    if (ver_is_atleast_magic_001(magic)) {
        if (deltasize) {
            memcpy(&_deltasize, _buf + file->blocksize - BLK_MARKER_SIZE
                    - sizeof(magic) - sizeof(hdr_len) - sizeof(bid)
                    - sizeof(_deltasize), sizeof(_deltasize));
            *deltasize = _endian_decode(_deltasize);
        }
    }

    if (sb_bmp_revnum && ver_superblock_support(magic)) {
        memcpy(&_bmp_revnum, _buf + file->blocksize - BLK_MARKER_SIZE
                - sizeof(magic) - sizeof(hdr_len) - sizeof(bid)
                - sizeof(_deltasize) - sizeof(_bmp_revnum), sizeof(_bmp_revnum));
        *sb_bmp_revnum = _endian_decode(_bmp_revnum);
    }

    put_buf(_buf);

    return status;
}

uint64_t filemgr_fetch_prev_header(struct filemgr *file, uint64_t bid,
                                   void *buf, size_t *len, fdb_seqnum_t *seqnum,
                                   filemgr_header_revnum_t *revnum,
                                   uint64_t *deltasize, uint64_t *version,
                                   uint64_t *sb_bmp_revnum,
                                   err_log_callback *log_callback)
{
    uint8_t *_buf;
    uint8_t marker[BLK_MARKER_SIZE];
    fdb_seqnum_t _seqnum;
    filemgr_header_revnum_t _revnum, cur_revnum, prev_revnum;
    filemgr_header_len_t hdr_len;
    filemgr_magic_t magic;
    bid_t _prev_bid, prev_bid;
    uint64_t _deltasize, _bmp_revnum;
    int found = 0;

    *len = 0;

    if (!bid || bid == BLK_NOT_FOUND) {
        // No other header available
        return bid;
    }
    _buf = (uint8_t *)get_buf();

    // Reverse scan the file for a previous DB header
    do {
        // Get prev_bid from the current header.
        // Since the current header is already cached during the previous
        // operation, no disk I/O will be triggered.
        if (filemgr_read(file, (bid_t)bid, _buf, NULL, log_callback, true)
                != FDB_RESULT_SUCCESS) {
            break;
        }

        memcpy(marker, _buf + file->blocksize - BLK_MARKER_SIZE,
               BLK_MARKER_SIZE);
        memcpy(&magic,
               _buf + file->blocksize - BLK_MARKER_SIZE - sizeof(magic),
               sizeof(magic));
        magic = _endian_decode(magic);

        if (marker[0] != BLK_MARKER_DBHEADER ||
            !ver_is_valid_magic(magic)) {
            // not a header block
            // this happens when this function is invoked between
            // fdb_set() call and fdb_commit() call, so the last block
            // in the file is not a header block
            bid_t latest_hdr = filemgr_get_header_bid(file);
            if (latest_hdr != BLK_NOT_FOUND && bid > latest_hdr) {
                // get the latest header BID
                bid = latest_hdr;
            } else {
                break;
            }
            cur_revnum = file->header.revnum + 1;
        } else {

            memcpy(&hdr_len,
                   _buf + file->blocksize - BLK_MARKER_SIZE - sizeof(magic) -
                   sizeof(hdr_len), sizeof(hdr_len));
            hdr_len = _endian_decode(hdr_len);

            memcpy(&_revnum, _buf + hdr_len,
                   sizeof(filemgr_header_revnum_t));
            cur_revnum = _endian_decode(_revnum);

            if (sb_bmp_exists(file->sb)) {
                // first check revnum
                if (cur_revnum <= sb_ops.get_min_live_revnum(file)) {
                    // previous headers already have been reclaimed
                    // no more logical prev header
                    break;
                }
            }

            memcpy(&_prev_bid,
                   _buf + file->blocksize - BLK_MARKER_SIZE - sizeof(magic) -
                       sizeof(hdr_len) - sizeof(_prev_bid),
                   sizeof(_prev_bid));
            prev_bid = _endian_decode(_prev_bid);
            bid = prev_bid;
        }

        // Read the prev header
        fdb_status fs = filemgr_read(file, (bid_t)bid, _buf, NULL, log_callback, true);
        if (fs != FDB_RESULT_SUCCESS) {
            fdb_log(log_callback, FDB_LOG_WARNING, fs,
                    "Failed to read a previous database header with block id %"
                    _F64 " in "
                    "a database file '%s'", bid, file->filename);
            break;
        }

        memcpy(marker, _buf + file->blocksize - BLK_MARKER_SIZE,
               BLK_MARKER_SIZE);
        if (marker[0] != BLK_MARKER_DBHEADER) {
            if (bid) {
                // broken linked list
                fdb_log(log_callback, FDB_LOG_DEBUG, FDB_RESULT_FILE_CORRUPTION,
                        "A block marker of the previous database header block id %"
                        _F64 " in "
                        "a database file '%s' does NOT match BLK_MARKER_DBHEADER!",
                        bid, file->filename);
            }
            break;
        }

        memcpy(&magic,
               _buf + file->blocksize - BLK_MARKER_SIZE - sizeof(magic),
               sizeof(magic));
        magic = _endian_decode(magic);
        if (!ver_is_valid_magic(magic)) {
            // broken linked list
            fdb_log(log_callback, FDB_LOG_WARNING, FDB_RESULT_FILE_CORRUPTION,
                    "A block magic value of %" _F64
                    " of the previous database header block id %" _F64 " in "
                    "a database file '%s' does NOT match FILEMGR_MAGIC %"
                    _F64"!", magic,
                    bid, file->filename, ver_get_latest_magic());
            break;
        }

        memcpy(&hdr_len,
               _buf + file->blocksize - BLK_MARKER_SIZE - sizeof(magic) -
               sizeof(hdr_len), sizeof(hdr_len));
        hdr_len = _endian_decode(hdr_len);

        if (buf) {
            memcpy(buf, _buf, hdr_len);
        }
        memcpy(&_revnum, _buf + hdr_len,
               sizeof(filemgr_header_revnum_t));
        prev_revnum = _endian_decode(_revnum);
        if (prev_revnum >= cur_revnum ||
            prev_revnum < sb_ops.get_min_live_revnum(file)) {
            // no more prev header, or broken linked list
            break;
        }

        memcpy(&_seqnum,
               _buf + hdr_len + sizeof(filemgr_header_revnum_t),
               sizeof(fdb_seqnum_t));
        if (ver_is_atleast_magic_001(magic)) {
            if (deltasize) {
                memcpy(&_deltasize,
                        _buf + file->blocksize - BLK_MARKER_SIZE - sizeof(magic)
                       - sizeof(hdr_len) - sizeof(prev_bid) - sizeof(_deltasize),
                        sizeof(_deltasize));
                *deltasize = _endian_decode(_deltasize);
            }
        }

        if (sb_bmp_revnum && ver_superblock_support(magic)) {
            memcpy(&_bmp_revnum, _buf + file->blocksize - BLK_MARKER_SIZE
                    - sizeof(magic) - sizeof(hdr_len) - sizeof(bid)
                    - sizeof(_deltasize) - sizeof(_bmp_revnum), sizeof(_bmp_revnum));
            *sb_bmp_revnum = _endian_decode(_bmp_revnum);
        }

        if (revnum) {
            *revnum = prev_revnum;
        }
        *seqnum = _endian_decode(_seqnum);
        *len = hdr_len;
        *version = magic;
        found = 1;
        break;
    } while (false); // no repetition

    if (!found) { // no other header found till end of file
        *len = 0;
        bid = BLK_NOT_FOUND;
    }

    put_buf(_buf);

    return bid;
}

fdb_status filemgr_close(struct filemgr *file, bool cleanup_cache_onclose,
                         const char *orig_file_name,
                         err_log_callback *log_callback)
{
    int rv = FDB_RESULT_SUCCESS;

    if (atomic_decr_uint32_t(&file->ref_count) > 0) {
        // File is still accessed by other readers or writers.
        return FDB_RESULT_SUCCESS;
    }

    fdb_log(log_callback, FDB_LOG_INFO, (fdb_status)rv,
            "Forestdb closed database file %s",
            file->filename);

    spin_lock(&filemgr_openlock); // Grab the filemgr lock to avoid the race with
                                  // filemgr_open() because file->lock won't
                                  // prevent the race condition.

    // remove filemgr structure if no thread refers to the file
    spin_lock(&file->lock);
    if (atomic_get_uint32_t(&file->ref_count) == 0) {
        if ((global_config.ncacheblock > 0 || (global_config.kv_cache_size > 0)) &&
            atomic_get_uint8_t(&file->status) != FILE_REMOVED_PENDING) {

            spin_unlock(&file->lock);
            if(global_config.ncacheblock > 0) {
                // discard all dirty blocks belonged to this file
                bcache_remove_dirty_blocks(file);
            }

            if(global_config.kv_cache_size > 0) {
                // discard all dirty blocks belonged to this file
                kvcache_flush(file);
            }
        } else {
            // If the file is in pending removal (i.e., FILE_REMOVED_PENDING),
            // then its dirty block entries will be cleaned up in either
            // filemgr_free_func() or register_file_removal() below.
            spin_unlock(&file->lock);
        }

        if (wal_is_initialized(file)) {
            wal_close(file, log_callback);
        }
#ifdef _LATENCY_STATS_DUMP_TO_FILE
        filemgr_dump_latency_stat(file, log_callback);
#endif // _LATENCY_STATS_DUMP_TO_FILE

        while(atomic_cas_uint8_t(&file->flushing, 0, 1) == 0) {
            usleep(1);
        }

        auto logs_to_flush = *file->outstanding_logs;
        filemgr_write_logs(file, false, logs_to_flush);
        filemgr_compact_logs(file, false);

        atomic_store_uint8_t(&file->flushing, 0);

        atomic_store_uint8_t(&file->flush_shutdown, 1);
        while(atomic_get_uint8_t(&file->flushing) == 1) {
            usleep(1);
        }

        _signal_flush(file);
        thread_join(file->flush_thread, NULL);
        atomic_store_uint8_t(&file->log_del_shutdown, 1);
        thread_join(file->flush_thread, NULL);
        thread_join(file->pair_del_thread, NULL);

        auto data = atomic_get_uint64_t(&file->data_in_kvssd);
        store(file, "DATA", &data, sizeof(uint64_t), NULL);
        auto udata = atomic_get_uint64_t(&file->user_data);
        store(file, "UDATA", &udata, sizeof(uint64_t), NULL);

        spin_lock(&file->lock);
        file->ops_kvssd->flush();

        delete file->open_snapshots;
        delete file->to_repack;
        delete file->outstanding_logs;
        delete file->logs_to_remove;
        delete file->pairs_to_remove;
        delete file->entries_per_node;
        delete file->logs_per_node;

        delete file->this_flush_leaves_updated;

        if (atomic_get_uint8_t(&file->status) == FILE_REMOVED_PENDING) {

            bool foreground_deletion = false;
            struct filemgr* new_file = get_instance_UNLOCKED(file->new_filename);

            // immediately remove file if background remove function is not set
            if (!lazy_file_deletion_enabled ||
                (new_file && new_file->in_place_compaction)) {
                // TODO: to avoid the scenario below, we prevent background
                //       deletion of in-place compacted files at this time.
                // 1) In-place compacted from 'A' to 'A.1'.
                // 2) Request to delete 'A'.
                // 3) Close 'A.1'; since 'A' is not deleted yet, 'A.1' is not renamed.
                // 4) User opens DB file using its original name 'A', not 'A.1'.
                // 5) Old file 'A' is opened, and then background thread deletes 'A'.
                // 6) Crash!

                // As the file is already unlinked, the file will be removed
                // as soon as we close it.
                if(file->config->kvssd) {
                    rv = file->ops_kvssd->close();
                } else {
                    rv = file->ops->close(file->fd);
                }
                _log_errno_str(file->ops, log_callback, (fdb_status)rv, "CLOSE", file->filename);
#if defined(WIN32) || defined(_WIN32)
                // For Windows, we need to manually remove the file.
                remove(file->filename);
#endif
                foreground_deletion = true;
            }

            // we can release lock becuase no one will open this file
            spin_unlock(&file->lock);
            struct hash_elem *ret = hash_remove(&hash, &file->e);
            fdb_assert(ret, 0, 0);

            spin_unlock(&filemgr_openlock);

            if (foreground_deletion) {
                filemgr_free_func(&file->e);
            } else {
                register_file_removal(file, log_callback);
            }
            return (fdb_status) rv;
        } else {
            if(!file->config->kvssd) {
                rv = file->ops->close(file->fd);
            }

            if (cleanup_cache_onclose) {
                _log_errno_str(file->ops, log_callback, (fdb_status)rv, "CLOSE", file->filename);
                if (file->in_place_compaction && orig_file_name) {
                    struct hash_elem *elem = NULL;
                    struct filemgr query;
                    uint32_t old_file_refcount = 0;

                    query.filename = (char *)orig_file_name;
                    elem = hash_find(&hash, &query.e);

                    if (file->old_filename) {
                        struct hash_elem *elem_old = NULL;
                        struct filemgr query_old;
                        struct filemgr *old_file = NULL;

                        // get old file's ref count if exists
                        query_old.filename = file->old_filename;
                        elem_old = hash_find(&hash, &query_old.e);
                        if (elem_old) {
                            old_file = _get_entry(elem_old, struct filemgr, e);
                            old_file_refcount = atomic_get_uint32_t(&old_file->ref_count);
                        }
                    }

                    // If old file is opened by other handle, renaming should be
                    // postponed. It will be renamed later by the handle referring
                    // to the old file.
                    if (!elem && old_file_refcount == 0 &&
                        is_file_removed(orig_file_name)) {
                        // If background file removal is not done yet, we postpone
                        // file renaming at this time.
                        if (rename(file->filename, orig_file_name) < 0) {
                            // Note that the renaming failure is not a critical
                            // issue because the last compacted file will be automatically
                            // identified and opened in the next fdb_open call.
                            _log_errno_str(file->ops, log_callback, FDB_RESULT_FILE_RENAME_FAIL,
                                           "CLOSE", file->filename);
                        }
                    }
                }
                spin_unlock(&file->lock);
                // Clean up global hash table, WAL index, and buffer cache.
                struct hash_elem *ret = hash_remove(&hash, &file->e);
                fdb_assert(ret, file, 0);

                spin_unlock(&filemgr_openlock);

                filemgr_free_func(&file->e);

                return (fdb_status) rv;
            } else {
                atomic_store_uint8_t(&file->status, FILE_CLOSED);
            }

            if(file->config->kvssd) {
                rv = file->ops_kvssd->close();
            }
        }
    }

    _log_errno_str(file->ops, log_callback, (fdb_status)rv, "CLOSE", file->filename);
    spin_unlock(&file->lock);
    spin_unlock(&filemgr_openlock);
    return (fdb_status) rv;
}

void filemgr_remove_all_buffer_blocks(struct filemgr *file)
{
    // remove all cached blocks
    if (global_config.ncacheblock > 0 &&
            file->bcache.load(std::memory_order_relaxed)) {
        bcache_remove_dirty_blocks(file);
        bcache_remove_clean_blocks(file);
        bcache_remove_file(file);
        file->bcache.store(NULL, std::memory_order_relaxed);
    }
}

void _free_fhandle_idx(struct avl_tree *idx);
void filemgr_free_func(struct hash_elem *h)
{
    struct filemgr *file = _get_entry(h, struct filemgr, e);

    filemgr_prefetch_status_t prefetch_state =
                              atomic_get_uint8_t(&file->prefetch_status);

    atomic_store_uint8_t(&file->prefetch_status, FILEMGR_PREFETCH_ABORT);
    if (prefetch_state == FILEMGR_PREFETCH_RUNNING) {
        // prefetch thread was running
        void *ret;
        // wait (the thread must have been created..)
        thread_join(file->prefetch_tid, &ret);
    }


    if(file->config->kvssd) {
        file->ops_kvssd->close();
    }

    // remove all cached blocks
    if (global_config.kv_cache_size > 0 &&
            file->kvcache.load(std::memory_order_relaxed)) {
        kvcache_flush(file);
        kvcache_remove_clean_blocks(file);
        kvcache_remove_file(file);
        file->kvcache.store(NULL, std::memory_order_relaxed);
    }

    if (global_config.ncacheblock > 0 &&
            file->bcache.load(std::memory_order_relaxed)) {
        bcache_remove_dirty_blocks(file);
        bcache_remove_clean_blocks(file);
        bcache_remove_file(file);
        file->bcache.store(NULL, std::memory_order_relaxed);
    }

    if (file->kv_header) {
        // multi KV intance mode & KV header exists
        file->free_kv_header(file);
    }

    // free global transaction
    wal_remove_transaction(file, &file->global_txn);
    free(file->global_txn.items);
    free(file->global_txn.wrapper);

    // destroy WAL
    if (wal_is_initialized(file)) {
        wal_shutdown(file, NULL);
        wal_destroy(file);
    }
    free(file->wal);

#ifdef _LATENCY_STATS
    for (int x = 0; x < FDB_LATENCY_NUM_STATS; ++x) {
        filemgr_destroy_latency_stat(&file->lat_stats[x]);
    }
#endif // _LATENCY_STATS

    // free filename and header
    free(file->filename);
    if (file->header.data) free(file->header.data);

    // free old/new filename if any
    free(file->old_filename);
    free(file->new_filename);

    // destroy locks
    spin_destroy(&file->lock);

#ifdef __FILEMGR_DATA_PARTIAL_LOCK
    plock_destroy(&file->plock);
#elif defined(__FILEMGR_DATA_MUTEX_LOCK)
    int i;
    for (i=0;i<DLOCK_MAX;++i) {
        mutex_destroy(&file->data_mutex[i]);
    }
#else
    int i;
    for (i=0;i<DLOCK_MAX;++i) {
        spin_destroy(&file->data_spinlock[i]);
    }
#endif //__FILEMGR_DATA_PARTIAL_LOCK

    mutex_destroy(&file->writer_lock.mutex);

    // free superblock
    if (sb_ops.release) {
        sb_ops.release(file);
    }

    // free dirty update index
    filemgr_dirty_update_free(file);

    // free fhandle idx
    _free_fhandle_idx(&file->fhandle_idx);
    spin_destroy(&file->fhandle_idx_lock);
    // free file structure
    struct list *stale_list = filemgr_get_stale_list(file);
    filemgr_clear_stale_list(file);
    filemgr_clear_stale_info_tree(file);
    filemgr_clear_mergetree(file);
    free(stale_list);
    free(file->config);
    free(file);
}

// permanently remove file from cache (not just close)
// LCOV_EXCL_START
void filemgr_remove_file(struct filemgr *file, err_log_callback *log_callback)
{
    struct hash_elem *ret;

    if (!file || atomic_get_uint32_t(&file->ref_count) > 0) {
        return;
    }

    // remove from global hash table
    spin_lock(&filemgr_openlock);
    ret = hash_remove(&hash, &file->e);
    fdb_assert(ret, ret, NULL);
    spin_unlock(&filemgr_openlock);

    struct filemgr *new_file = filemgr_get_instance(file->new_filename);

    if (!lazy_file_deletion_enabled ||
        (new_file && new_file->in_place_compaction)) {
        filemgr_free_func(&file->e);
    } else {
        register_file_removal(file, log_callback);
    }
}
// LCOV_EXCL_STOP

static
void *_filemgr_is_closed(struct hash_elem *h, void *ctx) {
    struct filemgr *file = _get_entry(h, struct filemgr, e);
    void *ret;
    spin_lock(&file->lock);
    if (atomic_get_uint32_t(&file->ref_count) != 0) {
        ret = (void *)file;
    } else {
        ret = NULL;
    }
    spin_unlock(&file->lock);
    return ret;
}

fdb_status filemgr_shutdown()
{
    fdb_status ret = FDB_RESULT_SUCCESS;
    void *open_file;
    if (filemgr_initialized) {

#ifndef SPIN_INITIALIZER
        // Windows: check if spin lock is already destroyed.
        if (InterlockedCompareExchange(&initial_lock_status, 1, 2) == 2) {
            spin_lock(&initial_lock);
        } else {
            // filemgr is already shut down
            return ret;
        }
#else
        spin_lock(&initial_lock);
#endif

        if (!filemgr_initialized) {
            // filemgr is already shut down
#ifdef SPIN_INITIALIZER
            spin_unlock(&initial_lock);
#endif
            return ret;
        }

        spin_lock(&filemgr_openlock);
        open_file = hash_scan(&hash, _filemgr_is_closed, NULL);
        spin_unlock(&filemgr_openlock);
        if (!open_file) {
            hash_free_active(&hash, filemgr_free_func);
            if(global_config.kv_cache_size > 0) {
                kvcache_shutdown();
            }

            if (global_config.ncacheblock > 0) {
                bcache_shutdown();
            }
            spin_unlock(&initial_lock);
            filemgr_initialized = 0;

#ifndef SPIN_INITIALIZER
            initial_lock_status = 0;
            spin_destroy(&initial_lock);
#else
            initial_lock = SPIN_INITIALIZER;
#endif
        } else {
            spin_unlock(&initial_lock);
            ret = FDB_RESULT_FILE_IS_BUSY;
        }
    }
    return ret;
}

bid_t filemgr_alloc(struct filemgr *file, err_log_callback *log_callback)
{
    bid_t bid = BLK_NOT_FOUND;

    if(!file->config->kvssd) {
        spin_lock(&file->lock);
    }

    if(file->config->kvssd) {
        bid = filemgr_assign_new_vernum(file);
        atomic_incr_uint64_t(&file->pos);
        return bid;
    } else {
        assert(0);
        // block reusing is not allowed for being compacted file
        // for easy implementation.
        if (filemgr_get_file_status(file) == FILE_NORMAL &&
            file->sb && sb_ops.alloc_block) {
            bid = sb_ops.alloc_block(file);
        }
        if (bid == BLK_NOT_FOUND) {
            bid = atomic_get_uint64_t(&file->pos) / file->blocksize;
            atomic_add_uint64_t(&file->pos, file->blocksize);
        } else {
        }

        if (global_config.ncacheblock <= 0) {
            // if block cache is turned off, write the allocated block before use
            uint8_t _buf = 0x0;
            ssize_t rv = file->ops->pwrite(file->fd, &_buf, 1,
                                           (bid+1) * file->blocksize - 1);
            _log_errno_str(file->ops, log_callback, (fdb_status) rv, "WRITE", file->filename);
        }

        spin_unlock(&file->lock);

        return bid;
    }
}

// Note that both alloc_multiple & alloc_multiple_cond are not used in
// the new version of DB file (with superblock support).
void filemgr_alloc_multiple(struct filemgr *file, int nblock, bid_t *begin,
                            bid_t *end, err_log_callback *log_callback)
{
    assert(0);
    spin_lock(&file->lock);
    *begin = atomic_get_uint64_t(&file->pos) / file->blocksize;
    *end = *begin + nblock - 1;
    atomic_add_uint64_t(&file->pos, file->blocksize * nblock);

    if (global_config.ncacheblock <= 0) {
        // if block cache is turned off, write the allocated block before use
        uint8_t _buf = 0x0;
        ssize_t rv = file->ops->pwrite(file->fd, &_buf, 1,
                                       atomic_get_uint64_t(&file->pos) - 1);
        _log_errno_str(file->ops, log_callback, (fdb_status) rv, "WRITE", file->filename);
    }
    spin_unlock(&file->lock);
}

// atomically allocate NBLOCK blocks only when current file position is same to nextbid
bid_t filemgr_alloc_multiple_cond(struct filemgr *file, bid_t nextbid, int nblock,
                                  bid_t *begin, bid_t *end,
                                  err_log_callback *log_callback)
{
    assert(0);
    bid_t bid;
    spin_lock(&file->lock);
    bid = atomic_get_uint64_t(&file->pos) / file->blocksize;
    if (bid == nextbid) {
        *begin = atomic_get_uint64_t(&file->pos) / file->blocksize;
        *end = *begin + nblock - 1;
        atomic_add_uint64_t(&file->pos, file->blocksize * nblock);

        if (global_config.ncacheblock <= 0) {
            // if block cache is turned off, write the allocated block before use
            uint8_t _buf = 0x0;
            ssize_t rv = file->ops->pwrite(file->fd, &_buf, 1,
                                           atomic_get_uint64_t(&file->pos));
            _log_errno_str(file->ops, log_callback, (fdb_status) rv, "WRITE", file->filename);
        }
    }else{
        *begin = BLK_NOT_FOUND;
        *end = BLK_NOT_FOUND;
    }
    spin_unlock(&file->lock);
    return bid;
}

INLINE fdb_status _filemgr_crc32_check(struct filemgr *file, void *buf)
{
    if ( *((uint8_t*)buf + file->blocksize-1) == BLK_MARKER_BNODE ) {
        uint32_t crc_file = 0;
        memcpy(&crc_file, (uint8_t *) buf + BTREE_CRC_OFFSET, sizeof(crc_file));
        crc_file = _endian_decode(crc_file);
        memset((uint8_t *) buf + BTREE_CRC_OFFSET, 0xff, BTREE_CRC_FIELD_LEN);
        if (!perform_integrity_check(reinterpret_cast<const uint8_t*>(buf),
                                     file->blocksize,
                                     crc_file,
                                     file->crc_mode)) {
            return FDB_RESULT_CHECKSUM_ERROR;
        }
    }
    return FDB_RESULT_SUCCESS;
}

INLINE fdb_status _filemgr_crc32_bnode_check_with_len(struct filemgr *file, void *buf, 
                                                      uint64_t len)
{
    uint32_t crc_file = 0;
    memcpy(&crc_file, (uint8_t *) buf + BTREE_CRC_OFFSET, sizeof(crc_file));
    crc_file = _endian_decode(crc_file);

    memset((uint8_t *) buf + BTREE_CRC_OFFSET, 0xff, BTREE_CRC_FIELD_LEN);
    if (!perform_integrity_check(reinterpret_cast<const uint8_t*>(buf),
                                 len,
                                 crc_file,
                                 file->crc_mode)) {
        return FDB_RESULT_CHECKSUM_ERROR;
    }
    return FDB_RESULT_SUCCESS;
}

bool filemgr_invalidate_block(struct filemgr *file, bid_t bid)
{
    bool ret;
    if (atomic_get_uint64_t(&file->last_commit) < bid) {
        ret = true; // block invalidated was allocated recently (uncommitted)
    } else {
        ret = false; // a block from the past is invalidated (committed)
    }
    if (global_config.ncacheblock > 0) {
        bcache_invalidate_block(file, bid);
    }
    return ret;
}

bool filemgr_is_fully_resident(struct filemgr *file)
{
    bool ret = false;
    if (global_config.ncacheblock > 0) {
        //TODO: A better thing to do is to track number of document blocks
        // and only compare those with the cached document block count
        double num_cached_blocks = (double)bcache_get_num_blocks(file);
        uint64_t num_blocks = atomic_get_uint64_t(&file->pos)
                                 / file->blocksize;
        double num_fblocks = (double)num_blocks;
        if (num_cached_blocks > num_fblocks * FILEMGR_RESIDENT_THRESHOLD) {
            ret = true;
        }
    }
    return ret;
}

uint64_t filemgr_flush_immutable(struct filemgr *file,
                                   err_log_callback *log_callback)
{
    uint64_t ret = 0;

    if(global_config.kv_cache_size > 0) {
        if (atomic_get_uint8_t(&file->io_in_prog)) {
            return 0;
        }
        fdb_status rv = kvcache_flush(file);
        if (rv != FDB_RESULT_SUCCESS) {
            _log_errno_str(file->ops, log_callback, (fdb_status)rv, "WRITE",
                           file->filename);
        }
        return 0;
    }

    if (global_config.ncacheblock > 0) {
        if (atomic_get_uint8_t(&file->io_in_prog)) {
            return 0;
        }
        ret = bcache_get_num_immutable(file);
        if (!ret) {
            return ret;
        }
        fdb_status rv = bcache_flush_immutable(file);
        if (rv != FDB_RESULT_SUCCESS) {
            _log_errno_str(file->ops, log_callback, (fdb_status)rv, "WRITE",
                           file->filename);
        }
        return bcache_get_num_immutable(file);
    }

    return ret;
}

idx_t filemgr_find_bnode_entry(struct filemgr *file, struct bnode *node, void *key) {
    int ksize = file->config->bnode_ksize;
    int vsize = file->config->bnode_vsize;

    idx_t start, end, middle;
    uint8_t *k = alca(uint8_t, ksize);
    int cmp;
#ifdef __BIT_CMP
    idx_t *_map1[3] = {&end, &start, &start};
    idx_t *_map2[3] = {&temp, &end, &temp};
#endif

    memset(k, 0, ksize);

    start = middle = 0;
    end = node->nentry;

    if (end > 0) {
        // compare with smallest key
        memcpy(k, (uint8_t*)node->data + 0*(ksize+vsize), ksize);

        // smaller than smallest key
        if (_cmp_binary64(key, k, NULL) < 0) {
        // if (_cmp_uint64_t(key, k, NULL) < 0) {
            return BTREE_IDX_NOT_FOUND;
        }

        // compare with largest key
        memcpy(k, (uint8_t*)node->data + (end-1)*(ksize+vsize), ksize);
        // larger than largest key
        if (_cmp_binary64(key, k, NULL) >= 0) {
        // if (_cmp_uint64_t(key, k, NULL) >= 0) {
            return end-1;
        }

        // binary search
        while(start+1 < end) {
            middle = (start + end) >> 1;

            // get key at middle
            memcpy(k, (uint8_t*)node->data + middle*(ksize+vsize), ksize);
            cmp = _cmp_binary64(key, k, NULL);
            // cmp = _cmp_uint64_t(key, k, NULL);

#ifdef __BIT_CMP
            cmp = _MAP(cmp) + 1;
            *_map1[cmp] = middle;
            *_map2[cmp] = 0;
#else
            if (cmp < 0) end = middle;
                else if (cmp > 0) start = middle;
                else {
                    //if (btree->kv_ops->free_kv_var) btree->kv_ops->free_kv_var(btree, k, NULL);
                    return middle;
                }
#endif
        }
        return start;
    }
    return BTREE_IDX_NOT_FOUND;

}

bool filemgr_merge_bnode_with_log(struct filemgr *file, struct bnode *node, 
                                  char* key, uint64_t int_value) {
    int ksize = file->config->bnode_ksize;
    int vsize = file->config->bnode_vsize;
    int kvsize = ksize + vsize;

    idx_t idx, idx_insert;
    uint8_t *k = alca(uint8_t, ksize);
    void *ptr = node->data;
    uint32_t nentry;
    uint32_t bnode_len = _get_bnode_write_len((void*)node, &nentry, false);

    void *value = (void*)malloc(vsize);
    memcpy(value, &int_value, vsize);

    memset(k, 0, ksize);

    idx = filemgr_find_bnode_entry(file, node, key);

    if (idx == BTREE_IDX_NOT_FOUND) idx_insert = 0;
    else {
        memcpy(k, (uint8_t*)ptr + idx * kvsize, ksize);
        if (!_cmp_binary64(key, k, NULL)) {
            // if same key already exists -> update its value
            memcpy((uint8_t *)ptr + idx * kvsize + ksize, value, vsize);
            free(value);
            return true;
        } else {
            idx_insert = idx + 1;
        }
    }

    if (bnode_len + kvsize > file->index_blocksize) {
        printf("Too many entries node %lu!\n", node->bid);
        assert(0);
    }
    
    if (idx_insert < nentry) {
        memmove(
                (uint8_t *)ptr + (idx_insert+1)*kvsize,
                (uint8_t *)ptr + idx_insert*kvsize,
                (nentry - idx_insert)*kvsize);
        memcpy((uint8_t *)ptr + idx_insert*kvsize, key, ksize);
        memcpy((uint8_t *)ptr + idx_insert*kvsize + ksize, value, vsize);
    } else {
        memcpy((uint8_t *)ptr + idx_insert * kvsize, key, ksize);
        memcpy((uint8_t *)ptr + idx_insert * kvsize + ksize, value, vsize);
    }
    node->nentry++;

    free(value);

    return true;
}

void filemgr_collect_node(struct filemgr *file, uint64_t bid, void *node_addr)
{
    uint16_t level;
    struct bnode* node = _init_bnode(bid, node_addr);
    std::string *latest_log;

    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
            "Collecting node for bid %lu\n",
            bid);

    latest_log = new std::string();
    mutex_lock(&file->outstanding_log_lock);
    auto it = file->outstanding_logs->find(bid);
    if(it != file->outstanding_logs->end()) {
        *latest_log = it->second;
    }
    mutex_unlock(&file->outstanding_log_lock);

    auto log = _filemgr_collect_logs(file, bid, &level, latest_log, NULL, NULL, false);

    for(auto &i: log) {
        filemgr_merge_bnode_with_log(file, node, 
                                     (char*)i.first.c_str(), i.second);
    }

    node->kvsize = _endian_encode(node->kvsize);
    node->flag = _endian_encode(node->flag);
    node->level = level;
    node->level = _endian_encode(node->level);
    node->nentry = _endian_encode(node->nentry);

    if(node->nentry == 0) {
        fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                "0 entries for log bid %lu\n", bid);
    }

    if(node->level == 1) {
        file->this_flush_leaf_reads++;
    } else {
        file->this_flush_inner_reads++;
    }

    mutex_lock(&file->entries_lock);
    auto entries = file->entries_per_node->find(bid);
    if(entries == file->entries_per_node->end()) {
        file->entries_per_node->insert({bid, node->nentry});
    }
    mutex_unlock(&file->entries_lock);

    delete latest_log;
}

fdb_status filemgr_read(struct filemgr *file, bid_t bid, void *buf, uint64_t *len,
                        err_log_callback *log_callback, bool read_on_cache_miss)
{
    size_t lock_no;
    ssize_t r;
    uint64_t pos;
    fdb_status status = FDB_RESULT_SUCCESS;
    bool index_block = false;
    uint32_t nentry;
    int write_len;

    pos = bid;

    if(len && (*len == file->index_blocksize)) {
        index_block = true;
        *len = 0;
    }

    if(global_config.kv_cache_size > 0 && !index_block) {
        uint64_t start = 0, end = 0;
        start = get_monotonic_ts_us();
        r = kvcache_read(file, bid, buf);

        if(r == 0) {
            // if normal file, just read a block
            int ret;
            ret = retrieve(file, bid, buf, NULL);

            if(ret <= 0) {
                fflush(stdout);
                _log_errno_str(file->ops, log_callback,
                        (fdb_status) r, "READ", file->filename);
                const char *msg = "Read error: BID %" _F64 " in a database file '%s' "
                    "is not read correctly: only %d bytes read.\n";
                status = r < 0 ? (fdb_status)r : FDB_RESULT_READ_FAIL;
                fdb_log(log_callback, FDB_LOG_ERROR, status,
                        msg, bid, file->filename, r);
                if (!log_callback || !log_callback->callback) {
                    //dbg_print_buf(buf, file->blocksize, true, 16);
                }
                return status;
            }

            if(len) {
                *len = ret;
            }

            r = kvcache_write(file, bid, buf, ret, KVCACHE_REQ_CLEAN);
            if (r != ret) {
                _log_errno_str(file->ops, log_callback,
                               (fdb_status) r, "WRITE", file->filename);
                const char *msg = "Read error: BID %" _F64 " in a database file '%s' "
                    "is not written in cache correctly: only %d bytes written.\n";
                status = r < 0 ? (fdb_status) r : FDB_RESULT_WRITE_FAIL;
                fdb_log(log_callback, FDB_LOG_ERROR, status,
                        msg, bid, file->filename, r);
                if (!log_callback || !log_callback->callback) {
                    dbg_print_buf(buf, file->blocksize, true, 16);
                }
                return status;
            }
        } else {
            if(len) {
                *len = r;
            }
        }
        end = get_monotonic_ts_us();
        if(check_lat) {
            printf("\nTID %lu *** READ LAT WAS %lu\n", pthread_self(), end - start);
            check_lat = 0;
        }
    } else if (global_config.ncacheblock > 0 && index_block) {
        lock_no = bid % DLOCK_MAX;
        (void)lock_no;

        plock_entry_t *plock_entry = NULL;
        bid_t is_writer = 0;
        bool locked = false; (void)locked;

        r = bcache_read(file, bid, buf);
        if (r == 0) {
            // Cache miss, grab lock and find once again.

            // WARNING:
            //   Regardless of writability, we SHOULD grab lock
            //   as reused blocks are immutable but writers can overwrite it
            //   and can update block cache sooner. Once race condition happens,
            //   reader may overwrite it using previous block.
            plock_entry = plock_lock(&file->plock, &bid, &is_writer);
            locked = true;
            r = bcache_read(file, bid, buf);
        }

        if (r == 0) {
            // Still cache miss, read from file.
            if (!read_on_cache_miss) {
                if (locked) {
                    plock_unlock(&file->plock, plock_entry);
                }
                return FDB_RESULT_READ_FAIL;
            }

            (void)lock_no;
            filemgr_collect_node(file, bid, buf);
            if (len) {
                *len = r;
            }
            write_len = _get_bnode_write_len(buf, &nentry, true);
            memset((uint8_t *)buf + file->index_blocksize - 1,
                    BLK_MARKER_BNODE, BLK_MARKER_SIZE);

            r = bcache_write(file, bid, buf, BCACHE_REQ_CLEAN, false);
            if (r != global_config.index_blocksize) {
                if (locked) {
                    plock_unlock(&file->plock, plock_entry);
                }
                _log_errno_str(file->ops, log_callback,
                               (fdb_status) r, "WRITE", file->filename);
                const char *msg = "Read error: BID %" _F64 " in a database file '%s' "
                    "is not written in cache correctly: only %d bytes written.\n";
                status = r < 0 ? (fdb_status) r : FDB_RESULT_WRITE_FAIL;
                fdb_log(log_callback, FDB_LOG_ERROR, status,
                        msg, bid, file->filename, r);
                if (!log_callback || !log_callback->callback) {
                    dbg_print_buf(buf, file->blocksize, true, 16);
                }
                return status;
            }
        } else {
        }

        if (locked) {
            plock_unlock(&file->plock, plock_entry);
        }
    } else if (file->config->kvssd) {
        if(!index_block) {
            int ret = retrieve(file, bid, buf, NULL);
            if(ret <= 0) {
                assert(0);
                _log_errno_str(file->ops, log_callback,
                        (fdb_status) r, "READ", file->filename);
                const char *msg = "Read error: BID %" _F64 " in a database file '%s' "
                    "is not read correctly: only %d bytes read.\n";
                status = r < 0 ? (fdb_status)r : FDB_RESULT_READ_FAIL;
                fdb_log(log_callback, FDB_LOG_ERROR, status,
                        msg, bid, file->filename, r);
                if (!log_callback || !log_callback->callback) {
                    dbg_print_buf(buf, file->blocksize, true, 16);
                }
                return status;
            }

            *len = ret;
        } else {
            filemgr_collect_node(file, bid, buf);
            write_len = _get_bnode_write_len(buf, &nentry, true);
            memset((uint8_t *)buf + file->index_blocksize - 1,
                    BLK_MARKER_BNODE, BLK_MARKER_SIZE);
        }
    } else {
        if (!read_on_cache_miss) {
            const char *msg = "Read error: BID %" _F64 " in a database file '%s':"
                "block cache is not enabled.\n";
            fdb_log(log_callback, FDB_LOG_ERROR, FDB_RESULT_READ_FAIL,
                    msg, bid, file->filename);
            return FDB_RESULT_READ_FAIL;
        }

        r = filemgr_read_block(file, NULL, buf, bid);
        if (r != (ssize_t)file->blocksize) {
            _log_errno_str(file->ops, log_callback, (fdb_status) r, "READ",
                           file->filename);
            const char *msg = "Read error: BID %" _F64 " in a database file '%s' "
                "is not read correctly: only %d bytes read (block cache disabled).\n";
            status = (r < 0)? (fdb_status)r : FDB_RESULT_READ_FAIL;
            fdb_log(log_callback, FDB_LOG_ERROR, status,
                    msg, bid, file->filename, r);
            if (!log_callback || !log_callback->callback) {
                dbg_print_buf(buf, file->blocksize, true, 16);
            }
            return status;
        }

        status = _filemgr_crc32_check(file, buf);
        if (status != FDB_RESULT_SUCCESS) {
            _log_errno_str(file->ops, log_callback, status, "READ",
                           file->filename);
            const char *msg = "Read error: checksum error on BID %" _F64 " in a database file '%s' "
                ": marker %x (block cache disabled)\n";
            fdb_log(log_callback, FDB_LOG_ERROR, status,
                    msg, bid,
                    file->filename, *((uint8_t*)buf + file->blocksize-1));
            if (!log_callback || !log_callback->callback) {
                dbg_print_buf(buf, file->blocksize, true, 16);
            }
            return status;
        }

    }
    return status;
}

fdb_status filemgr_write_offset(struct filemgr *file, bid_t bid,
                                uint64_t offset, uint64_t len, void *buf,
                                bool final_write,
                                err_log_callback *log_callback)
{
    ssize_t r = 0;
    uint64_t pos;
    struct docio_length docio_len;
    uint8_t marker = 0x0;

    pos = bid;

    if (len != UINT64_MAX && 
       ((!file->config->kvssd &&offset + len > file->blocksize) || 
       (file->config->kvssd && offset + len > file->config->kvssd_max_value_size))) {
        const char *msg = "Write error: trying to write the buffer data "
            "(offset: %" _F64 ", len: %" _F64 " that exceeds the block size "
            "%" _F64 " in a database file '%s'\n";
        fdb_log(log_callback, FDB_LOG_ERROR, FDB_RESULT_WRITE_FAIL,
                msg, offset, len,
                file->blocksize, file->filename);
        return FDB_RESULT_WRITE_FAIL;
    }

    if(len != UINT64_MAX &&
       global_config.kv_cache_size > 0 &&
       global_config.kv_cache_doc_writes) {
        r = kvcache_write(file, bid, buf, len, KVCACHE_REQ_CLEAN);
        if (r != (ssize_t) len) {
            assert(0);
            _log_errno_str(file->ops, log_callback,
                    (fdb_status) r, "WRITE", file->filename);
            return r < 0 ? (fdb_status) r : FDB_RESULT_WRITE_FAIL;
        }

        store(file, bid, buf, len, NULL);
        atomic_add_uint64_t(&file->data_in_kvssd, VALUE_RETRIEVE_LENGTH - 1024 + sizeof(uint64_t));
        atomic_add_uint64_t(&file->written_to_kvssd, len);
    } else if (global_config.ncacheblock > 0 && len == UINT64_MAX) {
            r = bcache_write(file, bid, buf, BCACHE_REQ_CLEAN, final_write);
            if (r != global_config.index_blocksize) {
                _log_errno_str(file->ops, log_callback,
                               (fdb_status) r, "WRITE", file->filename);
                return r < 0 ? (fdb_status) r : FDB_RESULT_WRITE_FAIL;
            }
    } else { // block cache disabled or not caching document blocks        
        if (file->config->kvssd) {
            ssize_t ret;
            if (len == UINT64_MAX) {
                len = 4096;

                /*
                 * There's no need to do anything here. The logs will be 
                 * written in the next flush, and we aren't caching.
                 * This is poor and should be changed later when we re-do CRC
                 * upon node collection, as it won't work when value sizes 
                 * are 4096 (or whatever the block size is).
                 */

                ret = len;
            } else {
                store(file, bid, buf, len, NULL);
                atomic_add_uint64_t(&file->data_in_kvssd, 
                                    VALUE_RETRIEVE_LENGTH - 1024 + sizeof(uint64_t));
                atomic_add_uint64_t(&file->written_to_kvssd, len);
                ret = len;
            }

            _log_errno_str(file->ops, log_callback, (fdb_status) ret, "WRITE", file->filename);
            if ((uint64_t) ret == len || marker == BLK_MARKER_BNODE) {
                return FDB_RESULT_SUCCESS;
            } else {
                return FDB_RESULT_WRITE_FAIL;
            }
        } else {
            r = file->ops->pwrite(file->fd, buf, len, pos);
            _log_errno_str(file->ops, log_callback, (fdb_status) r, "WRITE", file->filename);
            if ((uint64_t)r != len) {
                return r < 0 ? (fdb_status) r : FDB_RESULT_WRITE_FAIL;
            }
        }
    } // block cache check
    return FDB_RESULT_SUCCESS;
}

fdb_status filemgr_write(struct filemgr *file, void *key, bid_t bid, void *buf,
                   err_log_callback *log_callback)
{
    return filemgr_write_offset(file, bid, 0, file->blocksize, buf,
                                    false, // TODO: track immutability of index blk
                                    log_callback);
}

fdb_status filemgr_commit(struct filemgr *file, bool sync,
                          err_log_callback *log_callback)
{
    // append header at the end of the file
    uint64_t bmp_revnum = 0;
    if (sb_ops.get_bmp_revnum) {
        bmp_revnum = sb_ops.get_bmp_revnum(file);
    }
    return filemgr_commit_bid(file, BLK_NOT_FOUND, bmp_revnum,
                              sync, log_callback);
}

fdb_status filemgr_commit_bid(struct filemgr *file, bid_t bid,
                              uint64_t bmp_revnum, bool sync,
                              err_log_callback *log_callback)
{
    struct avl_node *a;
    struct kvs_node *node;
    bid_t prev_bid, _prev_bid;
    uint64_t _deltasize, _bmp_revnum;
    fdb_seqnum_t _seqnum;
    uint64_t _vernum;
    filemgr_header_revnum_t _revnum;
    int result = FDB_RESULT_SUCCESS;
    bool block_reusing = false;

    filemgr_set_io_inprog(file);
    spin_lock(&file->lock);

    uint16_t header_len = file->header.size;
    struct kvs_header *kv_header = file->kv_header;
    filemgr_magic_t magic = file->version;

    if (file->header.size > 0 && file->header.data) {
        void *buf = get_buf();
        uint8_t marker[BLK_MARKER_SIZE];

        // [header data]:        'header_len' bytes   <---+
        // [header revnum]:      8 bytes                  |
        // [default KVS seqnum]: 8 bytes                  |
        // [current vernum]:     8 bytes                  |
        // ...                                            |
        // (empty)                                    blocksize
        // ...                                            |
        // [SB bitmap revnum]:   8 bytes                  |
        // [Delta size]:         8 bytes                  |
        // [prev header bid]:    8 bytes                  |
        // [header length]:      2 bytes                  |
        // [magic number]:       8 bytes                  |
        // [block marker]:       1 byte               <---+

        // header data
        memcpy(buf, file->header.data, header_len);
        // header rev number
        _revnum = _endian_encode(file->header.revnum);
        memcpy((uint8_t *)buf + header_len, &_revnum,
               sizeof(filemgr_header_revnum_t));
        // file's sequence number (default KVS seqnum)
        _seqnum = _endian_encode(file->header.seqnum.load());
        memcpy((uint8_t *)buf + header_len + sizeof(filemgr_header_revnum_t),
               &_seqnum, sizeof(fdb_seqnum_t));
        // file's current vernum
        _vernum = _endian_encode(file->header.vernum.load());
        memcpy((uint8_t *)buf + header_len + sizeof(filemgr_header_revnum_t) + sizeof(fdb_seqnum_t),
                &_vernum, sizeof(uint64_t));

        // current header's sb bmp revision number
        if (file->sb) {
            _bmp_revnum = _endian_encode(bmp_revnum);
            memcpy((uint8_t *)buf + (file->blocksize - sizeof(filemgr_magic_t)
                   - sizeof(header_len) - sizeof(_prev_bid)
                   - sizeof(_deltasize) - sizeof(_bmp_revnum)
                   - BLK_MARKER_SIZE),
                   &_bmp_revnum, sizeof(_bmp_revnum));
        }

        // delta size since prior commit
        _deltasize = _endian_encode(file->header.stat.deltasize //index+data
                                  + wal_get_datasize(file)); // wal datasize
        memcpy((uint8_t *)buf + (file->blocksize - sizeof(filemgr_magic_t)
               - sizeof(header_len) - sizeof(_prev_bid)*2 - BLK_MARKER_SIZE),
               &_deltasize, sizeof(_deltasize));

        // Reset in-memory delta size of the header for next commit...
        file->header.stat.deltasize = 0; // single kv store header
        if (kv_header) { // multi kv store stats
            a = avl_first(kv_header->idx_id);
            while (a) {
                node = _get_entry(a, struct kvs_node, avl_id);
                a = avl_next(&node->avl_id);
                node->stat.deltasize = 0;
            }
        }

        // prev header bid
        prev_bid = atomic_get_uint64_t(&file->header.bid);
        _prev_bid = _endian_encode(prev_bid);
        memcpy((uint8_t *)buf + (file->blocksize - sizeof(filemgr_magic_t)
               - sizeof(header_len) - sizeof(_prev_bid) - BLK_MARKER_SIZE),
               &_prev_bid, sizeof(_prev_bid));
        // header length
        header_len = _endian_encode(header_len);
        memcpy((uint8_t *)buf + (file->blocksize - sizeof(filemgr_magic_t)
               - sizeof(header_len) - BLK_MARKER_SIZE),
               &header_len, sizeof(header_len));
        // magic number
        magic = _endian_encode(magic);
        memcpy((uint8_t *)buf + (file->blocksize - sizeof(filemgr_magic_t)
               - BLK_MARKER_SIZE), &magic, sizeof(magic));

        // marker
        memset(marker, BLK_MARKER_DBHEADER, BLK_MARKER_SIZE);
        memcpy((uint8_t *)buf + file->blocksize - BLK_MARKER_SIZE,
               marker, BLK_MARKER_SIZE);

        if (bid == BLK_NOT_FOUND) {
            // append header at the end of file
            if(file->config->kvssd) {
                bid = atomic_get_uint64_t(&file->pos); // TODO wrong
            } else {
                bid = atomic_get_uint64_t(&file->pos) / file->blocksize;
            }
            block_reusing = false;
        } else {
            // write header in the allocated (reused) block
            block_reusing = true;
            // we MUST invalidate the header block 'bid', since previous
            // contents of 'bid' may remain in block cache and cause data
            // inconsistency if reading header block hits the cache.
            if (! file->config->kvssd) {
                bcache_invalidate_block(file, bid);
            }
        }

        ssize_t rv;
        if(global_config.kvssd) {
            rv = filemgr_write_blocks_kvssd(file, NULL, buf, 1, bid);

            std::string k;
            k.append("WRITES!@#$");
            k.append(std::to_string(file->header.revnum - 1));

            void *writes = malloc(sizeof(uint64_t));
            memcpy(writes, &file->writes_this_commit, sizeof(uint64_t));

            fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                    "Persisting a write marker of %u writes during commit.", 
                    file->writes_this_commit);

            store(file, k, writes, sizeof(writes), NULL);

            free(writes);
            atomic_add_uint64_t(&file->data_in_kvssd, 
                                k.length() + sizeof(writes));
            atomic_add_uint64_t(&file->written_to_kvssd, 
                                k.length() + sizeof(writes));

            /* Store milestone */

            store(file, MILESTONE_K, &bid, sizeof(bid), NULL);
            atomic_add_uint64_t(&file->data_in_kvssd, 
                                MILESTONE_K.length() + sizeof(bid));
            atomic_add_uint64_t(&file->written_to_kvssd, 
                                MILESTONE_K.length() + sizeof(bid));

            if(sync) {
                file->ops_kvssd->flush();
            }
        } else {
            rv = filemgr_write_blocks(file, buf, 1, bid);
        }


        file->writes_this_commit = 0;

        _log_errno_str(file->ops, log_callback, (fdb_status) rv,
                       "WRITE", file->filename);
        if (rv != (ssize_t)file->blocksize) {
            put_buf(buf);
            spin_unlock(&file->lock);
            filemgr_clear_io_inprog(file);
            return rv < 0 ? (fdb_status) rv : FDB_RESULT_WRITE_FAIL;
        }

        if (prev_bid) {
            // mark prev DB header as stale
            filemgr_add_stale_block_kvssd(file, prev_bid, file->blocksize);
        }

        atomic_store_uint64_t(&file->header.bid, bid);
        if (!block_reusing) {
            if(file->config->kvssd) {
                atomic_add_uint64_t(&file->pos, 1);
            } else {
                atomic_add_uint64_t(&file->pos, file->blocksize);
            }
        }

        put_buf(buf);
    }

    atomic_store_uint64_t(&file->last_commit, atomic_get_uint64_t(&file->pos));

    spin_unlock(&file->lock);
    filemgr_clear_io_inprog(file);
    return (fdb_status) result;
}

fdb_status filemgr_sync(struct filemgr *file, bool sync_option,
                        err_log_callback *log_callback)
{
    fdb_status result = FDB_RESULT_SUCCESS;
    if (global_config.ncacheblock > 0) {
        result = bcache_flush(file);
        if (result != FDB_RESULT_SUCCESS) {
            _log_errno_str(file->ops, log_callback, (fdb_status) result,
                           "FLUSH", file->filename);
            return result;
        }
    }

    if (sync_option && file->fflags & FILEMGR_SYNC) {
        int rv = file->ops->fsync(file->fd);
        _log_errno_str(file->ops, log_callback, (fdb_status)rv, "FSYNC", file->filename);
        return (fdb_status) rv;
    }
    return result;
}

fdb_status filemgr_copy_file_range(struct filemgr *src_file,
                                   struct filemgr *dst_file,
                                   bid_t src_bid, bid_t dst_bid,
                                   bid_t clone_len)
{
    assert(0);
    uint32_t blocksize = src_file->blocksize;
    fdb_status fs = (fdb_status)dst_file->ops->copy_file_range(
                                            src_file->fs_type,
                                            src_file->fd,
                                            dst_file->fd,
                                            src_bid * blocksize,
                                            dst_bid * blocksize,
                                            clone_len * blocksize);
    if (fs != FDB_RESULT_SUCCESS) {
        return fs;
    }
    atomic_store_uint64_t(&dst_file->pos, (dst_bid + clone_len) * blocksize);
    return FDB_RESULT_SUCCESS;
}

void filemgr_update_file_status(struct filemgr *file, file_status_t status)
{
    spin_lock(&file->lock);
    atomic_store_uint8_t(&file->status, status);
    spin_unlock(&file->lock);
}

static void assign_old_filename(struct filemgr *file, const char *old_filename)
{
    free(file->old_filename);
    if (old_filename) {
        file->old_filename = (char*)malloc(strlen(old_filename) + 1);
        strcpy(file->old_filename, old_filename);
    } else {
        file->old_filename = NULL;
    }
}

static void assign_new_filename(struct filemgr *file, const char *new_filename)
{
    free(file->new_filename);
    if (new_filename) {
        file->new_filename = (char*)malloc(strlen(new_filename) + 1);
        strcpy(file->new_filename, new_filename);
    } else {
        file->new_filename = NULL;
    }
}

bool filemgr_update_file_linkage(struct filemgr *file,
                                 const char *old_filename,
                                 const char *new_filename)
{

    bool ret = true;
    spin_lock(&file->lock);
    if (old_filename) {
        if (!file->old_filename) {
            assign_old_filename(file, old_filename);
        } else {
            ret = false;
            fdb_assert(atomic_get_uint32_t(&file->ref_count),
                       atomic_get_uint32_t(&file->ref_count), 0);
        }
    }
    if (new_filename) {
        assign_new_filename(file, new_filename);
    }
    spin_unlock(&file->lock);
    return ret;
}

void filemgr_set_compaction_state(struct filemgr *old_file,
                                  struct filemgr *new_file,
                                  file_status_t status)
{
    if (old_file) {
        spin_lock(&old_file->lock);
        assign_new_filename(old_file, new_file ? new_file->filename : NULL);
        atomic_store_uint8_t(&old_file->status, status);
        spin_unlock(&old_file->lock);

        if (new_file) {
            spin_lock(&new_file->lock);
            assign_old_filename(new_file, old_file->filename);
            spin_unlock(&new_file->lock);
        }
    }
}

bool filemgr_set_kv_header(struct filemgr *file, struct kvs_header *kv_header,
                           void (*free_kv_header)(struct filemgr *file))
{
    bool ret;
    spin_lock(&file->lock);

    if (!file->kv_header) {
        file->kv_header = kv_header;
        file->free_kv_header = free_kv_header;
        ret = true;
    } else {
        ret = false;
    }

    spin_unlock(&file->lock);

    return ret;
}

struct kvs_header *filemgr_get_kv_header(struct filemgr *file)
{
    struct kvs_header *kv_header = NULL;
    spin_lock(&file->lock);
    kv_header = file->kv_header;
    spin_unlock(&file->lock);
    return kv_header;
}

// Check if there is a file that still points to the old_file that is being
// compacted away. If so open the file and return its pointer.
static
void *_filemgr_check_stale_link(struct hash_elem *h, void *ctx) {
    struct filemgr *cur_file = (struct filemgr *)ctx;
    struct filemgr *file = _get_entry(h, struct filemgr, e);
    spin_lock(&file->lock);
    if (atomic_get_uint8_t(&file->status) == FILE_REMOVED_PENDING &&
        !strcmp(file->new_filename, cur_file->filename)) {
        // Incrementing reference counter below is the same as filemgr_open()
        // We need to do this to ensure that the pointer returned does not
        // get freed outside the filemgr_open lock
        atomic_incr_uint32_t(&file->ref_count);
        spin_unlock(&file->lock);
        return (void *)file;
    }
    spin_unlock(&file->lock);
    return (void *)NULL;
}

struct filemgr *filemgr_search_stale_links(struct filemgr *cur_file) {
    struct filemgr *very_old_file;
    spin_lock(&filemgr_openlock);
    very_old_file = (struct filemgr *)hash_scan(&hash,
                                         _filemgr_check_stale_link, cur_file);
    spin_unlock(&filemgr_openlock);
    return very_old_file;
}

char *filemgr_redirect_old_file(struct filemgr *very_old_file,
                                struct filemgr *new_file,
                                filemgr_redirect_hdr_func
                                redirect_header_func) {
    if (!very_old_file || !new_file) {
        return NULL;
    }

    size_t old_header_len, new_header_len;
    uint16_t new_filename_len;
    char *past_filename;
    spin_lock(&very_old_file->lock);

    struct filemgr *new_file_of_very_old_file =
        filemgr_get_instance(very_old_file->new_filename);

    if (very_old_file->header.size == 0 || !new_file_of_very_old_file) {
        spin_unlock(&very_old_file->lock);
        return NULL;
    }

    old_header_len = very_old_file->header.size;
    new_filename_len = strlen(new_file->filename);
    // Find out the new DB header length with new_file's filename
    new_header_len = old_header_len
                     - strlen(new_file_of_very_old_file->filename)
                     + new_filename_len;
    // As we are going to change the new_filename field in the DB header of the
    // very_old_file, maybe reallocate DB header buf to accomodate bigger value
    if (new_header_len > old_header_len) {
        very_old_file->header.data = realloc(very_old_file->header.data,
                                             new_file->blocksize);
    }
    // Re-direct very_old_file to new_file
    assign_new_filename(very_old_file, new_file->filename);
    // Note that the old_filename of the new_file is not updated, this
    // is so that every file in the history is reachable from the current file.

    past_filename = redirect_header_func(very_old_file,
                                         (uint8_t *)very_old_file->header.data,
                                         new_file);//Update in-memory header
    very_old_file->header.size = new_header_len;
    ++(very_old_file->header.revnum);

    spin_unlock(&very_old_file->lock);
    return past_filename;
}

void filemgr_remove_pending(struct filemgr *old_file,
                            struct filemgr *new_file,
                            err_log_callback *log_callback)
{
    if (new_file == NULL) {
        return;
    }

    spin_lock(&old_file->lock);
    if (atomic_get_uint32_t(&old_file->ref_count) > 0) {
        // delay removing
        assign_new_filename(old_file, new_file->filename);
        atomic_store_uint8_t(&old_file->status, FILE_REMOVED_PENDING);

#if !(defined(WIN32) || defined(_WIN32))
        // Only for Posix
        int ret;
        ret = unlink(old_file->filename);
        _log_errno_str(old_file->ops, log_callback, (fdb_status)ret,
                       "UNLINK", old_file->filename);
#endif

        spin_unlock(&old_file->lock);

        // Update new_file's old_filename
        spin_lock(&new_file->lock);
        assign_old_filename(new_file, old_file->filename);
        spin_unlock(&new_file->lock);
    } else {
        // immediatly remove
        // LCOV_EXCL_START
        spin_unlock(&old_file->lock);

        struct filemgr *new_file_of_old_file =
            filemgr_get_instance(old_file->new_filename);

        if (!lazy_file_deletion_enabled ||
            (new_file_of_old_file && new_file_of_old_file->in_place_compaction)) {
            remove(old_file->filename);
        }
        filemgr_remove_file(old_file, log_callback);
        // LCOV_EXCL_STOP
    }
}

// migrate default kv store stats over to new_file
struct kvs_ops_stat *filemgr_migrate_op_stats(struct filemgr *old_file,
                                              struct filemgr *new_file,
                                              struct kvs_info *kvs)
{
    kvs_ops_stat *ret = NULL;
    if (new_file == NULL) {
        return NULL;
    }

    spin_lock(&old_file->lock);
    new_file->header.op_stat = old_file->header.op_stat;
    ret = &new_file->header.op_stat;
    spin_unlock(&old_file->lock);
    return ret;
}

// Note: filemgr_openlock should be held before calling this function.
fdb_status filemgr_destroy_file(char *filename,
                                struct filemgr_config *config,
                                struct hash *destroy_file_set)
{
    struct filemgr *file = NULL;
    struct hash to_destroy_files;
    struct hash *destroy_set = (destroy_file_set ? destroy_file_set :
                                                  &to_destroy_files);
    struct filemgr query;
    struct hash_elem *e = NULL;
    fdb_status status = FDB_RESULT_SUCCESS;
    char *old_filename = NULL;

    if (!destroy_file_set) { // top level or non-recursive call
        hash_init(destroy_set, NBUCKET, _file_hash, _file_cmp);
    }

    query.filename = filename;
    // check whether file is already being destroyed in parent recursive call
    e = hash_find(destroy_set, &query.e);
    if (e) { // Duplicate filename found, nothing to be done in this call
        if (!destroy_file_set) { // top level or non-recursive call
            hash_free(destroy_set);
        }
        return status;
    } else {
        // Remember file. Stack value ok IFF single direction recursion
        hash_insert(destroy_set, &query.e);
    }

    // check global list of known files to see if it is already opened or not
    e = hash_find(&hash, &query.e);
    if (e) {
        // already opened (return existing structure)
        file = _get_entry(e, struct filemgr, e);

        spin_lock(&file->lock);
        if (atomic_get_uint32_t(&file->ref_count)) {
            spin_unlock(&file->lock);
            status = FDB_RESULT_FILE_IS_BUSY;
            if (!destroy_file_set) { // top level or non-recursive call
                hash_free(destroy_set);
            }
            return status;
        }
        spin_unlock(&file->lock);
        if (file->old_filename) {
            status = filemgr_destroy_file(file->old_filename, config,
                                          destroy_set);
            if (status != FDB_RESULT_SUCCESS) {
                if (!destroy_file_set) { // top level or non-recursive call
                    hash_free(destroy_set);
                }
                return status;
            }
        }

        // Cleanup file from in-memory as well as on-disk
        e = hash_remove(&hash, &file->e);
        fdb_assert(e, e, 0);
        filemgr_free_func(&file->e);
        if (filemgr_does_file_exist(filename) == FDB_RESULT_SUCCESS) {
            if (remove(filename)) {
                status = FDB_RESULT_FILE_REMOVE_FAIL;
            }
        }
    } else { // file not in memory, read on-disk to destroy older versions..
        file = (struct filemgr *)alca(struct filemgr, 1);
        memset(file, 0x0, sizeof(struct filemgr));
        file->filename = filename;
        file->ops = get_filemgr_ops();
        file->ops_kvssd = get_filemgr_ops_kvssd();
        file->fd = file->ops->open(file->filename, O_RDWR, 0666);
        file->blocksize = global_config.blocksize;
        file->index_blocksize = global_config.index_blocksize;
        file->config = (struct filemgr_config *)alca(struct filemgr_config, 1);
        *file->config = *config;
        fdb_init_encryptor(&file->encryption, &config->encryption_key);
        if (file->fd < 0) {
            if (file->fd != FDB_RESULT_NO_SUCH_FILE) {
                if (!destroy_file_set) { // top level or non-recursive call
                    hash_free(destroy_set);
                }
                return (fdb_status) file->fd;
            }
        } else { // file successfully opened, seek to end to get DB header
            cs_off_t offset;
            if(file->config->kvssd) {
                retrieve(file, MILESTONE_K, &offset, NULL);
            } else {
                offset = file->ops->goto_eof(file->fd);
            }
            if (offset < 0) {
                if (!destroy_file_set) { // top level or non-recursive call
                    hash_free(destroy_set);
                }
                return (fdb_status) offset;
            } else { // Need to read DB header which contains old filename
                atomic_store_uint64_t(&file->pos, offset);
                // initialize CRC mode
                if (file->config && file->config->options & FILEMGR_CREATE_CRC32) {
                    file->crc_mode = CRC32;
                } else {
                    file->crc_mode = CRC_DEFAULT;
                }

                status = _filemgr_load_sb(file, NULL);
                if (status != FDB_RESULT_SUCCESS) {
                    if (!destroy_file_set) { // top level or non-recursive call
                        hash_free(destroy_set);
                    }
                    file->ops->close(file->fd);
                    return status;
                }

                status = _filemgr_read_header(file, NULL);
                if (status != FDB_RESULT_SUCCESS) {
                    if (!destroy_file_set) { // top level or non-recursive call
                        hash_free(destroy_set);
                    }
                    file->ops->close(file->fd);
                    if (sb_ops.release && file->sb) {
                        sb_ops.release(file);
                    }
                    return status;
                }
                if (file->header.data) {
                    size_t new_fnamelen_off = ver_get_new_filename_off(file->
                                                                      version);
                    size_t old_fnamelen_off = new_fnamelen_off + 2;
                    uint16_t *new_filename_len_ptr = (uint16_t *)((char *)
                                                     file->header.data
                                                     + new_fnamelen_off);
                    uint16_t new_filename_len =
                                      _endian_decode(*new_filename_len_ptr);
                    uint16_t *old_filename_len_ptr = (uint16_t *)((char *)
                                                     file->header.data
                                                     + old_fnamelen_off);
                    uint16_t old_filename_len =
                                      _endian_decode(*old_filename_len_ptr);
                    old_filename = (char *)file->header.data + old_fnamelen_off
                                   + 2 + new_filename_len;
                    if (old_filename_len) {
                        status = filemgr_destroy_file(old_filename, config,
                                                      destroy_set);
                    }
                    free(file->header.data);
                }
                if(file->config->kvssd) {
                    file->ops_kvssd->close();
                } else {
                    file->ops->close(file->fd);
                }
                if (sb_ops.release && file->sb) {
                    sb_ops.release(file);
                }
                if (status == FDB_RESULT_SUCCESS) {
                    if (filemgr_does_file_exist(filename)
                                               == FDB_RESULT_SUCCESS) {
                        if (remove(filename)) {
                            status = FDB_RESULT_FILE_REMOVE_FAIL;
                        }
                    }
                }
            }
        }
    }

    if (!destroy_file_set) { // top level or non-recursive call
        hash_free(destroy_set);
    }

    return status;
}

bool filemgr_is_rollback_on(struct filemgr *file)
{
    bool rv;
    spin_lock(&file->lock);
    rv = (file->fflags & FILEMGR_ROLLBACK_IN_PROG);
    spin_unlock(&file->lock);
    return rv;
}

void filemgr_set_rollback(struct filemgr *file, uint8_t new_val)
{
    spin_lock(&file->lock);
    if (new_val) {
        file->fflags |= FILEMGR_ROLLBACK_IN_PROG;
    } else {
        file->fflags &= ~FILEMGR_ROLLBACK_IN_PROG;
    }
    spin_unlock(&file->lock);
}

void filemgr_set_cancel_compaction(struct filemgr *file, bool cancel)
{
    spin_lock(&file->lock);
    if (cancel) {
        file->fflags |= FILEMGR_CANCEL_COMPACTION;
    } else {
        file->fflags &= ~FILEMGR_CANCEL_COMPACTION;
    }
    spin_unlock(&file->lock);
}

bool filemgr_is_compaction_cancellation_requested(struct filemgr *file)
{
    bool rv;
    spin_lock(&file->lock);
    rv = (file->fflags & FILEMGR_CANCEL_COMPACTION);
    spin_unlock(&file->lock);
    return rv;
}

void filemgr_set_successfully_compacted(struct filemgr *file)
{
    spin_lock(&file->lock);
    file->fflags |= FILEMGR_SUCCESSFULLY_COMPACTED;
    spin_unlock(&file->lock);
}

bool filemgr_is_successfully_compacted(struct filemgr *file)
{
    bool rv;
    spin_lock(&file->lock);
    rv = (file->fflags & FILEMGR_SUCCESSFULLY_COMPACTED);
    spin_unlock(&file->lock);
    return rv;
}

void filemgr_set_in_place_compaction(struct filemgr *file,
                                     bool in_place_compaction) {
    spin_lock(&file->lock);
    file->in_place_compaction = in_place_compaction;
    spin_unlock(&file->lock);
}

bool filemgr_is_in_place_compaction_set(struct filemgr *file)

{
    bool ret = false;
    spin_lock(&file->lock);
    ret = file->in_place_compaction;
    spin_unlock(&file->lock);
    return ret;
}

void filemgr_mutex_openlock(struct filemgr_config *config)
{
    filemgr_init(config);

    spin_lock(&filemgr_openlock);
}

void filemgr_mutex_openunlock(void)
{
    spin_unlock(&filemgr_openlock);
}

void filemgr_mutex_lock(struct filemgr *file)
{
    mutex_lock(&file->writer_lock.mutex);
    file->writer_lock.locked = true;
}

bool filemgr_mutex_trylock(struct filemgr *file) {
    if (mutex_trylock(&file->writer_lock.mutex)) {
        file->writer_lock.locked = true;
        return true;
    }
    return false;
}

void filemgr_mutex_unlock(struct filemgr *file)
{
    if (file->writer_lock.locked) {
        file->writer_lock.locked = false;
        mutex_unlock(&file->writer_lock.mutex);
    }
}

bool filemgr_is_commit_header(void *head_buffer, size_t blocksize)
{
    uint8_t marker[BLK_MARKER_SIZE];
    filemgr_magic_t magic;
    marker[0] = *(((uint8_t *)head_buffer)
                 + blocksize - BLK_MARKER_SIZE);
    if (marker[0] != BLK_MARKER_DBHEADER) {
        return false;
    }

    memcpy(&magic, (uint8_t *) head_buffer
            + blocksize - BLK_MARKER_SIZE - sizeof(magic), sizeof(magic));
    magic = _endian_decode(magic);

    return ver_is_valid_magic(magic);
}

bool filemgr_is_cow_supported(struct filemgr *src, struct filemgr *dst)
{
    src->fs_type = src->ops->get_fs_type(src->fd);
    if (src->fs_type < 0) {
        return false;
    }
    dst->fs_type = dst->ops->get_fs_type(dst->fd);
    if (dst->fs_type < 0) {
        return false;
    }
    if (src->fs_type == dst->fs_type && src->fs_type != FILEMGR_FS_NO_COW) {
        return true;
    }
    return false;
}

void filemgr_set_throttling_delay(struct filemgr *file, uint64_t delay_us)
{
    atomic_store_uint32_t(&file->throttling_delay, delay_us,
                          std::memory_order_relaxed);
}

uint32_t filemgr_get_throttling_delay(struct filemgr *file)
{
    return atomic_get_uint32_t(&file->throttling_delay,
                               std::memory_order_relaxed);
}

void filemgr_clear_stale_list(struct filemgr *file)
{
    if (file->stale_list) {
        // if the items in the list are not freed yet, release them first.
        struct list_elem *e;
        struct stale_data *item;

        e = list_begin(file->stale_list);
        while (e) {
            item = _get_entry(e, struct stale_data, le);
            e = list_remove(file->stale_list, e);
            free(item);
        }
        file->stale_list = NULL;
    }
}

void filemgr_clear_stale_info_tree(struct filemgr *file)
{
    struct avl_node *a;
    struct list_elem *e;
    struct stale_info_commit *commit;
    struct stale_info_entry *entry;

    a = avl_first(&file->stale_info_tree);
    while (a) {
        commit = _get_entry(a, struct stale_info_commit, avl);
        a = avl_next(&commit->avl);
        avl_remove(&file->stale_info_tree, &commit->avl);

        e = list_begin(&commit->doc_list);
        while (e) {
            entry = _get_entry(e, struct stale_info_entry, le);
            e = list_next(&entry->le);
            list_remove(&commit->doc_list, &entry->le);
            free(entry->ctx);
            free(entry);
        }
        free(commit);
    }
}

void filemgr_clear_mergetree(struct filemgr *file)
{
    struct avl_node *a;
    struct stale_data *entry;

    a = avl_first(&file->mergetree);
    while (a) {
        entry = _get_entry(a, struct stale_data, avl);
        a = avl_next(&entry->avl);
        avl_remove(&file->mergetree, &entry->avl);
        free(entry);
    }
}

void filemgr_add_stale_block_kvssd(struct filemgr *file, bid_t pos, size_t length)
{
    fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
            "Putting %lu in the stale list", pos);
    file->pairs_to_remove->enqueue(pos);
}

void filemgr_add_stale_block(struct filemgr *file,
                             bid_t pos,
                             size_t len)
{
    if (file->stale_list) {
        struct stale_data *item;
        struct list_elem *e;

        e = list_end(file->stale_list);
        
        if (e) {
            item = _get_entry(e, struct stale_data, le);
            if (item->pos + item->len == pos) {
                // merge if consecutive item
                item->len += len;
                return;
            }
        }

        item = (struct stale_data*)calloc(1, sizeof(struct stale_data));
        item->pos = pos;
        item->len = len;
        list_push_back(file->stale_list, &item->le);
    }
}

size_t filemgr_actual_stale_length(struct filemgr *file,
                                   bid_t offset,
                                   size_t length)
{
    size_t actual_len;
    bid_t start_bid, end_bid;

    start_bid = offset / file->blocksize;
    end_bid = (offset + length) / file->blocksize;

    actual_len = length + (end_bid - start_bid);
    if ((offset + actual_len) % file->blocksize ==
        file->blocksize - 1) {
        actual_len += 1;
    }

    return actual_len;
}

// if a document is not physically consecutive,
// return all fragmented regions.
struct stale_regions filemgr_actual_stale_regions(struct filemgr *file,
                                                  bid_t offset,
                                                  size_t length)
{
    uint8_t *buf = alca(uint8_t, file->blocksize);
    size_t remaining = length;
    size_t real_blocksize = file->blocksize;
    size_t blocksize = real_blocksize;
    size_t cur_pos, space_in_block, count;
    bid_t cur_bid;
    bool non_consecutive = ver_non_consecutive_doc(file->version);
    struct docblk_meta blk_meta;
    struct stale_regions ret;
    struct stale_data *arr = NULL, *cur_region;

    if (non_consecutive) {
        blocksize -= DOCBLK_META_SIZE;

        cur_bid = offset / file->blocksize;
        // relative position in the block 'cur_bid'
        cur_pos = offset % file->blocksize;

        count = 0;
        while (remaining) {
            if (count == 1) {
                // more than one stale region .. allocate array
                size_t arr_size = (length / blocksize) + 2;
                arr = (struct stale_data *)calloc(arr_size, sizeof(struct stale_data));
                arr[0] = ret.region;
                ret.regions = arr;
            }

            if (count == 0) {
                // Since n_regions will be 1 in most cases,
                // we do not allocate heap memory when 'n_regions==1'.
                cur_region = &ret.region;
            } else {
                cur_region = &ret.regions[count];
            }
            cur_region->pos = (cur_bid * real_blocksize) + cur_pos;

            // subtract data size in the current block
            space_in_block = blocksize - cur_pos;
            if (space_in_block <= remaining) {
                // rest of the current block (including block meta)
                cur_region->len = real_blocksize - cur_pos;
                remaining -= space_in_block;
            } else {
                cur_region->len = remaining;
                remaining = 0;
            }
            count++;

            if (remaining) {
                // get next BID
                filemgr_read(file, cur_bid, (void *)buf, NULL, NULL, true);
                memcpy(&blk_meta, buf + blocksize, sizeof(blk_meta));
                cur_bid = _endian_decode(blk_meta.next_bid);
                cur_pos = 0; // beginning of the block
            }
        }
        ret.n_regions = count;

    } else {
        // doc blocks are consecutive .. always return a single region.
        ret.n_regions = 1;
        ret.region.pos = offset;
        ret.region.len = filemgr_actual_stale_length(file, offset, length);
    }

    return ret;
}

void filemgr_mark_stale(struct filemgr *file,
                        bid_t offset, size_t length)
{
    if(file->config->kvssd) {
        filemgr_add_stale_block_kvssd(file, offset, length);
    } else {
        if (file->stale_list && length) {
            size_t i;
            struct stale_regions sr;

            sr = filemgr_actual_stale_regions(file, offset, length);

            if (sr.n_regions > 1) {
                for (i=0; i<sr.n_regions; ++i){
                    filemgr_add_stale_block(file, sr.regions[i].pos, sr.regions[i].len);
                }
                free(sr.regions);
            } else if (sr.n_regions == 1) {
                filemgr_add_stale_block(file, sr.region.pos, sr.region.len);
            }
        }
    }
}

INLINE int _fhandle_idx_cmp(struct avl_node *a, struct avl_node *b, void *aux)
{
    uint64_t aaa, bbb;
    struct filemgr_fhandle_idx_node *aa, *bb;
    aa = _get_entry(a, struct filemgr_fhandle_idx_node, avl);
    bb = _get_entry(b, struct filemgr_fhandle_idx_node, avl);
    aaa = (uint64_t)aa->fhandle;
    bbb = (uint64_t)bb->fhandle;

#ifdef __BIT_CMP
    return _CMP_U64(aaa, bbb);
#else
    if (aaa < bbb) {
        return -1;
    } else if (aaa > bbb) {
        return 1;
    } else {
        return 0;
    }
#endif
}

void _free_fhandle_idx(struct avl_tree *idx)
{
    struct avl_node *a;
    struct filemgr_fhandle_idx_node *item;

    a = avl_first(idx);
    while (a) {
        item = _get_entry(a, struct filemgr_fhandle_idx_node, avl);
        a = avl_next(a);
        avl_remove(idx, &item->avl);
        free(item);
    }
}

bool filemgr_fhandle_add(struct filemgr *file, void *fhandle)
{
    bool ret;
    struct filemgr_fhandle_idx_node *item, query;
    struct avl_node *a;

    spin_lock(&file->fhandle_idx_lock);

    query.fhandle = fhandle;
    a = avl_search(&file->fhandle_idx, &query.avl, _fhandle_idx_cmp);
    if (!a) {
        // not exist, create a node and insert
        item = (struct filemgr_fhandle_idx_node *)calloc(1, sizeof(struct filemgr_fhandle_idx_node));
        item->fhandle = fhandle;
        avl_insert(&file->fhandle_idx, &item->avl, _fhandle_idx_cmp);
        ret = true;
    } else {
        ret = false;
    }

    spin_unlock(&file->fhandle_idx_lock);
    return ret;
}

bool filemgr_fhandle_remove(struct filemgr *file, void *fhandle)
{
    bool ret;
    struct filemgr_fhandle_idx_node *item, query;
    struct avl_node *a;

    spin_lock(&file->fhandle_idx_lock);

    query.fhandle = fhandle;
    a = avl_search(&file->fhandle_idx, &query.avl, _fhandle_idx_cmp);
    if (a) {
        // exist, remove & free the item
        item = _get_entry(a, struct filemgr_fhandle_idx_node, avl);
        avl_remove(&file->fhandle_idx, &item->avl);
        free(item);
        ret = true;
    } else {
        ret = false;
    }

    spin_unlock(&file->fhandle_idx_lock);
    return ret;
}

static void _filemgr_dirty_update_remove_node(struct filemgr *file,
                                              struct filemgr_dirty_update_node *node);

void filemgr_dirty_update_init(struct filemgr *file)
{
    avl_init(&file->dirty_update_idx, NULL);
    spin_init(&file->dirty_update_lock);
    atomic_init_uint64_t(&file->dirty_update_counter, 0);
    file->latest_dirty_update = NULL;
}

void filemgr_dirty_update_free(struct filemgr *file)
{
    struct avl_node *a = avl_first(&file->dirty_update_idx);
    struct filemgr_dirty_update_node *node;

    while (a) {
        node = _get_entry(a, struct filemgr_dirty_update_node, avl);
        a = avl_next(a);
        avl_remove(&file->dirty_update_idx, &node->avl);
        _filemgr_dirty_update_remove_node(file, node);
    }
    spin_destroy(&file->dirty_update_lock);
}

INLINE int _dirty_update_idx_cmp(struct avl_node *a, struct avl_node *b, void *aux)
{
    struct filemgr_dirty_update_node *aa, *bb;
    aa = _get_entry(a, struct filemgr_dirty_update_node, avl);
    bb = _get_entry(b, struct filemgr_dirty_update_node, avl);

    return _CMP_U64(aa->id, bb->id);
}

INLINE int _dirty_blocks_cmp(struct avl_node *a, struct avl_node *b, void *aux)
{
    struct filemgr_dirty_update_block *aa, *bb;
    aa = _get_entry(a, struct filemgr_dirty_update_block, avl);
    bb = _get_entry(b, struct filemgr_dirty_update_block, avl);

    return _CMP_U64(aa->bid, bb->bid);
}

struct filemgr_dirty_update_node *filemgr_dirty_update_new_node(struct filemgr *file)
{
    struct filemgr_dirty_update_node *node;

    node = (struct filemgr_dirty_update_node *)
           calloc(1, sizeof(struct filemgr_dirty_update_node));
    node->id = atomic_incr_uint64_t(&file->dirty_update_counter);
    node->immutable = false; // currently being written
    node->expired = false;
    atomic_init_uint32_t(&node->ref_count, 0);
    node->idtree_root = node->seqtree_root = BLK_NOT_FOUND;
    avl_init(&node->dirty_blocks, NULL);

    spin_lock(&file->dirty_update_lock);
    avl_insert(&file->dirty_update_idx, &node->avl, _dirty_update_idx_cmp);
    spin_unlock(&file->dirty_update_lock);

    return node;
}

struct filemgr_dirty_update_node *filemgr_dirty_update_get_latest(struct filemgr *file)
{
    struct filemgr_dirty_update_node *node = NULL;

    // find the first immutable node from the end
    spin_lock(&file->dirty_update_lock);

    node = file->latest_dirty_update;
    if (node) {
        atomic_incr_uint32_t(&node->ref_count);
    }

    spin_unlock(&file->dirty_update_lock);
    return node;
}

void filemgr_dirty_update_inc_ref_count(struct filemgr_dirty_update_node *node)
{
    if (!node) {
        return;
    }
    atomic_incr_uint32_t(&node->ref_count);
}

INLINE void filemgr_dirty_update_flush(struct filemgr *file,
                                       struct filemgr_dirty_update_node *node,
                                       err_log_callback *log_callback)
{
    struct avl_node *a;
    struct filemgr_dirty_update_block *block;

    if (!node) {
        return;
    }

    // Flush all dirty blocks belonging to this dirty update entry
    a = avl_first(&node->dirty_blocks);
    while (a) {
        block = _get_entry(a, struct filemgr_dirty_update_block, avl);
        a = avl_next(a);
        if (1 || filemgr_is_writable(file, block->bid) && !block->immutable) {
            filemgr_write_offset(file, block->bid, 0, UINT64_MAX, block->addr, false, log_callback);
        }
    }
    node->expired = true;
}

void filemgr_dirty_update_commit(struct filemgr *file,
                                 struct filemgr_dirty_update_node *commit_node,
                                 err_log_callback *log_callback)
{
    struct avl_node *a;
    struct filemgr_dirty_update_node *node;
    struct list remove_queue;
    struct list_elem *le;

    // 1. write back all blocks in the given (committed) node
    // 2. remove all other immutable dirty update entries
    list_init(&remove_queue);
    if (commit_node) {
        filemgr_dirty_update_flush(file, commit_node, log_callback);
    }

    spin_lock(&file->dirty_update_lock);
    file->latest_dirty_update = NULL;

    a = avl_first(&file->dirty_update_idx);
    while (a) {
        node = _get_entry(a, struct filemgr_dirty_update_node, avl);
        a = avl_next(a);

        if (node->immutable &&
            atomic_get_uint32_t(&node->ref_count) == 0) {
            // detach from tree and insert into remove queue
            avl_remove(&file->dirty_update_idx, &node->avl);
            list_push_front(&remove_queue, &node->le);
        }
    }

    spin_unlock(&file->dirty_update_lock);

    le = list_begin(&remove_queue);
    while (le) {
        node = _get_entry(le, struct filemgr_dirty_update_node, le);
        le = list_remove(&remove_queue, &node->le);
        _filemgr_dirty_update_remove_node(file, node);
    }
}

void filemgr_dirty_update_set_immutable(struct filemgr *file,
                                        struct filemgr_dirty_update_node *prev_node,
                                        struct filemgr_dirty_update_node *node)
{
    struct avl_node *a;
    struct filemgr_dirty_update_node *cur_node;
    struct list remove_queue;
    struct list_elem *le;

    if (!node) {
        return;
    }

    list_init(&remove_queue);

    spin_lock(&file->dirty_update_lock);
    node->immutable = true;

    // absorb all blocks that exist in the previous dirty update
    // but not exist in the current dirty update
    if (prev_node) {
        bool migration = false;
        struct avl_node *aa, *bb;
        struct filemgr_dirty_update_block *block, *block_copy, query;

        if (prev_node->immutable && atomic_get_uint32_t(&prev_node->ref_count) == 1) {
            // only the current thread is referring this dirty update entry.
            // we don't need to copy blocks; just migrate them directly.
            migration = true;
        }

        if (prev_node->expired) {
            // skip already copied node as its blocks are already in
            // the new node or DB file
            aa = NULL;
        } else {
            aa = avl_first(&prev_node->dirty_blocks);
        }

        while (aa) {
            block = _get_entry(aa, struct filemgr_dirty_update_block, avl);
            aa = avl_next(aa);

            if (block->immutable || !filemgr_is_writable(file, block->bid)) {
                // this block is already committed.
                // it can happen when previous dirty update was flushed but
                // was not closed as other handle was still referring it.

                // ignore this block and set the flag to avoid future copy
                // (filemgr_is_writable() alone is not enough because a block
                //  can become writable again due to circular block reuse).
                block->immutable = true;
                continue;
            }

            query.bid = block->bid;
            bb = avl_search(&node->dirty_blocks, &query.avl, _dirty_blocks_cmp);
            if (!bb) {
                // not exist in the current dirty update .. copy (or move) it
                if (migration) {
                    // move
                    avl_remove(&prev_node->dirty_blocks, &block->avl);
                    block_copy = block;
                } else {
                    // copy
                    block_copy = (struct filemgr_dirty_update_block *)
                                 calloc(1, sizeof(struct filemgr_dirty_update_block));
                    void *addr;
                    malloc_align(addr, FDB_SECTOR_SIZE, file->index_blocksize);
                    block_copy->addr = addr;
                    block_copy->bid = block->bid;
                    block_copy->immutable = block->immutable;
                    memcpy(block_copy->addr, block->addr, file->index_blocksize);
                }
                avl_insert(&node->dirty_blocks, &block_copy->avl, _dirty_blocks_cmp);
            }
        }

        // now we don't need to copy blocks in this node in the future
        prev_node->expired = true;
    }

    // set latest dirty update
    file->latest_dirty_update = node;

    // remove all previous dirty updates whose ref_count == 0
    // (except for 'node')
    a = avl_first(&file->dirty_update_idx);
    while (a) {
        cur_node = _get_entry(a, struct filemgr_dirty_update_node, avl);
        if (cur_node == node) {
            break;
        }
        a = avl_next(a);
        if (cur_node->immutable && atomic_get_uint32_t(&cur_node->ref_count) == 0 &&
            cur_node != node) {
            // detach from tree and insert into remove queue
            avl_remove(&file->dirty_update_idx, &cur_node->avl);
            list_push_front(&remove_queue, &cur_node->le);
        }
    }

    spin_unlock(&file->dirty_update_lock);

    le = list_begin(&remove_queue);
    while (le) {
        cur_node = _get_entry(le, struct filemgr_dirty_update_node, le);
        le = list_remove(&remove_queue, &cur_node->le);
        _filemgr_dirty_update_remove_node(file, cur_node);
    }
}

static void _filemgr_dirty_update_remove_node(struct filemgr *file,
                                              struct filemgr_dirty_update_node *node)
{
    struct avl_node *a;
    struct filemgr_dirty_update_block *block;

    if (!node) {
        return;
    }

    // free all dirty blocks belonging to this node
    a = avl_first(&node->dirty_blocks);
    while (a) {
        block = _get_entry(a, struct filemgr_dirty_update_block, avl);
        a = avl_next(a);
        avl_remove(&node->dirty_blocks, &block->avl);
        free_align(block->addr);
        free(block);
    }

    free(node);
}

void filemgr_dirty_update_remove_node(struct filemgr *file,
                                      struct filemgr_dirty_update_node *node)
{
    spin_lock(&file->dirty_update_lock);
    avl_remove(&file->dirty_update_idx, &node->avl);
    spin_unlock(&file->dirty_update_lock);

    _filemgr_dirty_update_remove_node(file, node);
}

void filemgr_dirty_update_close_node(struct filemgr *file,
                                     struct filemgr_dirty_update_node *node)
{
    if (!node) {
        return;
    }

    // just decrease the ref count
    // (any nodes whose ref_count==0 will be removed lazily)
    atomic_decr_uint32_t(&node->ref_count);
}

fdb_status filemgr_write_dirty(struct filemgr *file, bid_t bid, void *buf,
                               struct filemgr_dirty_update_node *node,
                               err_log_callback *log_callback)
{
    struct avl_node *a;
    struct filemgr_dirty_update_block *block, query;

    query.bid = bid;
    a = avl_search(&node->dirty_blocks, &query.avl, _dirty_blocks_cmp);
    if (a) {
        // already exist .. overwrite
        block = _get_entry(a, struct filemgr_dirty_update_block, avl);
    } else {
        // not exist .. create a new block for this update node
        block = (struct filemgr_dirty_update_block *)
                calloc(1, sizeof(struct filemgr_dirty_update_block));
        void *addr = NULL;
        malloc_align(addr, FDB_SECTOR_SIZE, file->index_blocksize);
        block->addr = addr;
        block->bid = bid;
        block->immutable = false;
        avl_insert(&node->dirty_blocks, &block->avl, _dirty_blocks_cmp);
    }

    memcpy(block->addr, buf, file->index_blocksize);
    return FDB_RESULT_SUCCESS;
}

fdb_status filemgr_read_dirty(struct filemgr *file, bid_t bid, void *buf,
                              struct filemgr_dirty_update_node *node_reader,
                              struct filemgr_dirty_update_node *node_writer,
                              err_log_callback *log_callback,
                              bool read_on_cache_miss)
{
    struct avl_node *a;
    struct filemgr_dirty_update_block *block, query;

    if (node_writer) {
        // search the current (being written / mutable) dirty update first
        query.bid = bid;
        a = avl_search(&node_writer->dirty_blocks, &query.avl, _dirty_blocks_cmp);
        if (a) {
            // exist .. directly read the dirty block
            block = _get_entry(a, struct filemgr_dirty_update_block, avl);
            memcpy(buf, block->addr, file->index_blocksize);
            return FDB_RESULT_SUCCESS;
        }
        // not exist .. search the latest immutable dirty update next
    }

    if (node_reader) {
        query.bid = bid;
        a = avl_search(&node_reader->dirty_blocks, &query.avl, _dirty_blocks_cmp);
        if (a) {
            // exist .. directly read the dirty block
            block = _get_entry(a, struct filemgr_dirty_update_block, avl);
            memcpy(buf, block->addr, file->index_blocksize);
            return FDB_RESULT_SUCCESS;
        }
    }
    
    uint64_t len = file->blocksize;
    // not exist in both dirty update entries .. call filemgr_read()
    return filemgr_read(file, bid, buf, &len, log_callback, read_on_cache_miss);
}

void _kvs_stat_set(struct filemgr *file,
                   fdb_kvs_id_t kv_id,
                   struct kvs_stat stat)
{
    if (kv_id == 0) {
        spin_lock(&file->lock);
        file->header.stat = stat;
        spin_unlock(&file->lock);
    } else {
        struct avl_node *a;
        struct kvs_node query, *node;
        struct kvs_header *kv_header = file->kv_header;

        spin_lock(&kv_header->lock);
        query.id = kv_id;
        a = avl_search(kv_header->idx_id, &query.avl_id, _kvs_stat_cmp);
        if (a) {
            node = _get_entry(a, struct kvs_node, avl_id);
            node->stat = stat;
        }
        spin_unlock(&kv_header->lock);
    }
}

static void _safe_add(uint64_t& src, int64_t& delta) {
    // Avoid overflow.
    if (delta < 0 && (int64_t)src + delta < 0) {
        src = 0;
    } else {
        src += delta;
    }
}

void _kvs_stat_update_attr(struct filemgr *file,
                           fdb_kvs_id_t kv_id,
                           kvs_stat_attr_t attr,
                           int64_t delta)
{
    spin_t *lock = NULL;
    struct kvs_stat *stat;

    if (kv_id == 0) {
        stat = &file->header.stat;
        lock = &file->lock;
        spin_lock(lock);
    } else {
        struct avl_node *a;
        struct kvs_node query, *node;
        struct kvs_header *kv_header = file->kv_header;

        lock = &kv_header->lock;
        spin_lock(lock);
        query.id = kv_id;
        a = avl_search(kv_header->idx_id, &query.avl_id, _kvs_stat_cmp);
        if (!a) {
            // KV instance corresponding to the kv_id is already removed
            spin_unlock(lock);
            return;
        }
        node = _get_entry(a, struct kvs_node, avl_id);
        stat = &node->stat;
    }

    if (attr == KVS_STAT_DATASIZE) {
        _safe_add(stat->datasize, delta);
    } else if (attr == KVS_STAT_NDOCS) {
        _safe_add(stat->ndocs, delta);
    } else if (attr == KVS_STAT_NDELETES) {
        _safe_add(stat->ndeletes, delta);
    } else if (attr == KVS_STAT_NLIVENODES) {
        _safe_add(stat->nlivenodes, delta);
    } else if (attr == KVS_STAT_WAL_NDELETES) {
        _safe_add(stat->wal_ndeletes, delta);
    } else if (attr == KVS_STAT_WAL_NDOCS) {
        _safe_add(stat->wal_ndocs, delta);
    } else if (attr == KVS_STAT_DELTASIZE) {
        stat->deltasize += delta;
    }
    spin_unlock(lock);
}

int _kvs_stat_get_kv_header(struct kvs_header *kv_header,
                            fdb_kvs_id_t kv_id,
                            struct kvs_stat *stat)
{
    int ret = 0;
    struct avl_node *a;
    struct kvs_node query, *node;

    query.id = kv_id;
    a = avl_search(kv_header->idx_id, &query.avl_id, _kvs_stat_cmp);
    if (a) {
        node = _get_entry(a, struct kvs_node, avl_id);
        *stat = node->stat;
    } else {
        ret = -1;
    }
    return ret;
}

fdb_seqnum_t _fdb_kvs_get_seqnum(struct kvs_header *kv_header,
                                 fdb_kvs_id_t id)
{
    fdb_seqnum_t seqnum;
    struct kvs_node query, *node;
    struct avl_node *a;

    spin_lock(&kv_header->lock);
    query.id = id;
    a = avl_search(kv_header->idx_id, &query.avl_id, _kvs_stat_cmp);
    if (a) {
        node = _get_entry(a, struct kvs_node, avl_id);
        seqnum = node->seqnum;
    } else {
        // not existing KV ID.
        // this is necessary for _fdb_restore_wal()
        // not to restore documents in deleted KV store.
        seqnum = 0;
    }
    spin_unlock(&kv_header->lock);

    return seqnum;
}

fdb_seqnum_t fdb_kvs_get_seqnum(struct filemgr *file,
                                fdb_kvs_id_t id)
{
    if (id == 0) {
        // default KV instance
        return filemgr_get_seqnum(file);
    }

    return _fdb_kvs_get_seqnum(file->kv_header, id);
}

int _kvs_stat_get(struct filemgr *file,
                  fdb_kvs_id_t kv_id,
                  struct kvs_stat *stat)
{
    int ret = 0;

    if (kv_id == 0) {
        spin_lock(&file->lock);
        *stat = file->header.stat;
        spin_unlock(&file->lock);
    } else {
        struct kvs_header *kv_header = file->kv_header;

        spin_lock(&kv_header->lock);
        ret = _kvs_stat_get_kv_header(kv_header, kv_id, stat);
        spin_unlock(&kv_header->lock);
    }

    return ret;
}

uint64_t _kvs_stat_get_sum(struct filemgr *file,
                           kvs_stat_attr_t attr)
{
    struct avl_node *a;
    struct kvs_node *node;
    struct kvs_header *kv_header = file->kv_header;

    uint64_t ret = 0;
    spin_lock(&file->lock);
    if (attr == KVS_STAT_DATASIZE) {
        ret += file->header.stat.datasize;
    } else if (attr == KVS_STAT_NDOCS) {
        ret += file->header.stat.ndocs;
    } else if (attr == KVS_STAT_NDELETES) {
        ret += file->header.stat.ndeletes;
    } else if (attr == KVS_STAT_NLIVENODES) {
        ret += file->header.stat.nlivenodes;
    } else if (attr == KVS_STAT_WAL_NDELETES) {
        ret += file->header.stat.wal_ndeletes;
    } else if (attr == KVS_STAT_WAL_NDOCS) {
        ret += file->header.stat.wal_ndocs;
    } else if (attr == KVS_STAT_DELTASIZE) {
        ret += file->header.stat.deltasize;
    }
    spin_unlock(&file->lock);

    if (kv_header) {
        spin_lock(&kv_header->lock);
        a = avl_first(kv_header->idx_id);
        while (a) {
            node = _get_entry(a, struct kvs_node, avl_id);
            a = avl_next(&node->avl_id);

            if (attr == KVS_STAT_DATASIZE) {
                ret += node->stat.datasize;
            } else if (attr == KVS_STAT_NDOCS) {
                ret += node->stat.ndocs;
            } else if (attr == KVS_STAT_NDELETES) {
                ret += node->stat.ndeletes;
            } else if (attr == KVS_STAT_NLIVENODES) {
                ret += node->stat.nlivenodes;
            } else if (attr == KVS_STAT_WAL_NDELETES) {
                ret += node->stat.wal_ndeletes;
            } else if (attr == KVS_STAT_WAL_NDOCS) {
                ret += node->stat.wal_ndocs;
            } else if (attr == KVS_STAT_DELTASIZE) {
                ret += node->stat.deltasize;
            }
        }
        spin_unlock(&kv_header->lock);
    }

    return ret;
}

int _kvs_ops_stat_get_kv_header(struct kvs_header *kv_header,
                                fdb_kvs_id_t kv_id,
                                struct kvs_ops_stat *stat)
{
    int ret = 0;
    struct avl_node *a;
    struct kvs_node query, *node;

    query.id = kv_id;
    a = avl_search(kv_header->idx_id, &query.avl_id, _kvs_stat_cmp);
    if (a) {
        node = _get_entry(a, struct kvs_node, avl_id);
        *stat = node->op_stat;
    } else {
        ret = -1;
    }
    return ret;
}

int _kvs_ops_stat_get(struct filemgr *file,
                      fdb_kvs_id_t kv_id,
                      struct kvs_ops_stat *stat)
{
    int ret = 0;

    if (kv_id == 0) {
        spin_lock(&file->lock);
        *stat = file->header.op_stat;
        spin_unlock(&file->lock);
    } else {
        struct kvs_header *kv_header = file->kv_header;

        spin_lock(&kv_header->lock);
        ret = _kvs_ops_stat_get_kv_header(kv_header, kv_id, stat);
        spin_unlock(&kv_header->lock);
    }

    return ret;
}

void _init_op_stats(struct kvs_ops_stat *stat) {
    atomic_init_uint64_t(&stat->num_sets, 0);
    atomic_init_uint64_t(&stat->num_dels, 0);
    atomic_init_uint64_t(&stat->num_commits, 0);
    atomic_init_uint64_t(&stat->num_compacts, 0);
    atomic_init_uint64_t(&stat->num_gets, 0);
    atomic_init_uint64_t(&stat->num_iterator_gets, 0);
    atomic_init_uint64_t(&stat->num_iterator_moves, 0);
}

struct kvs_ops_stat *filemgr_get_ops_stats(struct filemgr *file,
                                           struct kvs_info *kvs)
{
    struct kvs_ops_stat *stat = NULL;
    if (!kvs || (kvs && kvs->id == 0)) {
        return &file->header.op_stat;
    } else {
        struct kvs_header *kv_header = file->kv_header;
        struct avl_node *a;
        struct kvs_node query, *node;
        spin_lock(&kv_header->lock);
        query.id = kvs->id;
        a = avl_search(kv_header->idx_id, &query.avl_id, _kvs_stat_cmp);
        if (a) {
            node = _get_entry(a, struct kvs_node, avl_id);
            stat = &node->op_stat;
        }
        spin_unlock(&kv_header->lock);
    }
    return stat;
}

const char *filemgr_latency_stat_name(fdb_latency_stat_type stat)
{
    switch(stat) {
        case FDB_LATENCY_SETS:          return "sets            ";
        case FDB_LATENCY_GETS:          return "gets            ";
        case FDB_LATENCY_SNAP_INMEM:    return "in-mem_snapshot ";
        case FDB_LATENCY_SNAP_DUR:      return "durable_snapshot";
        case FDB_LATENCY_COMMITS:       return "commits         ";
        case FDB_LATENCY_COMPACTS:      return "compact         ";
        case FDB_LATENCY_ITR_INIT:      return "itr-init        ";
        case FDB_LATENCY_ITR_SEQ_INIT:  return "itr-seq-ini     ";
        case FDB_LATENCY_ITR_NEXT:      return "itr-next        ";
        case FDB_LATENCY_ITR_PREV:      return "itr-prev        ";
        case FDB_LATENCY_ITR_GET:       return "itr-get         ";
        case FDB_LATENCY_ITR_GET_META:  return "itr-get-meta    ";
        case FDB_LATENCY_ITR_SEEK:      return "itr-seek        ";
        case FDB_LATENCY_ITR_SEEK_MAX:  return "itr-seek-max    ";
        case FDB_LATENCY_ITR_SEEK_MIN:  return "itr-seek-min    ";
        case FDB_LATENCY_ITR_CLOSE:     return "itr-close       ";
        case FDB_LATENCY_OPEN:          return "fdb_open        ";
        case FDB_LATENCY_KVS_OPEN:      return "fdb_kvs_open    ";
        case FDB_LATENCY_SNAP_CLONE:    return "clone-snapshot  ";
    }
    return NULL;
}

#ifdef _LATENCY_STATS
void filemgr_init_latency_stat(struct latency_stat *val) {
    atomic_init_uint32_t(&val->lat_max, 0);
    atomic_init_uint32_t(&val->lat_min, (uint32_t)(-1));
    atomic_init_uint64_t(&val->lat_sum, 0);
    atomic_init_uint64_t(&val->lat_num, 0);
}

void filemgr_migrate_latency_stats(struct filemgr *src, struct filemgr *dst) {
    for (int type = 0; type < FDB_LATENCY_NUM_STATS; ++type) {
        atomic_store_uint32_t(&dst->lat_stats[type].lat_min,
                              atomic_get_uint32_t(&src->lat_stats[type].lat_min),
                              std::memory_order_relaxed);
        atomic_store_uint32_t(&dst->lat_stats[type].lat_max,
                              atomic_get_uint32_t(&src->lat_stats[type].lat_max),
                              std::memory_order_relaxed);
        atomic_store_uint64_t(&dst->lat_stats[type].lat_sum,
                              atomic_get_uint64_t(&src->lat_stats[type].lat_sum),
                              std::memory_order_relaxed);
        atomic_store_uint64_t(&dst->lat_stats[type].lat_num,
                              atomic_get_uint64_t(&src->lat_stats[type].lat_num),
                              std::memory_order_relaxed);
    }
}

void filemgr_destroy_latency_stat(struct latency_stat *val) {
    (void) val;
}

void filemgr_update_latency_stat(struct filemgr *file,
                                 fdb_latency_stat_type type,
                                 uint32_t val)
{
    int retry = MAX_STAT_UPDATE_RETRIES;
    do {
        uint32_t lat_max = atomic_get_uint32_t(&file->lat_stats[type].lat_max,
                                               std::memory_order_relaxed);
        if (lat_max < val) {
            if (!atomic_cas_uint32_t(&file->lat_stats[type].lat_max,
                                     lat_max, val)) {
                continue;
            }
        }
        break;
    } while (--retry);
    retry = MAX_STAT_UPDATE_RETRIES;
    do {
        uint32_t lat_min = atomic_get_uint32_t(&file->lat_stats[type].lat_min,
                                               std::memory_order_relaxed);
        if (val < lat_min) {
            if (!atomic_cas_uint32_t(&file->lat_stats[type].lat_min,
                                     lat_min, val)) {
                continue;
            }
        }
        break;
    } while (--retry);
    atomic_add_uint64_t(&file->lat_stats[type].lat_sum, val,
                        std::memory_order_relaxed);
    atomic_incr_uint64_t(&file->lat_stats[type].lat_num,
                         std::memory_order_relaxed);
}

void filemgr_get_latency_stat(struct filemgr *file, fdb_latency_stat_type type,
                              fdb_latency_stat *stat)
{
    uint64_t num = atomic_get_uint64_t(&file->lat_stats[type].lat_num,
                                       std::memory_order_relaxed);
    if (!num) {
        memset(stat, 0, sizeof(fdb_latency_stat));
        return;
    }
    stat->lat_max = atomic_get_uint32_t(&file->lat_stats[type].lat_max,
                                        std::memory_order_relaxed);
    stat->lat_min = atomic_get_uint32_t(&file->lat_stats[type].lat_min,
                                        std::memory_order_relaxed);
    stat->lat_count = num;
    stat->lat_avg = atomic_get_uint64_t(&file->lat_stats[type].lat_sum,
                                        std::memory_order_relaxed) / num;
}

#ifdef _LATENCY_STATS_DUMP_TO_FILE
static const int _MAX_STATSFILE_LEN = FDB_MAX_FILENAME_LEN + 4;
void filemgr_dump_latency_stat(struct filemgr *file,
                               err_log_callback *log_callback) {
    FILE *lat_file;
    char latency_file_path[_MAX_STATSFILE_LEN];
    strncpy(latency_file_path, file->filename, _MAX_STATSFILE_LEN);
    strncat(latency_file_path, ".lat", _MAX_STATSFILE_LEN);
    lat_file = fopen(latency_file_path, "a");
    if (!lat_file) {
        fdb_status status = FDB_RESULT_OPEN_FAIL;
        const char *msg = "Warning: Unable to open latency stats file '%s'\n";
        fdb_log(log_callback, FDB_LOG_ERROR, status, msg, latency_file_path);
        return;
    }
    fprintf(lat_file, "latency(us)\t\tmin\t\tavg\t\tmax\t\tnum_samples\n");
    for (int i = 0; i < FDB_LATENCY_NUM_STATS; ++i) {
        uint32_t avg;
        uint64_t num;
        num = atomic_get_uint64_t(&file->lat_stats[i].lat_num,
                                  std::memory_order_relaxed);
        if (!num) {
            continue;
        }
        avg = atomic_get_uint64_t(&file->lat_stats[i].lat_sum,
                                  std::memory_order_relaxed) / num;
        fprintf(lat_file, "%s:\t\t%u\t\t%u\t\t%u\t\t%" _F64 "\n",
                filemgr_latency_stat_name(i),
                atomic_get_uint32_t(&file->lat_stats[i].lat_min,
                                    std::memory_order_relaxed),
                avg,
                atomic_get_uint32_t(&file->lat_stats[i].lat_max,
                                    std::memory_order_relaxed),
                num);
    }
    fflush(lat_file);
    fclose(lat_file);
}
#endif // _LATENCY_STATS_DUMP_TO_FILE
#endif // _LATENCY_STATS

void buf2kvid(size_t chunksize, void *buf, fdb_kvs_id_t *id)
{
    size_t size_id = sizeof(fdb_kvs_id_t);
    fdb_kvs_id_t temp;

    if (chunksize == size_id) {
        temp = *((fdb_kvs_id_t*)buf);
    } else if (chunksize < size_id) {
        temp = 0;
        memcpy((uint8_t*)&temp + (size_id - chunksize), buf, chunksize);
    } else { // chunksize > sizeof(fdb_kvs_id_t)
        memcpy(&temp, (uint8_t*)buf + (chunksize - size_id), size_id);
    }
    *id = _endian_decode(temp);
}

void kvid2buf(size_t chunksize, fdb_kvs_id_t id, void *buf)
{
    size_t size_id = sizeof(fdb_kvs_id_t);
    id = _endian_encode(id);

    if (chunksize == size_id) {
        memcpy(buf, &id, size_id);
    } else if (chunksize < size_id) {
        memcpy(buf, (uint8_t*)&id + (size_id - chunksize), chunksize);
    } else { // chunksize > sizeof(fdb_kvs_id_t)
        memset(buf, 0x0, chunksize - size_id);
        memcpy((uint8_t*)buf + (chunksize - size_id), &id, size_id);
    }
}

void buf2buf(size_t chunksize_src, void *buf_src,
             size_t chunksize_dst, void *buf_dst)
{
    if (chunksize_dst == chunksize_src) {
        memcpy(buf_dst, buf_src, chunksize_src);
    } else if (chunksize_dst < chunksize_src) {
        memcpy(buf_dst, (uint8_t*)buf_src + (chunksize_src - chunksize_dst),
               chunksize_dst);
    } else { // chunksize_dst > chunksize_src
        memset(buf_dst, 0x0, chunksize_dst - chunksize_src);
        memcpy((uint8_t*)buf_dst + (chunksize_dst - chunksize_src),
               buf_src, chunksize_src);
    }
}

fdb_status convert_errno_to_fdb_status(int errno_value,
                                       fdb_status default_status)
{
    switch (errno_value) {
    case EACCES:
        return FDB_RESULT_EACCESS;
    case EEXIST:
        return FDB_RESULT_EEXIST;
    case EFAULT:
        return FDB_RESULT_EFAULT;
    case EFBIG:
        return FDB_RESULT_EFBIG;
    case EINVAL:
        return FDB_RESULT_EINVAL;
    case EISDIR:
        return FDB_RESULT_EISDIR;
    case ELOOP:
        return FDB_RESULT_ELOOP;
    case EMFILE:
        return FDB_RESULT_EMFILE;
    case ENAMETOOLONG:
        return FDB_RESULT_ENAMETOOLONG;
    case ENFILE:
        return FDB_RESULT_ENFILE;
    case ENODEV:
        return FDB_RESULT_ENODEV;
    case ENOENT:
        return FDB_RESULT_NO_SUCH_FILE;
    case ENOMEM:
        return FDB_RESULT_ENOMEM;
    case ENOSPC:
        return FDB_RESULT_ENOSPC;
    case ENOTDIR:
        return FDB_RESULT_ENOTDIR;
    case ENXIO:
        return FDB_RESULT_ENXIO;
    case EOPNOTSUPP:
        return FDB_RESULT_EOPNOTSUPP;
    case EOVERFLOW:
        return FDB_RESULT_EOVERFLOW;
    case EPERM:
        return FDB_RESULT_EPERM;
    case EROFS:
        return FDB_RESULT_EROFS;
    case EBADF:
        return FDB_RESULT_EBADF;
    case EIO:
        return FDB_RESULT_EIO;
    case ENOBUFS:
        return FDB_RESULT_ENOBUFS;
    case EAGAIN:
        return FDB_RESULT_EAGAIN;

    default:
        return default_status;
    }
}

void _print_cache_stats() {
    bcache_print_items();
}


