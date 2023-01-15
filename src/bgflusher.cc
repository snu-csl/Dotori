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

// TODO: Consolidate Various ForestDB Tasks into a Shared Thread Pool

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#if !defined(WIN32) && !defined(_WIN32)
#include <sys/time.h>
#include <dirent.h>
#include <unistd.h>
#endif

#include "libforestdb/forestdb.h"
#include "fdb_internal.h"
#include "filemgr.h"
#include "avltree.h"
#include "common.h"
#include "bgflusher.h"
#include "memleak.h"
#include "time_utils.h"
#include "atomic.h"
#include "hbtrie.h"
#include "wal.h"

#ifdef __DEBUG
#ifndef __DEBUG_CPT
    #undef DBG
    #undef DBGCMD
    #undef DBGSW
    #define DBG(...)
    #define DBGCMD(...)
    #define DBGSW(n, ...)
#endif
#endif

// variables for initialization
volatile uint8_t bgflusher_initialized = 0;
static mutex_t bgf_lock;

static size_t num_bgflusher_threads = DEFAULT_NUM_BGFLUSHER_THREADS;
static thread_t *bgflusher_tids = NULL;
static size_t num_wal_preloader_threads = DEFAULT_NUM_WAL_PRELOADER_THREADS;
static thread_t *wal_preloader_tids = NULL;

static size_t sleep_duration = FDB_BGFLUSHER_SLEEP_DURATION;

static mutex_t sync_mutex;
static thread_cond_t sync_cond;

static volatile uint8_t bgflusher_terminate_signal = 0;

static struct avl_tree openfiles;
static struct avl_tree openhandles;

struct openfiles_elem {
    char filename[FDB_MAX_FILENAME_LEN];
    struct filemgr *file;
    fdb_config config;
    uint32_t register_count;
    bool background_flush_in_progress;
    err_log_callback *log_callback;
    struct avl_node avl;
};

struct openhandles_elem {
    char filename[FDB_MAX_FILENAME_LEN];
    char dbname[FDB_MAX_FILENAME_LEN];
    struct filemgr *file;
    fdb_kvs_handle *dbhandle;
    fdb_file_handle *dbfile;
    fdb_config config;
    fdb_kvs_config kvs_config;
    uint32_t register_count;
    bool background_flush_in_progress;
    err_log_callback *log_callback;
    struct avl_node avl;
    wal_flush_func *flush_func;
};


// compares file names
static int _bgflusher_cmp(struct avl_node *a, struct avl_node *b, void *aux)
{
    struct openfiles_elem *aa, *bb;
    aa = _get_entry(a, struct openfiles_elem, avl);
    bb = _get_entry(b, struct openfiles_elem, avl);
    return strncmp(aa->filename, bb->filename, FDB_MAX_FILENAME_LEN);
}

// compares file names
static int _preloader_cmp(struct avl_node *a, struct avl_node *b, void *aux)
{
    struct openhandles_elem *aa, *bb;
    aa = _get_entry(a, struct openhandles_elem, avl);
    bb = _get_entry(b, struct openhandles_elem, avl);
    return strncmp(aa->dbname, bb->dbname, FDB_MAX_FILENAME_LEN);
}

static void * bgflusher_thread(void *voidargs)
{
    fdb_status fs;
    struct avl_node *a;
    struct filemgr *file;
    struct openfiles_elem *elem;
    err_log_callback *log_callback = NULL;

    return NULL;

    while (1) {
        uint64_t num_blocks = 0;

        mutex_lock(&bgf_lock);
        a = avl_first(&openfiles);
        while(a) {
            filemgr_open_result ffs;
            elem = _get_entry(a, struct openfiles_elem, avl);
            file = elem->file;
            if (!file) {
                a = avl_next(a);
                avl_remove(&openfiles, &elem->avl);
                free(elem);
                continue;
            }

            if (elem->background_flush_in_progress) {
                a = avl_next(a);
            } else {
                elem->background_flush_in_progress = true;
                log_callback = elem->log_callback;
                ffs = filemgr_open(file->filename, file->ops,
                        file->config, log_callback);
                fs = (fdb_status)ffs.rv;
                mutex_unlock(&bgf_lock);
                if (fs == FDB_RESULT_SUCCESS) {
                    num_blocks += filemgr_flush_immutable(file,
                                                          log_callback);
                    filemgr_close(file, 0, file->filename, log_callback);
                } else {
                    fdb_log(log_callback, FDB_LOG_ERROR, fs,
                            "Failed to open the file '%s' for background flushing\n.",
                            file->filename);
                }
                mutex_lock(&bgf_lock);
                elem->background_flush_in_progress = false;
                a = avl_next(&elem->avl);
                if (bgflusher_terminate_signal) {
                    mutex_unlock(&bgf_lock);
                    return NULL;
                }
            }
        }
        mutex_unlock(&bgf_lock);

        mutex_lock(&sync_mutex);
        if (bgflusher_terminate_signal) {
            mutex_unlock(&sync_mutex);
            break;
        }
        if (!num_blocks) {
            thread_cond_timedwait(&sync_cond, &sync_mutex,
                                  (unsigned)(sleep_duration * 1000));
        }
        if (bgflusher_terminate_signal) {
            mutex_unlock(&sync_mutex);
            break;
        }
        mutex_unlock(&sync_mutex);
    }
    return NULL;
}

// static void _fdb_sync_dirty_root(fdb_kvs_handle *handle)
// {
//     if (handle->config.do_not_search_wal) {
//         return;
//     }

//     bid_t dirty_idtree_root = BLK_NOT_FOUND;
//     bid_t dirty_seqtree_root = BLK_NOT_FOUND;

//     if (handle->shandle) {
//         // skip snapshot
//         return;
//     }

//     struct filemgr_dirty_update_node *dirty_update;
//     dirty_update = filemgr_dirty_update_get_latest(handle->file);
//     btreeblk_set_dirty_update(handle->bhandle, dirty_update);

//     if (dirty_update) {
//         filemgr_dirty_update_get_root(handle->file, dirty_update,
//                                       &dirty_idtree_root, &dirty_seqtree_root);
//         _fdb_import_dirty_root(handle, dirty_idtree_root, dirty_seqtree_root);
//         btreeblk_discard_blocks(handle->bhandle);
//     }

//     return;
// }

// static void _fdb_release_dirty_root(fdb_kvs_handle *handle)
// {
//     if (!handle->shandle) {
//         struct filemgr_dirty_update_node *dirty_update;
//         dirty_update = btreeblk_get_dirty_update(handle->bhandle);
//         if (dirty_update) {
//             filemgr_dirty_update_close_node(handle->file, dirty_update);
//             btreeblk_clear_dirty_update(handle->bhandle);
//         }
//     }
// }

// std::pair<void*, uint32_t> _get_preload_key(struct filemgr* file,
//                                             struct openhandles_elem *elem)
// {
//     std::pair<void*, uint32_t> ret;

//     std::unique_lock<std::mutex> lock(file->preload_lock);
//     while (file->preload_keys->empty())
//     {
//         if (file->preload_cond.wait_for(lock, std::chrono::seconds(1)) == std::cv_status::timeout)
//         {
//             fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
//                     "1 second without any keys to preload");
//         }

//         if(bgflusher_terminate_signal || !elem->file) {
//             return ret;
//         }
//     }

//     ret = file->preload_keys->front();
//     file->preload_keys->pop();

//     return ret;
// }

// static void * wal_preloader_thread(void *voidargs)
// {
//     struct avl_node *a;
//     struct filemgr *file;
//     fdb_kvs_handle *dbhandle = NULL;
//     fdb_file_handle *wal_preload_fhandle = NULL;
//     // char dbname[FDB_MAX_FILENAME_LEN];
//     struct openhandles_elem *elem;
//     std::pair<void*, uint32_t> next_preload;
//     hbtrie_result hr;
//     fdb_status status = FDB_RESULT_SUCCESS;

//     while (1) {
//         mutex_lock(&bgf_lock);
//         a = avl_first(&openhandles);
        
//         while(a) {
//             elem = _get_entry(a, struct openhandles_elem, avl);

//             if (!elem->file) {
//                 a = avl_next(a);

//                 if(elem->dbhandle) {
//                     status = fdb_kvs_close(elem->dbhandle);
//                     assert(status == FDB_RESULT_SUCCESS);
//                     elem->dbhandle = NULL;
//                 }

//                 if(elem->dbfile) {
//                     status = fdb_close(elem->dbfile);
//                     assert(status == FDB_RESULT_SUCCESS);
//                     elem->dbfile = NULL;
//                 }

//                 avl_remove(&openhandles, &elem->avl);
//                 continue;
//             }

//             dbhandle = elem->dbhandle;
//             wal_preload_fhandle = elem->dbfile;
//             file = elem->file;

//             if (bgflusher_terminate_signal) {
//                 elem->dbhandle = NULL;
//                 elem->dbfile = NULL;
//                 mutex_unlock(&bgf_lock);
//                 return NULL;
//             }

//             mutex_unlock(&bgf_lock);

//             next_preload = _get_preload_key(file, elem);

//             if(next_preload.first == NULL) {
//                 mutex_lock(&bgf_lock);
//                 continue;
//             }

            
//                 For each old offset <-> key pair to be preloaded,
//                 we perform an async trie search. The process is the
//                 same as a normal trie search, except when we go to
//                 read the key we put the second half of the search
//                 inside a callback that's carried out after the read.
//                 See _fdb_readkey_wrap_async, _fdb_readkey_wrap_async_cb
//                 in hbtrie.cc. A HBTRIE_RESULT_FAIL indicates a shared
//                 chunk wasn't found and the item definitely doesn't exist.
//                 Otherwise, preload_count will be increased in the read
//                 callback.
            

//             fdb_sync_db_header(dbhandle);
//             _fdb_sync_dirty_root(dbhandle);
//             hr = hbtrie_find_async(dbhandle->trie, 
//                                    next_preload.first,
//                                    next_preload.second, 
//                                    (void*)&file->preload_count,
//                                    NULL);
//             btreeblk_end(dbhandle->bhandle);
//             _fdb_release_dirty_root(dbhandle);

//             std::string k = std::string((char*)next_preload.first, 
//                                                next_preload.second);
//             if(hr != HBTRIE_RESULT_SUCCESS) {
//                 fdb_log(NULL, FDB_LOG_DEBUG,
//                     FDB_RESULT_SUCCESS,
//                     "Tried to preload %s but wasn't found",
//                     k.c_str());
//                 atomic_incr_uint64_t(&file->preload_count);
//             } else {
//                 fdb_log(NULL, FDB_LOG_DEBUG,
//                     FDB_RESULT_WRITE_FAIL,
//                     "Key %s existed in trie for preload", 
//                     k.c_str());
//             }

//             free(next_preload.first);

//             mutex_lock(&bgf_lock);
//             a = avl_next(&elem->avl);
//         }
//         mutex_unlock(&bgf_lock);

//         mutex_lock(&sync_mutex);
//         if (bgflusher_terminate_signal) {
//             mutex_unlock(&sync_mutex);
//             break;
//         }
//         if (bgflusher_terminate_signal) {
//             mutex_unlock(&sync_mutex);
//             break;
//         }
//         mutex_unlock(&sync_mutex);
//     }
    
//     return NULL;
// }

void bgflusher_init(struct bgflusher_config *config)
{
    if (!bgflusher_initialized) {
        // Note that this function is synchronized by spin lock in fdb_init API.
        mutex_init(&bgf_lock);

        mutex_lock(&bgf_lock);
        if (!bgflusher_initialized) {
            // initialize
            avl_init(&openfiles, NULL);

            bgflusher_terminate_signal = 0;

            mutex_init(&sync_mutex);
            thread_cond_init(&sync_cond);

            // create worker threads
            num_bgflusher_threads = config->num_threads;
            bgflusher_tids = (thread_t *) calloc(num_bgflusher_threads,
                                                 sizeof(thread_t));
            for (size_t i = 0; i < num_bgflusher_threads; ++i) {
                thread_create(&bgflusher_tids[i], bgflusher_thread, NULL);
            }
            (void)bgflusher_thread;
            (void)_preloader_cmp;

            // num_wal_preloader_threads = config->num_threads;
            // wal_preloader_tids = (thread_t *) calloc(num_wal_preloader_threads,
            //                                      sizeof(thread_t));
            // for (size_t i = 0; i < num_wal_preloader_threads; ++i) {
            //     thread_create(&wal_preloader_tids[i], wal_preloader_thread, NULL);
            // }

            bgflusher_initialized = 1;
        }
        mutex_unlock(&bgf_lock);
    }
}

void bgflusher_shutdown()
{
    void *ret;
    struct avl_node *a = NULL;
    struct openfiles_elem *elem;
    struct openhandles_elem *h_elem;

    if (!bgflusher_tids && !wal_preloader_tids) {
        return;
    }

    // set terminate signal
    mutex_lock(&sync_mutex);
    bgflusher_terminate_signal = 1;
    thread_cond_broadcast(&sync_cond);
    mutex_unlock(&sync_mutex);

    if(bgflusher_tids) {
        for (size_t i = 0; i < num_bgflusher_threads; ++i) {
            thread_join(bgflusher_tids[i], &ret);
        }
        free(bgflusher_tids);
        bgflusher_tids = NULL;
    }

    if(wal_preloader_tids) {
        for (size_t i = 0; i < num_wal_preloader_threads; ++i) {
            thread_join(wal_preloader_tids[i], &ret);
        }
        free(wal_preloader_tids);
        wal_preloader_tids = NULL;
    }

    mutex_lock(&bgf_lock);
    // free all elems in the tree
    a = avl_first(&openfiles);
    while (a) {
        elem = _get_entry(a, struct openfiles_elem, avl);
        a = avl_next(a);

        avl_remove(&openfiles, &elem->avl);
        free(elem);
    }

    a = avl_first(&openhandles);
    while (a) {
        h_elem = _get_entry(a, struct openhandles_elem, avl);
        a = avl_next(a);

        avl_remove(&openhandles, &h_elem->avl);
        free(h_elem);
    }

    sleep_duration = FDB_BGFLUSHER_SLEEP_DURATION;
    bgflusher_initialized = 0;
    mutex_destroy(&sync_mutex);
    thread_cond_destroy(&sync_cond);
    mutex_unlock(&bgf_lock);

    mutex_destroy(&bgf_lock);
}

fdb_status bgflusher_register_file(struct filemgr *file,
                                   fdb_config *config,
                                   err_log_callback *log_callback)
{
    file_status_t fstatus;
    fdb_status fs = FDB_RESULT_SUCCESS;
    struct avl_node *a = NULL;
    struct openfiles_elem query, *elem;

    // Ignore files whose status is FILE_COMPACT_OLD to prevent
    // reinserting of files undergoing compaction if it is in the catchup phase
    // Also ignore files whose status is REMOVED_PENDING.
    fstatus = filemgr_get_file_status(file);
    if (fstatus == FILE_COMPACT_OLD ||
        fstatus == FILE_REMOVED_PENDING) {
        return fs;
    }

    strcpy(query.filename, file->filename);
    // first search the existing file
    mutex_lock(&bgf_lock);
    a = avl_search(&openfiles, &query.avl, _bgflusher_cmp);
    if (a == NULL) {
        // doesn't exist
        // create elem and insert into tree
        elem = (struct openfiles_elem *)calloc(1, sizeof(struct openfiles_elem));
        elem->file = file;
        strcpy(elem->filename, file->filename);
        elem->config = *config;
        elem->register_count = 1;
        elem->background_flush_in_progress = false;
        elem->log_callback = log_callback;
        avl_insert(&openfiles, &elem->avl, _bgflusher_cmp);
    } else {
        // already exists
        elem = _get_entry(a, struct openfiles_elem, avl);
        if (!elem->file) {
            elem->file = file;
        }
        elem->register_count++;
        elem->log_callback = log_callback; // use the latest
    }
    mutex_unlock(&bgf_lock);
    return fs;
}

fdb_status bgflusher_register_kv_handle(struct filemgr *file,
                                        fdb_file_handle *fhandle,
                                        fdb_kvs_handle *dbhandle,
                                        const char *filename,
                                        const char *dbname,
                                        fdb_config *config,
                                        fdb_kvs_config *kvs_config,
                                        err_log_callback *log_callback,
                                        wal_flush_func *flush_func)
{
    // assert(0);
    // file_status_t fstatus;
    // fdb_status fs = FDB_RESULT_SUCCESS;
    // struct avl_node *a = NULL;
    // struct openhandles_elem query, *elem;

    // // Ignore files whose status is FILE_COMPACT_OLD to prevent
    // // reinserting of files undergoing compaction if it is in the catchup phase
    // // Also ignore files whose status is REMOVED_PENDING.
    // // fstatus = filemgr_get_file_status(file);
    // // if (fstatus == FILE_COMPACT_OLD ||
    // //     fstatus == FILE_REMOVED_PENDING) {
    // //     return fs;
    // // }

    // strcpy(query.dbname, dbname);
    // // first search the existing file
    // mutex_lock(&bgf_lock);
    // a = avl_search(&openhandles, &query.avl, _preloader_cmp);
    // if (a == NULL) {
    //     // doesn't exist
    //     // create elem and insert into tree
    //     elem = (struct openhandles_elem *)calloc(1, sizeof(struct openhandles_elem));
    //     elem->file = file;
    //     elem->dbfile = fhandle;
    //     elem->dbhandle = dbhandle;
    //     strcpy(elem->filename, filename);
    //     strcpy(elem->dbname, dbname);
    //     elem->config = *config;
    //     elem->register_count = 1;
    //     elem->background_flush_in_progress = false;
    //     elem->log_callback = log_callback;
    //     elem->flush_func = flush_func;
    //     avl_insert(&openhandles, &elem->avl, _preloader_cmp);

    //     /*
    //         If a DB handle is opened here and wal_preloading_started 
    //         is set to 1 when fdb_sets are already being called 
    //         (e.g. start calling fdb_set directly after opening a DB), 
    //         then not every key will be preloaded. Set the preload count 
    //         to a very high number here so that the initial preload passes 
    //         (see _wal_flush).
    //     */

    //     atomic_store_uint64_t(&elem->file->preload_count, 0xFFFFFFFF);
    //     elem->file->wal_preloading_started = 1;

    // } else {
    //     // already exists
    //     elem = _get_entry(a, struct openhandles_elem, avl);
    //     if (!elem->file) {
    //         elem->file = file;
    //     }

    //     if (!elem->dbfile) {
    //         elem->dbfile = fhandle;
    //     }

    //     elem->register_count++;
    //     elem->log_callback = log_callback; // use the latest
    //     elem->flush_func = flush_func;
    // }

    // mutex_unlock(&bgf_lock);
    return FDB_RESULT_SUCCESS;
}

void bgflusher_switch_file(struct filemgr *old_file, struct filemgr *new_file,
                           err_log_callback *log_callback)
{
    struct avl_node *a = NULL;
    struct openfiles_elem query, *elem;

    strcpy(query.filename, old_file->filename);
    mutex_lock(&bgf_lock);
    a = avl_search(&openfiles, &query.avl, _bgflusher_cmp);
    if (a) {
        elem = _get_entry(a, struct openfiles_elem, avl);
        avl_remove(&openfiles, a);
        strcpy(elem->filename, new_file->filename);
        elem->file = new_file;
        elem->register_count = 1;
        elem->background_flush_in_progress = false;
        avl_insert(&openfiles, &elem->avl, _bgflusher_cmp);
        mutex_unlock(&bgf_lock);
    } else {
        mutex_unlock(&bgf_lock);
    }
}

void bgflusher_deregister_file(struct filemgr *file)
{
    struct avl_node *a = NULL;
    struct openfiles_elem query, *elem;

    strcpy(query.filename, file->filename);
    mutex_lock(&bgf_lock);
    a = avl_search(&openfiles, &query.avl, _bgflusher_cmp);
    if (a) {
        elem = _get_entry(a, struct openfiles_elem, avl);
        if ((--elem->register_count) == 0) {
            // if no handle refers this file
            if (elem->background_flush_in_progress) {
                // Background flusher is writing blocks while the file is closed.
                // Do not remove 'elem' for now. The 'elem' will be automatically
                // removed once background flushing is done. Set elem->file
                // to NULL to indicate this intent.
                elem->file = NULL;
            } else {
                // remove from the tree
                avl_remove(&openfiles, &elem->avl);
                free(elem);
            }
        }
    }

    mutex_unlock(&bgf_lock);
}

void bgflusher_deregister_kv_handle(const char *dbname)
{
    assert(0);
    struct avl_node *a = NULL;
    struct openhandles_elem query, *elem;

    strcpy(query.dbname, dbname);
    mutex_lock(&bgf_lock);
    a = avl_search(&openhandles, &query.avl, _preloader_cmp);
    if (a) {
        elem = _get_entry(a, struct openhandles_elem, avl);
        if ((--elem->register_count) == 0) {
            mutex_unlock(&bgf_lock);
            elem->file = NULL;
            while(elem->dbhandle != NULL) {usleep(1);}
            free(elem);
            mutex_lock(&bgf_lock);
        }
    }

    mutex_unlock(&bgf_lock);
}
