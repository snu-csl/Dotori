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

#include <stdint.h>
#include <string.h>

#include "configuration.h"
#include "system_resource_stats.h"

static ssize_t prime_size_table[] = {
    11, 31, 47, 73, 97, 109, 211, 313, 419, -1
};

fdb_config get_default_config(void) {
    fdb_config fconfig;

    fconfig.chunksize = sizeof(uint64_t);
    // 4KB by default.
    fconfig.blocksize = FDB_BLOCKSIZE;
    // 128MB by default.
    fconfig.buffercache_size = 134217728;
    // 4K WAL entries by default.
    fconfig.wal_threshold = 4096;
    fconfig.max_log_threshold = 10;
    fconfig.split_threshold = FDB_BLOCKSIZE;
    fconfig.wal_flush_before_commit = true;
    fconfig.auto_commit = false;
    // 0 second by default.
    fconfig.purging_interval = 0;
    // Sequnce trees are disabled by default.
    fconfig.seqtree_opt = FDB_SEQTREE_NOT_USE;
    // Use a synchronous commit by default.
    fconfig.durability_opt = FDB_DRB_NONE;
    fconfig.flags = FDB_OPEN_FLAG_CREATE;
    // 4MB by default.
    fconfig.compaction_buf_maxsize = FDB_COMP_BUF_MINSIZE;
    // Clean up cache entries when a file is closed.
    fconfig.cleanup_cache_onclose = true;
    // Compress the body of documents using snappy. Disabled by default.
    fconfig.compress_document_body = false;
    // Auto compaction is disabled by default
    fconfig.compaction_mode = FDB_COMPACTION_MANUAL;
    // Compaction threshold, 30% by default
    fconfig.compaction_threshold = FDB_DEFAULT_COMPACTION_THRESHOLD;
    fconfig.compaction_minimum_filesize = 1048576; // 1MB by default
    // 8 hours by default
    fconfig.compactor_sleep_duration = FDB_COMPACTOR_SLEEP_DURATION;
    // Multi KV Instance mode is enabled by default
    fconfig.multi_kv_instances = true;
    // TODO: Re-enable this after prefetch ThreadSanitizer fixes are in..
    fconfig.prefetch_duration = 0;

    // Determine the number of WAL and buffer cache partitions by considering the
    // number of cores available in the host environment.
    int i = 0;
    ssize_t num_cores = (ssize_t) get_num_cores();
    for (; prime_size_table[i] > 0 && prime_size_table[i] < num_cores; ++i) {
        // Finding the smallest prime number that is greater than the number of cores.
    }
    if (prime_size_table[i] == -1) {
        fconfig.num_wal_partitions = prime_size_table[i-1];
        fconfig.num_bcache_partitions = prime_size_table[i-1];
    } else {
        fconfig.num_wal_partitions = prime_size_table[i];
        // For bcache partitions pick a higher value for smaller avl trees
        fconfig.num_bcache_partitions = prime_size_table[i];
    }

    // No compaction callback function by default
    fconfig.compaction_cb = NULL;
    fconfig.compaction_cb_mask = 0x0;
    fconfig.compaction_cb_ctx = NULL;
    fconfig.max_writer_lock_prob = 100;
    // 4 daemon compactor threads by default
    fconfig.num_compactor_threads = DEFAULT_NUM_COMPACTOR_THREADS;
    fconfig.enable_background_compactor = true;
    fconfig.num_bgflusher_threads = DEFAULT_NUM_BGFLUSHER_THREADS;
    // Block reusing threshold, 65% by default (i.e., almost 3x space amplification)
    fconfig.block_reusing_threshold = 65;
    // Trigger block reuse after 16MB.
    fconfig.min_block_reuse_filesize = SB_MIN_BLOCK_REUSING_FILESIZE;
    // Unlimited cycle.
    fconfig.max_block_reusing_cycle = 0;
    // Keep at most 5 recent committed database snapshots
    fconfig.num_keeping_headers = 5;

    fconfig.encryption_key.algorithm = FDB_ENCRYPTION_NONE;
    memset(fconfig.encryption_key.bytes, 0, sizeof(fconfig.encryption_key.bytes));

    // Breakpad minidump directory, set to current working dir
    fconfig.breakpad_minidump_dir = ".";

    // Default log level: FATAL (1)
    fconfig.log_msg_level = 1;

    // Auto move to new file.
    fconfig.do_not_move_to_compacted_file = false;

    // Disable reserved blocks.
    fconfig.enable_reusable_block_reservation = false;

    // Disable bulk load mode.
    fconfig.bulk_load_mode = false;

    // WAL data should be visiable by default.
    fconfig.do_not_search_wal = false;

    // Document blocks are cached by default.
    fconfig.do_not_cache_doc_blocks = false;
    
    // Block SSD by default
    fconfig.kvssd = false;
	fconfig.logging = false;
	fconfig.max_log_threshold = -1;
	fconfig.split_threshold = FDB_BLOCKSIZE;
    
    fconfig.kvssd_emu_config_file = NULL;
    fconfig.kvssd_dev_path = (char*) "/dev/kvemul";;

    // Use the block cache by default.
    fconfig.kv_cache_size = 0;
    fconfig.kv_cache_doc_writes = false;
    fconfig.kvssd_max_value_size = 0;
    fconfig.background_wal_preload = false;
    fconfig.wal_flush_preloading = false;
    fconfig.max_logs_per_node = 5;
    fconfig.num_aio_workers = 1;

    return fconfig;
}

fdb_config get_default_config_kvssd(void) {
    fdb_config fconfig;

    fconfig.chunksize = sizeof(uint64_t);
    // 4KB by default.
    fconfig.blocksize = FDB_BLOCKSIZE;
    fconfig.index_blocksize = FDB_BLOCKSIZE;
    fconfig.split_threshold = FDB_BLOCKSIZE;
    // 128MB by default.
    fconfig.buffercache_size = (128 * 1024 * 1024) * .2;
    // 4K WAL entries by default.
    fconfig.wal_threshold = 4096;
    fconfig.wal_flush_before_commit = false;
    fconfig.auto_commit = false;
    // 0 second by default.
    fconfig.purging_interval = 0;
    // Sequnce trees are disabled by default.
    fconfig.seqtree_opt = FDB_SEQTREE_NOT_USE;
    // Use a synchronous commit by default.
    fconfig.durability_opt = FDB_DRB_NONE;
    fconfig.flags = FDB_OPEN_FLAG_CREATE;
    // 4MB by default.
    fconfig.compaction_buf_maxsize = FDB_COMP_BUF_MINSIZE;
    // Clean up cache entries when a file is closed.
    fconfig.cleanup_cache_onclose = true;
    // Compress the body of documents using snappy. Disabled by default.
    fconfig.compress_document_body = false;
    // Auto compaction is disabled by default
    fconfig.compaction_mode = FDB_COMPACTION_MANUAL;
    // Compaction threshold, 30% by default
    fconfig.compaction_threshold = FDB_DEFAULT_COMPACTION_THRESHOLD;
    fconfig.compaction_minimum_filesize = 1048576; // 1MB by default
    // 8 hours by default
    fconfig.compactor_sleep_duration = FDB_COMPACTOR_SLEEP_DURATION;
    // Multi KV Instance mode is enabled by default
    fconfig.multi_kv_instances = false;
    // TODO: Re-enable this after prefetch ThreadSanitizer fixes are in..
    fconfig.prefetch_duration = 0;

    // Determine the number of WAL and buffer cache partitions by considering the
    // number of cores available in the host environment.
    int i = 0;
    ssize_t num_cores = (ssize_t) get_num_cores();
    for (; prime_size_table[i] > 0 && prime_size_table[i] < num_cores; ++i) {
        // Finding the smallest prime number that is greater than the number of cores.
    }
    if (prime_size_table[i] == -1) {
        fconfig.num_wal_partitions = prime_size_table[i-1];
        fconfig.num_bcache_partitions = prime_size_table[i-1];
    } else {
        fconfig.num_wal_partitions = prime_size_table[i];
        // For bcache partitions pick a higher value for smaller avl trees
        fconfig.num_bcache_partitions = prime_size_table[i];
    }

    // No compaction callback function by default
    fconfig.compaction_cb = NULL;
    fconfig.compaction_cb_mask = 0x0;
    fconfig.compaction_cb_ctx = NULL;
    fconfig.max_writer_lock_prob = 100;
    // 1 daemon compactor thread by default
    fconfig.num_compactor_threads = 1;
    fconfig.enable_background_compactor = true;
    fconfig.num_bgflusher_threads = 1;
    // Block reusing threshold, 0% by default as we don't use it on the KVSSD
    fconfig.block_reusing_threshold = 0;
    // Trigger block reuse after 16MB.
    fconfig.min_block_reuse_filesize = SB_MIN_BLOCK_REUSING_FILESIZE;
    // Unlimited cycle.
    fconfig.max_block_reusing_cycle = 0;
    // Keep at most 5 recent committed database snapshots
    fconfig.num_keeping_headers = 5;

    fconfig.encryption_key.algorithm = FDB_ENCRYPTION_NONE;
    memset(fconfig.encryption_key.bytes, 0, sizeof(fconfig.encryption_key.bytes));

    // Breakpad minidump directory, set to current working dir
    fconfig.breakpad_minidump_dir = ".";

    // Default log level: FATAL (1)
    fconfig.log_msg_level = 1;

    // Auto move to new file.
    fconfig.do_not_move_to_compacted_file = false;

    // Disable reserved blocks.
    fconfig.enable_reusable_block_reservation = false;

    // Disable bulk load mode.
    fconfig.bulk_load_mode = false;

    // WAL data should be visiable by default.
    fconfig.do_not_search_wal = false;

    // Document blocks are cached by default.
    fconfig.do_not_cache_doc_blocks = false;
    
    // KVSSD by default
    fconfig.kvssd = true;
    fconfig.max_log_threshold = 250;
    fconfig.split_threshold = FDB_BLOCKSIZE;

	// Logging by default
	fconfig.logging = true;

    fconfig.kvssd_emu_config_file = 
    (char*)"kvssd_emul.conf";

#ifdef __RAN_KVEMU_DEV
    /*
     * When running the tests, we don't have a good way right now to reset the
     * emulator after every test. Generate a random emulator device name here
     * every time so each test runs on a new device. The user can overwrite this
     * with their own device after this function returns, if necessary.
     */

    uint64_t rand_dev = rand();
    char* buf = (char*) malloc(32);
    memset(buf, 0x0, 32);
    sprintf(buf, "/dev/%lu", rand_dev);
    fconfig.kvssd_dev_path = buf;
#else
    fconfig.kvssd_dev_path = (char*) "/dev/kvemul";
#endif
    
    // Use the block cache by default.
    fconfig.kv_cache_size = (128 * 1024 * 1024) * .8;
    fconfig.kv_cache_doc_writes = true;
    fconfig.kvssd_max_value_size = 2097152;
    fconfig.background_wal_preload = false;
    fconfig.wal_flush_preloading = false;
    fconfig.delete_during_wal_flush = false;

    // trim the size of the OAK-Tree logs or not
    // value in MB, 0 means don't trim
    fconfig.log_trim_mb = 100;

    // threshold in seconds for cold OAK-Tree
    // log repacking, 0 means don't repack
    fconfig.cold_log_threshold = 60;

    // scan for old logs per N commits
    fconfig.cold_log_scan_interval = 1;

    // trim log after % of stale data
    fconfig.log_trim_threshold = 10;

    fconfig.write_index_on_close = true;

    fconfig.max_logs_per_node = 5;

    fconfig.num_aio_workers = 1;
    fconfig.max_outstanding = 64;

    // run deletion routine every N seconds
    fconfig.deletion_interval = 1;
    fconfig.async_wal_flushes = 0;
    fconfig.kvssd_retrieve_length = 4096;

    return fconfig;
}

fdb_kvs_config get_default_kvs_config(void) {
    fdb_kvs_config kvs_config;

    // create an empty KV store if it doesn't exist.
    kvs_config.create_if_missing = true;
    // lexicographical key order by default
    kvs_config.custom_cmp = NULL;
    kvs_config.custom_cmp_param = NULL;

    return kvs_config;
}

bool validate_fdb_config(fdb_config *fconfig) {
    assert(fconfig);

    if (fconfig->chunksize < 4 || fconfig->chunksize > 64) {
        // Chunk size should be set between 4 and 64 bytes.
        return false;
    }
    if (fconfig->chunksize < sizeof(void *)) {
        // Chunk size should be equal to or greater than the address bus size
        return false;
    }
    if (fconfig->blocksize < 1024 || fconfig->blocksize > 131072) {
        // Block size should be set between 1KB and 128KB
        return false;
    }
    if (fconfig->seqtree_opt != FDB_SEQTREE_NOT_USE &&
        fconfig->seqtree_opt != FDB_SEQTREE_USE) {
        return false;
    }
    if (fconfig->durability_opt != FDB_DRB_NONE &&
        fconfig->durability_opt != FDB_DRB_ODIRECT &&
        fconfig->durability_opt != FDB_DRB_ASYNC &&
        fconfig->durability_opt != FDB_DRB_ODIRECT_ASYNC) {
        return false;
    }
    if ((fconfig->flags & FDB_OPEN_FLAG_CREATE) &&
        (fconfig->flags & FDB_OPEN_FLAG_RDONLY)) {
        return false;
    }
    if (fconfig->compaction_threshold > 100) {
        // Compaction threshold should be equal or less then 100 (%).
        return false;
    }
    if (fconfig->compactor_sleep_duration == 0) {
        // Sleep duration should be larger than zero
        return false;
    }
    if (!fconfig->num_wal_partitions ||
        (fconfig->num_wal_partitions > MAX_NUM_WAL_PARTITIONS)) {
        return false;
    }
    if (!fconfig->num_bcache_partitions ||
        (fconfig->num_bcache_partitions > MAX_NUM_BCACHE_PARTITIONS)) {
        return false;
    }
    if (fconfig->max_writer_lock_prob < 20 ||
        fconfig->max_writer_lock_prob > 100) {
        return false;
    }
    if (fconfig->num_compactor_threads < 1 ||
        fconfig->num_compactor_threads > MAX_NUM_COMPACTOR_THREADS) {
        return false;
    }
    if (fconfig->num_bgflusher_threads > MAX_NUM_BGFLUSHER_THREADS) {
        return false;
    }
    if (fconfig->num_keeping_headers == 0) {
        // num_keeping_headers should be greater than zero
        return false;
    }
    if (fconfig->log_msg_level > 6) {
        // Log level: 0 to 6.
        return false;
    }

    return true;
}

bool validate_fdb_kvs_config(fdb_kvs_config *kvs_config) {
    return true;
}

