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

#include "filemgr.h"
#include "filemgr_ops.h"
#include "blockcache.h"
#include "btreeblock.h"
#include "btree.h"
#include "btree_kv.h"
#include "test_kvssd.h"

#include "memleak.h"

void basic_test()
{
     TEST_INIT();
     _reset_kvssd("/dev/nvme0n1", "forestKV");

     int ksize = 8;
     int vsize = 8;
     int blocksize = sizeof(struct bnode) + (ksize + vsize) * 4 + 1;
     struct filemgr *file;
     struct btreeblk_handle btree_handle;
     struct btree btree;
     struct filemgr_config config;
     int r;
     char *fname = (char *) "./btreeblock_testfile";

     r = system(SHELL_DEL" btreeblock_testfile");
     (void) r;
     memset(&config, 0, sizeof(config));
     config.kvssd = true;
     config.logging = true;
     config.kv_cache_size = 4096;
     config.kv_cache_doc_writes = true;
     config.blocksize = blocksize;

     /*
              40B       ...         1B
         [struct bnode][data][BLK_MARKER_BNODE]
     */

     config.index_blocksize = blocksize;
     config.kvssd_max_value_size = 2097152;
     config.ncacheblock = 1;
     config.flag = 0x0;
     config.options = FILEMGR_CREATE;
     config.num_wal_shards = 8;
     /* Config for logging */
     config.max_log_threshold = 5;
     config.split_threshold = config.index_blocksize;
     config.kvssd_emu_config_file =
     (char*)"/home/ubuntuvm/KVSSD/PDK/core/kvssd_emul.conf";

     r = system(SHELL_DEL" btreeblock_testfile");
     (void) r;
     filemgr_open_result result = filemgr_open(fname, get_filemgr_ops(), &config, NULL);
     file = result.file;
     btreeblk_init(&btree_handle, file, config.index_blocksize);

     btree_init(&btree, (void *) &btree_handle, btreeblk_get_ops(),
                btree_kv_get_ku64_vu64(), config.index_blocksize, ksize, vsize, 0x0, NULL);

     /*
         Insert "00000001", gets put in the in-memory block allocated
         to the root node (bid 0). Assert that the node's log is empty.
         This insert will write directly to the node as it already
         existed in memory.
     */

     btree_insert(&btree, (void*) "00000001", (void *) "VAL00001");
     TEST_CHK(file->bnode_logs->at(0).update_num == 0);
     TEST_CHK(file->bnode_logs->at(0).key_in_node == 0);


     /*
         Insert "00000002" to "00000004". Should all write directly
         to in-memory node for bid 0 and not to log. Node is now full.
         Assert log is still empty.
     */

     btree_insert(&btree, (void*) "00000002", (void*) "VAL00002");
     btree_insert(&btree, (void*) "00000003", (void*) "VAL00003");
     btree_insert(&btree, (void*) "00000004", (void*) "VAL00004");
     TEST_CHK(file->bnode_logs->at(0).update_num == 0);
     TEST_CHK(file->bnode_logs->at(0).key_in_node == 0);

     /*
         Insert "00000005" to "00000008" to fill a new node.
         Normal b+tree behaviour until now.
     */

     btree_insert(&btree, (void*) "00000005", (void*) "VAL00005");
     btree_insert(&btree, (void*) "00000006", (void*) "VAL00006");
     btree_insert(&btree, (void*) "00000007", (void*) "VAL00007");
     btree_insert(&btree, (void*) "00000008", (void*) "VAL00008");

     /*
         ["00000001"]["00000002"] BID 0
     */

     TEST_CHK(file->bnode_logs->at(0).update_num == 0);
     TEST_CHK(file->bnode_logs->at(0).key_in_node == 0);

     /*
         ["00000003"]["00000004"] BID 1
     */

     TEST_CHK(file->bnode_logs->at(1).update_num == 0);
     TEST_CHK(file->bnode_logs->at(1).key_in_node == 0);

     /*
         ["00000001"]["00000003"]["00000005"] BID 2
     */

     TEST_CHK(file->bnode_logs->at(2).update_num == 0);
     TEST_CHK(file->bnode_logs->at(2).key_in_node == 0);

     /*
         ["00000005"]["00000006"]["00000007"]["00000008"] BID 3
     */

     TEST_CHK(file->bnode_logs->at(3).update_num == 0);
     TEST_CHK(file->bnode_logs->at(3).key_in_node == 0);

     btreeblk_end(&btree_handle);

     /*
         Overwrite "00000001". Its block is cached in the read list
         right now, but we append the update to the log instead of
         writing directly to the node to avoid a leaf-to-root
         modification of the tree.
     */

     btree_insert(&btree, (void*) "00000001", (void *) "VALV2001");
     TEST_CHK(!bcache_read(file, 0, NULL));
     TEST_CHK(file->bnode_logs->at(0).update_num == 1);
     TEST_CHK(!bcache_read(file, 0, NULL));

     /*
         And again. Log should still grow.
     */

     btree_insert(&btree, (void*) "00000001", (void *) "VALV3001");
     TEST_CHK(!bcache_read(file, 0, NULL));
     TEST_CHK(file->bnode_logs->at(0).update_num == 2);
     TEST_CHK(!bcache_read(file, 0, NULL));

     /*
         Flush BID 0 to storage. Log should still be size 2 as bnode_logs
         refers to log size in mem and storage.
     */

     btreeblk_end(&btree_handle);
     TEST_CHK(file->bnode_logs->at(0).update_num == 2);

     /*
         Overwrite "00000002" to "00000004". Node has been previously
         split so updates should go to different logs.
     */

     btree_insert(&btree, (void*) "00000002", (void*) "VALV2002");
     btree_insert(&btree, (void*) "00000003", (void*) "VALV2003");
     btree_insert(&btree, (void*) "00000004", (void*) "VALV2004");
     TEST_CHK(file->bnode_logs->at(0).update_num == 3);
     TEST_CHK(file->bnode_logs->at(1).update_num == 2);

     /*
         BID 3 isn't cached right now. Updates should go to the log.
     */

     TEST_CHK(!bcache_read(file, 3, NULL));
     btree_insert(&btree, (void*) "00000005", (void*) "VALV2005");
     btree_insert(&btree, (void*) "00000006", (void*) "VALV2006");
     btree_insert(&btree, (void*) "00000007", (void*) "VALV2007");
     btree_insert(&btree, (void*) "00000008", (void*) "VALV2008");
     TEST_CHK(file->bnode_logs->at(3).update_num == 4);

     /*
         The max log size is 5. Log three more entries in bid 0 then
         repack. Should be done automatically when calling from fdb_commit.
     */

     btree_insert(&btree, (void*) "00000001", (void *) "VALV4001");
     btree_insert(&btree, (void*) "00000001", (void *) "VALV5001");
     btree_insert(&btree, (void*) "00000001", (void *) "VALV6001");

     for (auto it = file->exceed_bnode_logs->begin(); it != file->exceed_bnode_logs->end(); it++) {
         btree_repack(&btree, (void *) &it->second);
         btreeblk_end(&btree_handle);
     }

     btreeblk_end(&btree_handle);
     file->header.revnum++;
     filemgr_set_vernum(file, file->header.revnum << 32);
     filemgr_commit(file, true, NULL);

     /*
         BID 0's log should be empty after the repack, as it was the only
         one exceeding the size limit.
     */

     TEST_CHK(file->bnode_logs->find(0) == file->bnode_logs->end()); // 0 doesn't exist now
     TEST_CHK(file->bnode_logs->at(1).update_num == 2);
     TEST_CHK(file->bnode_logs->at(3).update_num == 4);

     btreeblk_free(&btree_handle);

     filemgr_close(file, true, NULL, NULL);
     filemgr_shutdown();

     TEST_RESULT("functionality test");
}

void btree_reverse_iterator_test()
{
    TEST_INIT();
    _reset_kvssd("/dev/nvme0n1", "forestKV");

    int ksize = 8, vsize = 8, r, c;
    int nodesize = 256;
    struct filemgr *file;
    struct btreeblk_handle bhandle;
    struct btree btree;
    struct btree_iterator bi;
    struct filemgr_config config;
    struct btree_kv_ops *kv_ops;
    btree_result br;
    filemgr_open_result fr;
    uint64_t i;
    uint64_t k,v;
    char *fname = (char *) "./btreeblock_testfile";

    r = system(SHELL_DEL" btreeblock_testfile");
    (void)r;

    memleak_start();

    memset(&config, 0, sizeof(config));
    config.kvssd = true;
    config.kv_cache_size = 0;
    config.kvssd_max_value_size = 4096;
    config.kv_cache_doc_writes = true;
    config.kv_cache_doc_writes = false;
    config.ncacheblock = 0;
    config.blocksize = nodesize;
    config.options = FILEMGR_CREATE;
    config.num_wal_shards = 8;
    fr = filemgr_open(fname, get_filemgr_ops(), &config, NULL);
    file = fr.file;

    btreeblk_init(&bhandle, file, nodesize);
    kv_ops = btree_kv_get_kb64_vb64(NULL);
    btree_init(&btree, (void*)&bhandle,
               btreeblk_get_ops(), kv_ops,
               nodesize, ksize, vsize, 0x0, NULL);

    for (i=10;i<40;++i) {
        k = _endian_encode(i*0x10);
        v = _endian_encode(i*0x100);
        btree_insert(&btree, (void*)&k, (void*)&v);
        btreeblk_end(&bhandle);
    }

    c = 0;
    btree_iterator_init(&btree, &bi, NULL);
    while ((br=btree_next(&bi, &k, &v)) == BTREE_RESULT_SUCCESS) {
        btreeblk_end(&bhandle);
        k = _endian_decode(k);
        v = _endian_decode(v);
        TEST_CHK(k == (uint64_t)(c+10)*0x10);
        TEST_CHK(v == (uint64_t)(c+10)*0x100);
        c++;
    }
    btreeblk_end(&bhandle);
    btree_iterator_free(&bi);
    TEST_CHK(c == 30);

    c = 0;
    i=10000;
    k = _endian_encode(i);
    btree_iterator_init(&btree, &bi, &k);
    while ((br=btree_next(&bi, &k, &v)) == BTREE_RESULT_SUCCESS) {
        btreeblk_end(&bhandle);
        k = _endian_decode(k);
        v = _endian_decode(v);
    }
    btreeblk_end(&bhandle);
    btree_iterator_free(&bi);
    TEST_CHK(c == 0);

    // reverse iteration with NULL initial key
    c = 0;
    btree_iterator_init(&btree, &bi, NULL);
    while ((br=btree_prev(&bi, &k, &v)) == BTREE_RESULT_SUCCESS) {
        btreeblk_end(&bhandle);
        k = _endian_decode(k);
        v = _endian_decode(v);
    }
    btreeblk_end(&bhandle);
    btree_iterator_free(&bi);
    TEST_CHK(c == 0);

    c = 0;
    i=10000;
    k = _endian_encode(i);
    btree_iterator_init(&btree, &bi, &k);
    while ((br=btree_prev(&bi, &k, &v)) == BTREE_RESULT_SUCCESS) {
        btreeblk_end(&bhandle);
        k = _endian_decode(k);
        v = _endian_decode(v);
        TEST_CHK(k == (uint64_t)(39-c)*0x10);
        TEST_CHK(v == (uint64_t)(39-c)*0x100);
        c++;
    }
    btreeblk_end(&bhandle);
    btree_iterator_free(&bi);
    TEST_CHK(c == 30);

    c = 0;
    i=0x175;
    k = _endian_encode(i);
    btree_iterator_init(&btree, &bi, &k);
    while ((br=btree_prev(&bi, &k, &v)) == BTREE_RESULT_SUCCESS) {
        btreeblk_end(&bhandle);
        k = _endian_decode(k);
        v = _endian_decode(v);
        TEST_CHK(k == (uint64_t)(0x17-c)*0x10);
        TEST_CHK(v == (uint64_t)(0x17-c)*0x100);
        c++;
    }
    btreeblk_end(&bhandle);
    btree_iterator_free(&bi);
    TEST_CHK(c == 14);

    c = 0xa0 - 0x10;
    btree_iterator_init(&btree, &bi, NULL);
    for (i=0;i<15;++i){
        c += 0x10;
        br = btree_next(&bi, &k, &v);
        TEST_CHK(br == BTREE_RESULT_SUCCESS);
        btreeblk_end(&bhandle);
        k = _endian_decode(k);
        v = _endian_decode(v);
        TEST_CHK(k == (uint64_t)c);
        TEST_CHK(v == (uint64_t)c*0x10);
    }
    for (i=0;i<7;++i){
        c -= 0x10;
        br = btree_prev(&bi, &k, &v);
        TEST_CHK(br == BTREE_RESULT_SUCCESS);
        btreeblk_end(&bhandle);
        k = _endian_decode(k);
        v = _endian_decode(v);
        TEST_CHK(k == (uint64_t)c);
        TEST_CHK(v == (uint64_t)c*0x10);
    }
    for (i=0;i<10;++i){
        c += 0x10;
        br = btree_next(&bi, &k, &v);
        TEST_CHK(br == BTREE_RESULT_SUCCESS);
        btreeblk_end(&bhandle);
        k = _endian_decode(k);
        v = _endian_decode(v);
        TEST_CHK(k == (uint64_t)c);
        TEST_CHK(v == (uint64_t)c*0x10);
    }
    for (i=0;i<17;++i){
        c -= 0x10;
        br = btree_prev(&bi, &k, &v);
        TEST_CHK(br == BTREE_RESULT_SUCCESS);
        btreeblk_end(&bhandle);
        k = _endian_decode(k);
        v = _endian_decode(v);
        TEST_CHK(k == (uint64_t)c);
        TEST_CHK(v == (uint64_t)c*0x10);
    }
    br = btree_prev(&bi, &k, &v);
    btreeblk_end(&bhandle);
    TEST_CHK(br == BTREE_RESULT_FAIL);

    btree_iterator_free(&bi);

    free(kv_ops);
    btreeblk_free(&bhandle);
    filemgr_close(file, true, NULL, NULL);
    filemgr_shutdown();

    memleak_end();

    TEST_RESULT("btree reverse iterator test");
}

/*
    Insert then 50/50 RW workload using the async 
    btree functions
*/

atomic_uint64_t count;

void cb_fail(struct async_read_ctx_t *ctx)
{
    assert(ctx->btreeres == BTREE_RESULT_FAIL);
    free(ctx->key);
    free(ctx->value_buf);
    // free(ctx);
    atomic_incr_uint64_t(&count);
}

void cb_pass(struct async_read_ctx_t *ctx)
{
    assert(ctx->btreeres == BTREE_RESULT_SUCCESS);
    assert(*(uint64_t*)ctx->value_buf == (uint64_t) ctx->doclen * 10);
    free(ctx->key);
    free(ctx->value_buf);
    // free(ctx);
    atomic_incr_uint64_t(&count);
}

void async_test()
{
    TEST_INIT();
    _reset_kvssd("/dev/nvme0n1", "forestKV");

    int n = 100000;
    int run = 100000;
    int ksize = 8;
    int vsize = 8;
    int nodesize = 4096;
    int blocksize = nodesize;
    struct filemgr *file;
    struct btreeblk_handle btree_handle;
    struct btree btree;
    struct filemgr_config config;
    int i, r, op, read_count, write_count;
    uint64_t k,v;
    uint64_t *find_k;
    void* find_v;
    struct async_read_ctx_t *find_ctx;
    char *fname = (char *) "./btreeblock_testfile";
    btree_result btreeres;

    memleak_start();

    atomic_init_uint64_t(&count, 0);

    r = system(SHELL_DEL" btreeblock_testfile");
    (void)r;
    memset(&config, 0, sizeof(config));
    config.kvssd = true;
    config.logging = true;
    config.kv_cache_size = 4*1024*1024; // doesn't work without caches right now
    config.kvssd_max_value_size = 4096;
    config.kvssd_emu_config_file = 
    (char*)"/home/ubuntuvm/KVSSD/PDK/core/kvssd_emul.conf";
    config.kv_cache_doc_writes = false;
    config.blocksize = blocksize;
    config.index_blocksize = config.blocksize;
    config.max_log_threshold = ((config.blocksize - sizeof(struct bnode))/16)*1;
    config.split_threshold = config.blocksize;
    config.ncacheblock = 1024; // doesn't work without caches right now
    config.flag = 0x0;
    config.options = FILEMGR_CREATE;
    config.num_wal_shards = 8;
    r = system(SHELL_DEL" btreeblock_testfile");
    (void)r;
    filemgr_open_result result = filemgr_open(fname, get_filemgr_ops(), &config, NULL);
    file = result.file;
    btreeblk_init(&btree_handle, file, nodesize);

    btree_init(&btree, (void*)&btree_handle, btreeblk_get_ops(),
               btree_kv_get_ku64_vu64(), nodesize, ksize, vsize, 0x0, NULL);

    for (i = 0; i < n; i++) {
        k = i; v = i * 10;
        btreeres = btree_insert(&btree, (void*)&k, (void*)&v);
        assert(btreeres == BTREE_RESULT_SUCCESS);
        btreeblk_end(&btree_handle);

        /*
            Flush the blocks out of the read and alloc lists
            so everything next won't just be read from them
        */

        if(i % 50 == 0) {
            filemgr_commit(file, true, NULL);
            file->header.revnum++;
            filemgr_set_vernum(file, file->header.revnum << 32);
        }

        if(i % 1000 == 0) {
            printf("Finished %d/%d inserts\n", i, n);
        }
    }

    btreeblk_end(&btree_handle);
    filemgr_commit(file, true, NULL);
    file->header.revnum++;
    filemgr_set_vernum(file, file->header.revnum << 32);

    write_count = 0;
    read_count = 0;

    for (i = 0; i < run; i++) {
        op = rand() % 100;

        k = rand() % n;

        if(op > 50) {
            v = k * 10;
            btreeres = btree_insert(&btree, (void*)&k, (void*)&v);
            assert(btreeres == BTREE_RESULT_UPDATE || btreeres == BTREE_RESULT_SUCCESS);
            btreeblk_end(&btree_handle);
            bcache_flush(file);
            write_count++;

            if(write_count % 50 == 0) {
                filemgr_commit(file, true, NULL);
                file->header.revnum++;
                filemgr_set_vernum(file, file->header.revnum << 32);
            }
        } else {
            find_ctx = (struct async_read_ctx_t*)malloc(sizeof(*find_ctx));
            find_k = (uint64_t*)malloc(sizeof(*find_k));
            find_v = malloc(sizeof(uint64_t));

            memset(find_v, 0x0, sizeof(uint64_t));
            memset(find_ctx, 0x0, sizeof(*find_ctx));
            find_ctx->bhandle = &btree_handle;

            /*  
                Not using the trie here, but just setting this
                so it's called after the btree op completes  
            */

            find_ctx->hbtrie_find_cb = cb_pass;
            find_ctx->value_buf = find_v;

            /*
                Doclen is only used when searching from fdb_get.
                Use it here to indicate what key we were searching
                for when in the cb.
            */

            find_ctx->doclen = k;

            find_ctx->file = file;
            mutex_init(&find_ctx->lock);

            *find_k = k;

            btreeres = btree_find_async(&btree, find_k, find_v, find_ctx);
            assert(btreeres == BTREE_RESULT_SUCCESS);
            btreeblk_end(&btree_handle);
            read_count++;
        }

        if(i % 1000 == 0) {
            printf("Finished %d/%d ops\n", i, run);
        }
    }

    while(atomic_get_uint64_t(&count) < (uint64_t) read_count) {}

    /*
        Find non existent key
    */

    find_ctx = (struct async_read_ctx_t*)malloc(sizeof(*find_ctx));
    find_k = (uint64_t*)malloc(sizeof(*find_k));
    find_v = malloc(sizeof(uint64_t));

    memset(find_v, 0x0, sizeof(uint64_t));
    memset(find_ctx, 0x0, sizeof(*find_ctx));
    find_ctx->bhandle = &btree_handle;
    find_ctx->hbtrie_find_cb = cb_fail;
    find_ctx->value_buf = find_v;
    find_ctx->doclen = n + 1;
    find_ctx->file = file;
    mutex_init(&find_ctx->lock);

    *find_k = n + 1;

    btreeres = btree_find_async(&btree, find_k, find_v, find_ctx);

    while(atomic_get_uint64_t(&count) < (uint64_t) read_count + 1) {}

    btreeblk_free(&btree_handle);
    filemgr_close(file, true, NULL, NULL);
    filemgr_shutdown();

    free(find_ctx);

    memleak_end();

    TEST_RESULT("async btree test");
}

void update_test()
{
    TEST_INIT();
    _reset_kvssd("/dev/nvme0n1", "forestKV");

    int n = 50000000;
    uint32_t commit = 65536;
    int ksize = 8;
    int vsize = 8;
    int nodesize = 4096;
    int blocksize = nodesize;
    struct filemgr *file;
    struct btreeblk_handle btree_handle;
    struct btree btree;
    struct filemgr_config config;
    int i, r;
    uint64_t k,v;
    void* find_v;
    char *fname = (char *) "./btreeblock_testfile";
    btree_result btreeres;

    memleak_start();

    atomic_init_uint64_t(&count, 0);

    r = system(SHELL_DEL" btreeblock_testfile");
    (void)r;
    memset(&config, 0, sizeof(config));
    config.kvssd = true;
    config.logging = true;
    config.kv_cache_size = 1024LU*1024*1024; // doesn't work without caches right now
    config.kvssd_max_value_size = 4096;
    config.kvssd_emu_config_file = 
    (char*)"/home/ubuntuvm/KVSSD/PDK/core/kvssd_emul.conf";
    config.kv_cache_doc_writes = false;
    config.blocksize = blocksize;
    config.index_blocksize = config.blocksize;
    config.max_log_threshold = ((config.blocksize - sizeof(struct bnode))/16)*1;
    config.split_threshold = config.blocksize;
    config.ncacheblock = (8192LU*1024*1024) / config.blocksize; // doesn't work without caches right now
    config.flag = 0x0;
    config.options = FILEMGR_CREATE;
    config.num_wal_shards = 8;
    r = system(SHELL_DEL" btreeblock_testfile");
    (void)r;
    filemgr_open_result result = filemgr_open(fname, get_filemgr_ops(), &config, NULL);
    file = result.file;
    btreeblk_init(&btree_handle, file, nodesize);

    btree_init(&btree, (void*)&btree_handle, btreeblk_get_ops(),
               btree_kv_get_ku64_vu64(), nodesize, ksize, vsize, 0x0, NULL);

    /*
        Insert n keys
    */

    for (i = 0; i < n; i++) {
        k = i; v = i * 10;
        btreeres = btree_insert(&btree, (void*)&k, (void*)&v);
        assert(btreeres == BTREE_RESULT_SUCCESS);
        btreeblk_end(&btree_handle);

        /*
            Flush the blocks out of the read and alloc lists
            so everything next won't just be read from them
        */

        if(i % commit == 0) {
            filemgr_commit(file, true, NULL);
            file->header.revnum++;
            filemgr_set_vernum(file, file->header.revnum << 32);
        }

        if(i % 10000 == 0) {
            printf("Finished %d/%d inserts\n", i, n);
            fdb_print_stats();
        }
    }

    btreeblk_end(&btree_handle);
    filemgr_commit(file, true, NULL);
    file->header.revnum++;
    filemgr_set_vernum(file, file->header.revnum << 32);

    /*
        Update n keys
    */

    for (i = 0; i < n; i++) {
        k = i; v = i * 10;
        btreeres = btree_insert(&btree, (void*)&k, (void*)&v);
        assert(btreeres == BTREE_RESULT_UPDATE || 
               btreeres == BTREE_RESULT_SUCCESS);
        btreeblk_end(&btree_handle);

        /*
            Flush the blocks out of the read and alloc lists
            so everything next won't just be read from them
        */

        if(i % commit == 0) {
            filemgr_commit(file, true, NULL);
            file->header.revnum++;
            filemgr_set_vernum(file, file->header.revnum << 32);
        }

        if(i % 10000 == 0) {
            printf("Finished %d/%d updates\n", i, n);
        }
    }
    btreeblk_end(&btree_handle);
    filemgr_commit(file, true, NULL);
    file->header.revnum++;
    filemgr_set_vernum(file, file->header.revnum << 32);


    /*
        Read n keys
    */

    find_v = malloc(sizeof(uint64_t));

    for (i = 0; i < n; i++) {
        k = rand() % n;

        btreeres = btree_find(&btree, &k, find_v);
        assert(btreeres == BTREE_RESULT_SUCCESS);
        btreeblk_end(&btree_handle);

        if(i % 1000 == 0) {
            printf("Finished %d/%d reads\n", i, n);
        }
    }

    btreeblk_free(&btree_handle);
    filemgr_close(file, true, NULL, NULL);
    filemgr_shutdown();

    memleak_end();

    TEST_RESULT("insert update read");   
}

int main()
{
#ifdef _MEMPOOL
    mempool_init();
#endif

    // basic_test();
    // iterator_test();
    // two_btree_test();
    // range_test();
    // btree_reverse_iterator_test();
    //async_test();
    update_test();
    _reset_kvssd("/dev/nvme0n1", "forestKV");

    return 0;
}
