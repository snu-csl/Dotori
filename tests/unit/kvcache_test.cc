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

#include "test.h"
#include "kv_data_cache.h"
#include "filemgr.h"
#include "filemgr_ops.h"
#include "crc32.h"
#include "ycsb_utils.h"

#include "memleak.h"

#undef THREAD_SANITIZER
#if __clang__
#   if defined(__has_feature) && __has_feature(thread_sanitizer)
#define THREAD_SANITIZER
#   endif
#endif

void basic_test()
{
//    TEST_INIT();
//
//    struct filemgr *file;
//    struct filemgr_config config;
//    int i;
//    uint8_t buf[4096];
//    char *fname = (char *) "./bcache_testfile";
//
//    memset(&config, 0, sizeof(config));
//    config.blocksize = 4096;
//    config.kv_data_cache = true;
//    config.kv_cache_size = 32 * 1024 * 1024;
//    config.options = FILEMGR_CREATE;
//    config.num_wal_shards = 8;
//    filemgr_open_result result = filemgr_open(fname, get_filemgr_ops(), &config, NULL);
//    file = result.file;
//
//    for (i=0;i<5;++i) {
//        filemgr_alloc(file, NULL);
//        filemgr_write(file, i, buf, NULL);
//    }
//    filemgr_commit(file, true, NULL);
//    for (i=5;i<10;++i) {
//        filemgr_alloc(file, NULL);
//        filemgr_write(file, i, buf, NULL);
//    }
//    filemgr_commit(file, true, NULL);
//
//    filemgr_read(file, 8, buf, NULL, true);
//    filemgr_read(file, 9, buf, NULL, true);
//
//    filemgr_read(file, 1, buf, NULL, true);
//    filemgr_read(file, 2, buf, NULL, true);
//    filemgr_read(file, 3, buf, NULL, true);
//
//    filemgr_read(file, 7, buf, NULL, true);
//    filemgr_read(file, 1, buf, NULL, true);
//    filemgr_read(file, 9, buf, NULL, true);
//
//    filemgr_alloc(file, NULL);
//    filemgr_write(file, 10, buf, NULL);
//
//    TEST_RESULT("basic test");
}

void basic_test2()
{
    TEST_INIT();

    struct filemgr *file;
    struct filemgr_config config;
    int i;
    uint8_t buf[4096];
    char *fname = (char *) "./bcache_testfile";
    int r;
    r = system(SHELL_DEL " bcache_testfile");
    (void)r;

    memset(&config, 0, sizeof(config));
    config.blocksize = 4096;
    config.kvssd = true;
    config.kv_data_cache = true;
    config.kv_cache_size = 32 * 1024 * 1024;
    config.flag = 0x0;
    config.options = FILEMGR_CREATE;
    config.num_wal_shards = 8;
    filemgr_open_result result = filemgr_open(fname, get_filemgr_ops(), &config, NULL);
    file = result.file;

    for (i=0;i<5;++i) {
        kvcache_write(file, i, buf, 4096, KVCACHE_REQ_DIRTY);
    }
    for (i=5;i<10;++i) {
        kvcache_write(file, i, buf, 4096, KVCACHE_REQ_DIRTY);
    }
    filemgr_commit(file, true, NULL);
    filemgr_close(file, true, NULL, NULL);
    filemgr_shutdown();

    TEST_RESULT("basic test");

}

struct worker_args {
    size_t n;
    struct filemgr *file;
    size_t writer;
    size_t nblocks;
    size_t time_sec;
    size_t ops;
};

void * worker(void *voidargs)
{
    uint8_t *buf = (uint8_t *)malloc(4096);
    struct worker_args *args = (struct worker_args*)voidargs;
    struct timeval ts_begin, ts_cur, ts_gap;

    ssize_t ret;
    bid_t bid;
    uint32_t crc, crc_file;
    uint64_t i, c, run_count=0;
    uint64_t hits = 0, misses = 0;
    TEST_INIT();

    memset(buf, 0, 4096);
    gettimeofday(&ts_begin, NULL);

    while(1) {
        bid = rand() % args->nblocks;
        ret = kvcache_read(args->file, bid, buf);
        if (ret <= 0) {
            misses++;
            if(!args->file->config->kvssd) {
                ret = args->file->ops->pread(args->file->fd, buf,
                                             args->file->blocksize, bid * args->file->blocksize);
            } else {
                char* keyBuf;
                uint8_t keysize;
                union {
                    int i;
                    unsigned char c[4];
                } u;
                u.i = bid;

                keysize = sizeof(bid);
                keyBuf = (char *) malloc(keysize);
                memset(keyBuf, 0x0, keysize);
                for (int i = 0; i < PREFIX_LEN; i++) {
                    keyBuf[i] = u.c[(PREFIX_LEN - 1) - i];
                }
                
                ret = args->file->kvssd->getKVTupleNoPrefix(keyBuf, sizeof(uint64_t), buf);
            }
            TEST_CHK(ret == (ssize_t)args->file->blocksize);
            ret = kvcache_write(args->file, bid, buf, args->file->blocksize, KVCACHE_REQ_CLEAN);
            TEST_CHK(ret == args->file->blocksize);
        } else {
            hits++;
        }
        crc_file = crc32_8(buf, sizeof(uint64_t)*2, 0);
        (void)crc_file;
        memcpy(&i, buf, sizeof(i));
        memcpy(&crc, buf + sizeof(uint64_t)*2, sizeof(crc));
        // Disable checking the CRC value at this time as pread and pwrite are
        // not thread-safe.
        // TEST_CHK(crc == crc_file && i==bid);
        //DBG("%d %d %d %x %x\n", (int)args->n, (int)i, (int)bid, (int)crc, (int)crc_file);

        if (args->writer) {
            memcpy(&c, buf+sizeof(i), sizeof(c));
            c++;
            memcpy(buf+sizeof(i), &c, sizeof(c));
            crc = crc32_8(buf, sizeof(uint64_t)*2, 0);
            memcpy(buf + sizeof(uint64_t)*2, &crc, sizeof(crc));
            ret = kvcache_write(args->file, bid, buf, args->file->blocksize, KVCACHE_REQ_DIRTY);
            TEST_CHK(ret == args->file->blocksize);
        } else { // have some of the reader threads flush dirty immutable blocks
            if (bid <= args->nblocks / 4) { // 25% probability
//                filemgr_flush_immutable(args->file, NULL);
            }
        }

        if(args->time_sec) {
            gettimeofday(&ts_cur, NULL);
            ts_gap = _utime_gap(ts_begin, ts_cur);
            if ((size_t)ts_gap.tv_sec >= args->time_sec) break;
        }

        run_count++;
        
        if(run_count >= args->ops) {
            break;
        }
    }

    printf("Hit ratio %f\n", (double) hits / (double) (hits + misses));
    
    free(buf);
    thread_exit(0);
}

double multi_thread_test(
    int nblocks, int cachesize, int blocksize, int time_sec,
                       size_t ops, int nwriters, int nreaders)
{
    TEST_INIT();

    struct filemgr *file;
    struct filemgr_config config;
    int n = nwriters + nreaders;
    uint64_t i, j;
    uint32_t crc;
    uint8_t *buf;
    int r;
    char *fname = (char *) "./bcache_testfile";
    thread_t *tid = alca(thread_t, n);
    struct worker_args *args = alca(struct worker_args, n);
    void **ret = alca(void *, n);
    struct timeval ts_begin, ts_end, ts_gap;

    r = system(SHELL_DEL " bcache_testfile");
    (void)r;

    memleak_start();

    buf = (uint8_t *)malloc(4096);
    memset(buf, 0, 4096);

    memset(&config, 0, sizeof(config));
    config.blocksize = blocksize;
    config.ncacheblock = 0;
    config.kvssd = true;
    config.kv_data_cache = true;
    config.kv_cache_size = cachesize * blocksize;
    config.flag = 0x0;
    config.options = FILEMGR_CREATE;
    config.num_wal_shards = 8;
    filemgr_open_result result = filemgr_open(fname, get_filemgr_ops(), &config, NULL);
    file = result.file;

    gettimeofday(&ts_begin, NULL);
    for (i=0;i<(uint64_t)nblocks;++i) {
        memcpy(buf, &i, sizeof(i));
        j = 0;
        memcpy(buf + sizeof(i), &j, sizeof(j));
        crc = crc32_8(buf, sizeof(i) + sizeof(j), 0);
        memcpy(buf + sizeof(i) + sizeof(j), &crc, sizeof(crc));
//        bcache_write(file, (bid_t)i, buf, BCACHE_REQ_DIRTY, false);
        kvcache_write(file, i, buf, blocksize, KVCACHE_REQ_DIRTY );
    }
    gettimeofday(&ts_end, NULL);
    ts_gap = _utime_gap(ts_begin, ts_end);
    printf("%d.%09d seconds fill\n", (int)ts_gap.tv_sec, (int)ts_gap.tv_usec);

    printf("Starting\n");
    gettimeofday(&ts_begin, NULL);
    for (i=0;i<(uint64_t)n;++i){
        args[i].n = i;
        args[i].file = file;
        args[i].writer = ((i<(uint64_t)nwriters)?(1):(0));
        args[i].nblocks = nblocks;
        args[i].time_sec = time_sec;
        args[i].ops = ops;
        thread_create(&tid[i], worker, &args[i]);
    }

    DBG("wait for %d seconds..\n", time_sec);
    for (i=0;i<(uint64_t)n;++i){
        thread_join(tid[i], &ret[i]);
    }
    gettimeofday(&ts_end, NULL);
    
    ts_gap = _utime_gap(ts_begin, ts_end);
    printf("%d.%09d seconds elapsed\n", (int)ts_gap.tv_sec, (int)ts_gap.tv_usec);

    filemgr_commit(file, true, NULL);
    filemgr_close(file, true, NULL, NULL);
    filemgr_shutdown();
    free(buf);

    memleak_end();
    TEST_RESULT("multi thread test");
    
    return ts_gap.tv_usec;
}

double fill_test(
    int nrecords, int recordsize, int cachesize, int blocksize, int time_sec,
                       size_t ops, int nwriters, int nreaders)
{
    TEST_INIT();

    struct filemgr *file;
    struct filemgr_config config;
    uint64_t i, j;
    uint32_t crc;
    uint8_t *buf;
    int r;
    char *fname = (char *) "./bcache_testfile";
    struct timeval ts_begin, ts_end, ts_gap;

    r = system(SHELL_DEL " bcache_testfile");
    (void)r;

    memleak_start();

    buf = (uint8_t *)malloc(4096);
    memset(buf, 0, 4096);

    memset(&config, 0, sizeof(config));
    config.blocksize = blocksize;
    config.ncacheblock = 0;
    config.kvssd = true;
    config.kv_data_cache = true;
    config.kv_cache_size = (uint64_t)cachesize * blocksize;
    config.flag = 0x0;
    config.options = FILEMGR_CREATE;
    config.num_wal_shards = 8;
    filemgr_open_result result = filemgr_open(fname, get_filemgr_ops(), &config, NULL);
    file = result.file;

    gettimeofday(&ts_begin, NULL);
    for (i=0;i<(uint64_t)nrecords;++i) {
        memcpy(buf, &i, sizeof(i));
        j = 0;
        memcpy(buf + sizeof(i), &j, sizeof(j));
        crc = crc32_8(buf, sizeof(i) + sizeof(j), 0);
        memcpy(buf + sizeof(i) + sizeof(j), &crc, sizeof(crc));
        kvcache_write(file, i, buf, recordsize, KVCACHE_REQ_DIRTY);
    }
    gettimeofday(&ts_end, NULL);
    ts_gap = _utime_gap(ts_begin, ts_end);
    printf("%d.%09d seconds fill\n", (int)ts_gap.tv_sec, (int)ts_gap.tv_usec);
    
    filemgr_commit(file, true, NULL);
    filemgr_close(file, true, NULL, NULL);
    filemgr_shutdown();
    free(buf);

    memleak_end();
    TEST_RESULT("multi thread test");
    
    return ts_gap.tv_usec;
}

double ycsba(
    int nrecords, int recordsize, int count, int cachesize, int blocksize, int time_sec,
                       size_t ops, int nwriters, int nreaders)
{
    TEST_INIT();

    struct filemgr *file;
    struct filemgr_config config;
    uint64_t i, j;
    uint32_t crc;
    uint8_t *buf;
    int r;
    char *fname = (char *) "/mnt/ssd/ycsb";
    struct timeval ts_begin, ts_end, ts_gap;

    r = system(SHELL_DEL " /mnt/ssd/ycsb");
    (void)r;

    memleak_start();

    buf = (uint8_t *)malloc(4096);
    memset(buf, 0, 4096);

    memset(&config, 0, sizeof(config));
    config.blocksize = blocksize;
    config.ncacheblock = 0;
    config.kvssd = false;
    config.kv_data_cache = true;
    config.kv_cache_size = (uint64_t)cachesize * blocksize;
    config.flag = 0x0;
    config.options = FILEMGR_CREATE;
    config.num_wal_shards = 8;
    filemgr_open_result result = filemgr_open(fname, get_filemgr_ops(), &config, NULL);
    file = result.file;

    gettimeofday(&ts_begin, NULL);
    for (i=0;i<(uint64_t)nrecords;++i) {
        memcpy(buf, &i, sizeof(i));
        j = 0;
        memcpy(buf + sizeof(i), &j, sizeof(j));
        crc = crc32_8(buf, sizeof(i) + sizeof(j), 0);
        memcpy(buf + sizeof(i) + sizeof(j), &crc, sizeof(crc));
        kvcache_write(file, i, buf, recordsize, KVCACHE_REQ_DIRTY);
    }
    gettimeofday(&ts_end, NULL);
    ts_gap = _utime_gap(ts_begin, ts_end);
    printf("%d.%09d seconds fill\n", (int)ts_gap.tv_sec, (int)ts_gap.tv_usec);
    
    filemgr_commit(file, true, NULL);
    
    srand(301);
    init_zipf_generator(0, nrecords);
    int next_op;
    int k;
    ssize_t ret;
    uint32_t hits = 0, misses = 0;
    
    gettimeofday(&ts_begin, NULL);
    for(int i = 0; i < count; i++) {
        k = nextValue();
        next_op = rand() % 100;
        
        if(next_op < 50) {
            ret = kvcache_write(file, k, buf, recordsize, KVCACHE_REQ_DIRTY);
        } else {
            ret = kvcache_read(file, k, buf);
            if(ret <= 0) {
                misses++;
                if(!file->config->kvssd) {
                    ret = file->ops->pread(file->fd, buf,
                                 recordsize, k * file->blocksize);
                } else {
                    char* keyBuf;
                    uint8_t keysize;
                    union {
                        int i;
                        unsigned char c[4];
                    } u;
                    u.i = k;

                    keysize = sizeof(k);
                    keyBuf = (char *) malloc(keysize);
                    memset(keyBuf, 0x0, keysize);
                    for (int i = 0; i < PREFIX_LEN; i++) {
                        keyBuf[i] = u.c[(PREFIX_LEN - 1) - i];
                    }
                    
                    ret = file->kvssd->getKVTupleNoPrefix(keyBuf, sizeof(uint64_t), buf);
                }
                TEST_CHK(ret == (ssize_t)recordsize);
                ret = kvcache_write(file, k, buf, recordsize, KVCACHE_REQ_CLEAN);
                TEST_CHK(ret == recordsize);
            } else {
                hits++;
            }
        }
        
        if(i % 10000 == 0) {
            printf("Count %d hit ratio %f\n", i, (double) hits / (double) (hits + misses));
        }
    }
    gettimeofday(&ts_end, NULL);
    ts_gap = _utime_gap(ts_begin, ts_end);
    printf("%d.%09d seconds YCSB A %d ops\n", (int)ts_gap.tv_sec, (int)ts_gap.tv_usec, count);
    
    filemgr_commit(file, true, NULL);
    filemgr_close(file, true, NULL, NULL);
    filemgr_shutdown();
    free(buf);
    
    printf("Hit ratio %f\n", (double) hits / (double) (hits + misses));

    memleak_end();
    TEST_RESULT("ycsb A");
    
    return ts_gap.tv_usec;
}

double ycsbc(
    int nrecords, int recordsize, int count, int cachesize, int blocksize, int time_sec,
                       size_t ops, int nwriters, int nreaders)
{
    TEST_INIT();

    struct filemgr *file;
    struct filemgr_config config;
    uint64_t i, j;
    uint32_t crc;
    uint8_t *buf;
    int r;
    char *fname = (char *) "/tmp/ycsb";
    struct timeval ts_begin, ts_end, ts_gap;

    r = system(SHELL_DEL " /tmp/ycsb");
    (void)r;

    memleak_start();

    buf = (uint8_t *)malloc(4096);
    memset(buf, 0, 4096);

    memset(&config, 0, sizeof(config));
    config.blocksize = blocksize;
    config.ncacheblock = 0;
    config.kvssd = true;
    config.kv_data_cache = true;
    config.kv_cache_size = (uint64_t)cachesize * blocksize;
    config.flag = 0x0;
    config.options = FILEMGR_CREATE;
    config.num_wal_shards = 8;
    filemgr_open_result result = filemgr_open(fname, get_filemgr_ops(), &config, NULL);
    file = result.file;

    gettimeofday(&ts_begin, NULL);
    for (i=0;i<(uint64_t)nrecords;++i) {
        memcpy(buf, &i, sizeof(i));
        j = 0;
        memcpy(buf + sizeof(i), &j, sizeof(j));
        crc = crc32_8(buf, sizeof(i) + sizeof(j), 0);
        memcpy(buf + sizeof(i) + sizeof(j), &crc, sizeof(crc));
        kvcache_write(file, i, buf, recordsize, KVCACHE_REQ_DIRTY);
    }
    gettimeofday(&ts_end, NULL);
    ts_gap = _utime_gap(ts_begin, ts_end);
    printf("%d.%09d seconds fill\n", (int)ts_gap.tv_sec, (int)ts_gap.tv_usec);
    
    filemgr_commit(file, true, NULL);
    
    srand(301);
    init_zipf_generator(0, nrecords);
    int next_op;
    int k;
    ssize_t ret;
    uint32_t hits = 0, misses = 0;
    
    gettimeofday(&ts_begin, NULL);
    for(int i = 0; i < count; i++) {
        k = nextValue() % count;
        next_op = rand() % 100;
        
        if(next_op < 0) {
            ret = kvcache_write(file, k, buf, recordsize, KVCACHE_REQ_DIRTY);
        } else {
            ret = kvcache_read(file, k, buf);
            if(ret <= 0) {
                misses++;
                if(!file->config->kvssd) {
                    ret = file->ops->pread(file->fd, buf,
                                 recordsize, k * file->blocksize);
                } else {
                    char* keyBuf;
                    uint8_t keysize;
                    union {
                        int i;
                        unsigned char c[4];
                    } u;
                    u.i = k;

                    keysize = sizeof(k);
                    keyBuf = (char *) malloc(keysize);
                    memset(keyBuf, 0x0, keysize);
                    for (int i = 0; i < PREFIX_LEN; i++) {
                        keyBuf[i] = u.c[(PREFIX_LEN - 1) - i];
                    }
                    
                    ret = file->kvssd->getKVTupleNoPrefix(keyBuf, sizeof(uint64_t), buf);
                }
                TEST_CHK(ret == (ssize_t)recordsize);
                ret = kvcache_write(file, k, buf, recordsize, KVCACHE_REQ_CLEAN);
                TEST_CHK(ret == recordsize);
            } else {
                hits++;
            }
        }
        
        if(i % 10000 == 0) {
            printf("Count %d hit ratio %f\n", i, (double) hits / (double) (hits + misses));
        }
    }
    gettimeofday(&ts_end, NULL);
    ts_gap = _utime_gap(ts_begin, ts_end);
    printf("%d.%09d seconds YCSB C %d ops\n", (int)ts_gap.tv_sec, (int)ts_gap.tv_usec, count);
    
    filemgr_commit(file, true, NULL);
    filemgr_close(file, true, NULL, NULL);
    filemgr_shutdown();
    free(buf);
    
    printf("Hit ratio %f\n", (double) hits / (double) (hits + misses));

    memleak_end();
    TEST_RESULT("ycsb C");
    
    return ts_gap.tv_usec;
}

int main()
{
//    basic_test2();
    
//    uint64_t thirtyTwo = (32 * 1024 * 1024) / 32;
//    uint64_t sixtyFour = (64 * 1024 * 1024) / 32;
    
//     int nblocks, int cachesize, int blocksize, int time_sec,
//     size_t ops, int nwriters, int nreaders
    multi_thread_test(4, 1, 32, 0, 10000, 1, 0);
    
    // int nrecords, int recordsize, int cachesize (blocks), int blocksize, int time_sec,
    // size_t ops, int nwriters, int nreaders
//    uint64_t fourGiB = (3800 * 1024 * (uint64_t) 1024) / (uint64_t) 4096;
//    uint64_t oneGiB = (1024 * 1024 * (uint64_t) 1024) / (uint64_t) 4096;
//    fill_test(20000000, 1024, oneGiB, 4096, 0, 10000, 1, 0);
    
//    double ycsba(
//    int nrecords, int recordsize, int count, int cachesize, int blocksize, int time_sec,
//                       size_t ops, int nwriters, int nreaders)
//    uint64_t oneMiB = (1 * 1024 * 1024) / 4096;
//    uint64_t threeGiB = oneMiB * 3072;
//    ycsba(20000000, 1024, 1000000, threeGiB, 4096, 0, 0, 1, 0);
//    ycsbc(100, 1024, 1000000, oneMiB, 4096, 0, 0, 1, 0);

    return 0;
}
