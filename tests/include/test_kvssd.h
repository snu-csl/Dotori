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

#ifndef _JSAHN_TEST_H
#define _JSAHN_TEST_H

#include <stdio.h>
#include <time.h>
#if !defined(WIN32) && !defined(_WIN32)
#include <sys/time.h>
#endif
#include "time_utils.h"

#include "kvssdmgr.h"
#include "libforestdb/forestdb.h"

#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

#define _TEST_GLOBAL
#ifdef _TEST_GLOBAL

#define RESET   "\033[0m"
#define BLACK   "\033[30m"      /* Black */
#define RED     "\033[31m"      /* Red */
#define GREEN   "\033[32m"      /* Green */
#define YELLOW  "\033[33m"      /* Yellow */
#define BLUE    "\033[34m"      /* Blue */
#define MAGENTA "\033[35m"      /* Magenta */
#define CYAN    "\033[36m"      /* Cyan */
#define WHITE   "\033[37m"      /* White */
#define BOLDBLACK   "\033[1m\033[30m"      /* Bold Black */
#define BOLDRED     "\033[1m\033[31m"      /* Bold Red */
#define BOLDGREEN   "\033[1m\033[32m"      /* Bold Green */
#define BOLDYELLOW  "\033[1m\033[33m"      /* Bold Yellow */
#define BOLDBLUE    "\033[1m\033[34m"      /* Bold Blue */
#define BOLDMAGENTA "\033[1m\033[35m"      /* Bold Magenta */
#define BOLDCYAN    "\033[1m\033[36m"      /* Bold Cyan */
#define BOLDWHITE   "\033[1m\033[37m"      /* Bold White */

#define TEST_INIT() \
    static int __test_pass=1; \
    struct timeval __test_begin, __test_prev, __test_cur, __test_interval_total, __test_interval_ins; \
    (void)__test_prev; \
    (void)__test_interval_total; \
    (void)__test_interval_ins; \
    (void)__test_pass; \
    gettimeofday(&__test_begin, NULL); \
    __test_cur = __test_begin

#define TEST_CHK(cond) {if (!(cond)) {fprintf(stderr, "Test failed: %s %d\n", __FILE__, __LINE__); __test_pass=0; assert(cond);}}
#define TEST_CMP(str1, str2, len) {if (memcmp(str1, str2, len)) {fprintf(stderr, "Test expected %s but got %s failed: %s %d\n", (char*)str2,(char*) str1, __FILE__, __LINE__); __test_pass=0; assert(false);}}
#define TEST_STATUS(status) {fdb_status s = (status); if (s != FDB_RESULT_SUCCESS) {fprintf(stderr, "Test failed with fdb_status %d (%s) at %s %d\n", s, fdb_error_msg(s), __FILE__, __LINE__); __test_pass=0; assert(false);}}
#define TEST_RESULT(name) {if ((__test_pass)) fprintf(stderr, GREEN "%s PASSED\n" RESET, (name)); else fprintf(stderr, RED "%s FAILED\n" RED, (name)); }

#define TEST_TIME() {\
    __test_prev = __test_cur; \
    gettimeofday(&__test_cur, NULL); \
    __test_interval_total = _utime_gap(__test_begin, __test_cur); \
    __test_interval_ins = _utime_gap(__test_prev, __test_cur); \
    DBG("Time elapsed: total %" _FSEC ".%06" _FUSEC " , interval %" _FSEC ".%06" _FUSEC "\n", \
        __test_interval_total.tv_sec, __test_interval_total.tv_usec, \
        __test_interval_ins.tv_sec, __test_interval_ins.tv_usec); }


#else

#define TEST_CHK(cond, sw) {if (!(cond)) {fprintf(stderr, "Test failed: %s %d\n", __FILE__, __LINE__); sw=0; assert(cond);}}
#define TEST_RESULT(name, sw) {if ((sw)) fprintf(stderr, "%s PASSED\n", (name)); else fprintf(stderr, "%s FAILED\n", (name)); }

#endif

/*
    ForestKV Modification
    Deleting a keyspace doesn't seem to drop the keys within it right now,
    and formatting the drive every test takes too long, so this is a reasonable
    way to clear the device for the next test. It still doesn't work in a handful
    of occasions, so a format is done before the test.
*/

#define kvssd_iter_buff_len (32*1024)
int _reset_kvssd(const char *dev_path, const char *keyspace_name)
{
#ifdef __KVSSD_EMU
    emulator_teardown();
    return 0;
#endif
    kvs_result ret;
    kvs_device_handle dev_handle;
    kvs_init_options kvs_options;
    char *container_name;
    kvs_container_handle chandle;
    kvs_container_context container_context = {{KVS_KEY_ORDER_NONE}};
    kvs_iterator_context iter_ctx_close;
    int count = KVS_MAX_ITERATE_HANDLE;
    kvs_iterator_info kvs_iters[count];
    int total_entries = 0;

    kvs_init_env_opts(&kvs_options);
    kvs_options.memory.use_dpdk = 0;
    kvs_init_env(&kvs_options);

    if(!strcmp(dev_path, "/dev/kvemul")) {
        printf("Can't reset the emulator\n");
        return -1;
    }

    ret = kvs_open_device(dev_path, &dev_handle);
    if (ret != KVS_SUCCESS) {
        printf("KVSSD %s open failed %s in _reset_kvssd()", dev_path, kvs_errstr(ret));
        return -1;
    }
    
    container_name = (char*) "forestKV";
    ret = kvs_open_container(dev_handle, container_name, &chandle);
    if(ret == KVS_ERR_CONT_NOT_EXIST) {
        ret = kvs_create_container(dev_handle, container_name, 0, &container_context);
        if(ret != KVS_SUCCESS) {
            printf("KVSSD container %s create failed in _reset_kvssd()", container_name);
        }
        ret = kvs_open_container(dev_handle, container_name, &chandle);    
        if(ret != KVS_SUCCESS) {
            printf("Container %s open failed in _reset_kvssd()", container_name);
            return -1;
        }
    }

    if(ret != KVS_SUCCESS) {
        printf("KVSSD container %s open failed in _reset_kvssd()", container_name);
        return -1;
    }

    iter_ctx_close.private1 = NULL;
    iter_ctx_close.private2 = NULL;
    memset(kvs_iters, 0, sizeof(kvs_iters));
    int res = kvs_list_iterators(chandle, kvs_iters, count);
    
    if(res) {
        printf("Couldn't check open iterators in reset_kvssd()");
        return -1;
    } else {
        for (int i = 0; i < count; i++){
            kvs_close_iterator(chandle, kvs_iters[i].iter_handle, &iter_ctx_close);
        }
    }

    char key[14];

    sprintf(key, "@milestone");
    kvs_key kvskey = {key, 10};
    kvs_delete_context del_ctx = { {false}, 0, 0};
    ret = kvs_delete_tuple(chandle, &kvskey, &del_ctx);
    if(ret) {
        printf("Failed to delete milestone in _reset_kvssd()\n");
    }

    sprintf(key, "@Super_Block0");
    kvskey.length = 13;
    for(int i = 0; i < 4; i++) {
        key[12] = i + 'A';
        ret = kvs_delete_tuple(chandle, &kvskey, &del_ctx);
        if(ret) {
            printf("Failed to delete %s in _reset_kvssd()\n", key);
        }
    }

    /*
     * Don't know how many KV pairs we just wrote since last run,
     * so just try and delete delete 50 commits to be safe.
     */

    for(uint64_t commit_id = 0; commit_id < 50; commit_id++) {
        unsigned char prefix[5] = "0000";
        vernum_to_prefix(commit_id, prefix, 4);

        struct kvssd_iterator_info *iter_info = (struct kvssd_iterator_info *) malloc(sizeof(struct kvssd_iterator_info));
        kvs_iterator_type iter_type = KVS_ITERATOR_KEY;
        iter_info->g_iter_mode.iter_type = iter_type;

        kvs_iterator_context iter_ctx_open;

        iter_ctx_open.bitmask = 0xFFFFFFFF;

        unsigned int bit_pattern = 0;
        for (int i = 0; i < PREFIX_LEN; i++){
            bit_pattern |= (prefix[i] << (3-i)*8);
        }

        iter_ctx_open.bit_pattern = bit_pattern;
        iter_ctx_open.private1 = NULL;
        iter_ctx_open.private2 = NULL;

        iter_ctx_open.option.iter_type = iter_type;

        ret = kvs_open_iterator(chandle, &iter_ctx_open, &iter_info->iter_handle);
        if(ret != KVS_SUCCESS) {
            printf("Iterator open failed in _reset_kvssd()\n");
            return -1;
        }

        iter_info->iter_list.size = kvssd_iter_buff_len;
        uint8_t *buffer;
        buffer = (uint8_t*) malloc(kvssd_iter_buff_len);
        iter_info->iter_list.it_list = (uint8_t*) buffer;

        kvs_iterator_context iter_ctx_next;
        iter_ctx_next.private1 = iter_info;
        iter_ctx_next.private2 = NULL;

        iter_info->iter_list.end = 0;
        iter_info->iter_list.num_entries = 0;

        /*
           ForestKV Modification
           The iterator sometimes doesn't get everything on one pass, so we do 5 to
           be safe.
           */

        for(int i = 0; i < 5; i++) {
            while(1) {
                iter_info->iter_list.size = kvssd_iter_buff_len;
                memset(iter_info->iter_list.it_list, 0, kvssd_iter_buff_len);
                ret = kvs_iterator_next(chandle, iter_info->iter_handle, &iter_info->iter_list, &iter_ctx_next);
                if(ret != KVS_SUCCESS) {
                    printf("Iterator next failed in _reset_kvssd()\n");
                    free(iter_info);
                    free(buffer);
                    return -1;
                }

                total_entries += iter_info->iter_list.num_entries;

                if(iter_info->iter_list.num_entries > 0) {
                    uint32_t key_size = 0;
                    char key[256];
                    uint8_t *it_buffer = (uint8_t*) iter_info->iter_list.it_list;

                    for(unsigned int i = 0; i < iter_info->iter_list.num_entries; i++) {
                        // get key size
                        key_size = *((unsigned int*)it_buffer);
                        it_buffer += sizeof(unsigned int);

                        // print key
                        memcpy(key, it_buffer, key_size);
                        key[key_size] = 0;
                        // fprintf(stdout, "%dth key --> %s len %u\n", i, key, key_size);

                        const kvs_key kvskey = {key, (kvs_key_t) key_size};
                        const kvs_delete_context del_ctx = { {true}, 0, 0};

                        ret = kvs_delete_tuple(chandle, &kvskey, &del_ctx);

                        if(ret) {
                            printf("Failed to delete a key in reset_kvssd\n");
                        }

                        it_buffer += key_size;
                    }
                }

                if(iter_info->iter_list.end) {
                    break;
                } else {
                    // fprintf(stdout, "More keys available, do another iteration\n");
                    memset(iter_info->iter_list.it_list, 0, kvssd_iter_buff_len);
                }
            } 
        }

        if(total_entries == 0) {
            printf("There were no keys to delete for %lu in _reset_kvssd()\n", 
                    commit_id);
        }

        total_entries = 0;

        iter_ctx_close.private1 = NULL;
        iter_ctx_close.private2 = NULL;
        ret = kvs_close_iterator(chandle, iter_info->iter_handle, &iter_ctx_close);
        if(ret != KVS_SUCCESS) {
            printf("Failed to close iterator in _reset_kvssd()");
            free(buffer);
            free(iter_info);
            return -1;
        }

        if(buffer) free(buffer);
        if(iter_info) free(iter_info);
    }

    int events = 0;
    while((events = kvs_get_ioevents(chandle, 64)) != 0) {
        printf("Flushed %d outstanding events in _reset_kvssd()", events);
    }
    
    kvs_close_container(chandle);
    kvs_delete_container(dev_handle, container_name);
    kvs_close_device(dev_handle);
    kvs_exit_env();

    return 0;
}

#ifdef __cplusplus
}
#endif

#if defined(WIN32) || defined(_WIN32)
#define SHELL_DEL "del /f "
#define SHELL_COPY "copy "
#define SHELL_MOVE "move "
#define SHELL_MKDIR "mkdir "
#define SHELL_RMDIR "rd /s/q "
#define SHELL_DMT "\\"
#define SHELL_MAX_PATHLEN (256)
#else
#define SHELL_DEL "rm -rf "
#define SHELL_COPY "cp "
#define SHELL_MOVE "mv "
#define SHELL_MKDIR "mkdir "
#define SHELL_RMDIR SHELL_DEL
#define SHELL_DMT "/"
#define SHELL_MAX_PATHLEN (1024)
#endif

#include "memleak.h"

#endif


