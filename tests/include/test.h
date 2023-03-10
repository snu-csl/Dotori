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

#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

#define RESET   "\033[0m"
#define RED     "\033[31m"      /* Red */
#define GREEN   "\033[32m"      /* Green */

#define _TEST_GLOBAL
#ifdef _TEST_GLOBAL

static void _print_key(char *key)
{
    printf("Key %x %x %x %x %x %x %x %x\n", 
                    (unsigned char)key[0],
                    (unsigned char)key[1],
                    (unsigned char)key[2],
                    (unsigned char)key[3],
                    (unsigned char)key[4],
                    (unsigned char)key[5],
                    (unsigned char)key[6],
                    (unsigned char)key[7]);
    fflush(stdout);
}

#ifdef __KVSSD_EMU
void emulator_teardown() {
    kvs_result ret;
    kvs_device_handle dev_hd;
    char* dev_path = (char*) "/dev/kvemul";

    ret = kvs_open_device((char*) dev_path, &dev_hd);
    if(ret != KVS_SUCCESS) {
        printf("Failed to open emulator in emulator_teardown.\n");
        return;
    }

    kvs_key_space_name ks_name;
    ks_name.name = (char*) "dotori";
    ks_name.name_len = strlen(ks_name.name);
    kvs_delete_key_space(dev_hd, &ks_name);
    kvs_close_device(dev_hd);
}
#endif

#define kvssd_iter_buff_len (32*1024)
int _reset_kvssd(const char *dev_path, const char *keyspace_name)
{
#ifdef __KVSSD_EMU
    return 0;
    emulator_teardown();
    return 0;
#endif
    char cmd[256];
    sprintf(cmd, "nvme format /dev/nvme0n1");
    system(cmd);
    return 0;
}

#define TEST_INIT() \
    static int __test_pass=1; \
    struct timeval __test_begin, __test_prev, __test_cur, __test_interval_total, __test_interval_ins; \
    (void)__test_prev; \
    (void)__test_interval_total; \
    (void)__test_interval_ins; \
    (void)__test_pass; \
    gettimeofday(&__test_begin, NULL); \
    __test_cur = __test_begin; \
    _reset_kvssd("/dev/nvme0n1", "dotori");

#define TEST_CHK(cond) {if (!(cond)) {fprintf(stderr, "Test failed: %s %d\n", __FILE__, __LINE__); __test_pass=0; assert(cond);}}
#define TEST_CMP(str1, str2, len) {if (memcmp(str1, str2, len)) {fprintf(stderr, "Test expected %s but got %s failed: %s %d\n", (char*)str2,(char*) str1, __FILE__, __LINE__); __test_pass=0; assert(false);}}
#define TEST_STATUS(status) {fdb_status s = (status); if (s != FDB_RESULT_SUCCESS) {fprintf(stderr, "Test failed with fdb_status %d (%s) at %s %d\n", s, fdb_error_msg(s), __FILE__, __LINE__); __test_pass=0; assert(false);}}
#define TEST_RESULT(name) {if ((__test_pass)) \
    fprintf(stderr, GREEN "%s PASSED\n" RESET, (name)); \
    else fprintf(stderr, RED "%s FAILED\n" RESET, (name)); }

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


