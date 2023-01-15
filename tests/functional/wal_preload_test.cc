#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#if !defined(WIN32) && !defined(_WIN32)
#include <unistd.h>
#endif

#include "libforestdb/forestdb.h"
#include "test_kvssd.h"
#include "internal_types.h"
#include "functional_util.h"
#include "profiling.h"

void basic_test()
{
    TEST_INIT();

    memleak_start();

    _reset_kvssd("/dev/nvme0n1", "forestKV");

    int r , n = 131072; //, commit_freq = 8192;
    fdb_status status;
    fdb_file_handle *dbfile;
    fdb_kvs_handle *db;
    fdb_config fconfig = fdb_get_default_config_kvssd();
    fconfig.num_bgflusher_threads = 1;
    fconfig.kv_cache_size = 128LU * 1024 * 1024;
    fconfig.wal_threshold = 32768;
    fconfig.log_msg_level = 1;
    fconfig.kv_cache_doc_writes = true;
    fconfig.buffercache_size = 128LU * 1024 * 1024;
    fconfig.multi_kv_instances = false;
    fdb_kvs_config kvs_config = fdb_get_default_kvs_config();
    fdb_doc **doc = alca(fdb_doc*, n);

    char keybuf[256], bodybuf[256];

    r = system(SHELL_DEL" wal_preload_test* > errorlog.txt");
    (void)r;

    status = fdb_open(&dbfile, "./wal_preload_test1", &fconfig);
    TEST_CHK(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open_default(dbfile, &db, &kvs_config);
    TEST_CHK(status == FDB_RESULT_SUCCESS);

    struct timeval  tv1, tv2;
    gettimeofday(&tv1, NULL);

    for(int i = 0; i < n + 1; i++) {
        sprintf(keybuf, "key%d", i);
        sprintf(bodybuf, "body%d", i);
        fdb_doc_create(&doc[i], (void*)keybuf, strlen(keybuf),
            NULL, 0, (void*)bodybuf, strlen(bodybuf));
    }

    for (int i = 0; i < n; i++){
        sprintf(keybuf, "key%d", i);
        sprintf(bodybuf, "body%d1", i);
        fdb_doc_update(&doc[i], NULL, 0,
            (void *)bodybuf, strlen(bodybuf));
        status = fdb_set(db, doc[i]);
        TEST_CHK(status == FDB_RESULT_SUCCESS);

        // if(i % commit_freq == 0) {
        //     fdb_commit(dbfile, FDB_COMMIT_NORMAL);
        // }
    }

    fdb_commit(dbfile, FDB_COMMIT_NORMAL);

    printf("********** LOOP ONE **********\n");
    // sleep(5);

    for (int i = 0; i < n; i++){
        sprintf(keybuf, "key%d", i);
        sprintf(bodybuf, "body%d2", i);
        fdb_doc_update(&doc[i], NULL, 0,
            (void *)bodybuf, strlen(bodybuf));
        status = fdb_set(db, doc[i]);
        TEST_CHK(status == FDB_RESULT_SUCCESS);

        // if(i % commit_freq == 0) {
        //     fdb_commit(dbfile, FDB_COMMIT_NORMAL);
        // }
    }

    fdb_commit(dbfile, FDB_COMMIT_NORMAL);

    printf("********** LOOP TWO **********\n");
    // sleep(5);

    for (int i = 0; i < n; i++){
        sprintf(keybuf, "key%d", i);
        sprintf(bodybuf, "body%d3", i);
        fdb_doc_update(&doc[i], NULL, 0,
            (void *)bodybuf, strlen(bodybuf));
        status = fdb_set(db, doc[i]);
        TEST_CHK(status == FDB_RESULT_SUCCESS);

        // if(i % commit_freq == 0) {
        //     fdb_commit(dbfile, FDB_COMMIT_NORMAL);
        // }
    }

    fdb_commit(dbfile, FDB_COMMIT_NORMAL);
    gettimeofday(&tv2, NULL);

    printf("********** DONE **********\n");

    printf ("Total time = %f seconds\n",
         (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
         (double) (tv2.tv_sec - tv1.tv_sec));

    // fdb_commit(dbfile, FDB_COMMIT_MANUAL_WAL_FLUSH);

    sleep(6000);

    status = fdb_close(dbfile);
    TEST_CHK(status == FDB_RESULT_SUCCESS);
    status = fdb_shutdown();
    // TEST_CHK(status == FDB_RESULT_SUCCESS);

    for (int i = 0; i < n; i++){
        fdb_doc_free(doc[i]);
    }

    memleak_end();

    TEST_RESULT("basic test");
}

void commit_loop()
{
    TEST_INIT();

    memleak_start();

    _reset_kvssd("/dev/nvme0n1", "forestKV");

    int r , n = 131072, commit_freq = 8192;
    fdb_status status;
    fdb_file_handle *dbfile;
    fdb_kvs_handle *db;
    fdb_config fconfig = fdb_get_default_config_kvssd();
    fconfig.wal_flush_preloading = true;
    fconfig.num_bgflusher_threads = 1;
    fconfig.kv_cache_size = 3172LU * 1024 * 1024;
    fconfig.wal_threshold = 32768;
    fconfig.log_msg_level = 1;
    fconfig.kv_cache_doc_writes = true;
    fconfig.buffercache_size = 1024LU * 1024 * 1024;
    fconfig.multi_kv_instances = false;
    fconfig.wal_flush_before_commit = false;
    fdb_kvs_config kvs_config = fdb_get_default_kvs_config();
    // fdb_doc **doc = alca(fdb_doc*, n);
    fdb_doc *doc;
    uint64_t done = 0;

    char keybuf[256], bodybuf[256];

    r = system(SHELL_DEL" wal_preload_test* > errorlog.txt");
    (void)r;

    status = fdb_open(&dbfile, "./wal_preload_test1", &fconfig);
    TEST_CHK(status == FDB_RESULT_SUCCESS);
    status = fdb_kvs_open_default(dbfile, &db, &kvs_config);
    TEST_CHK(status == FDB_RESULT_SUCCESS);

    struct timeval  tv1, tv2;
    gettimeofday(&tv1, NULL);

    for(int i = 0; i < n; i++) {
        sprintf(keybuf, "key%d", i);
        sprintf(bodybuf, "body%d", i);
        fdb_doc_create(&doc, (void*)keybuf, strlen(keybuf),
            NULL, 0, (void*)bodybuf, strlen(bodybuf));
        status = fdb_set(db, doc);
        TEST_CHK(status == FDB_RESULT_SUCCESS);
        fdb_doc_free(doc);

        done++;

        if(done % commit_freq == 0) {
            fdb_commit(dbfile, FDB_COMMIT_NORMAL);
        }  
    }

    fdb_commit(dbfile, FDB_COMMIT_MANUAL_WAL_FLUSH);
    fdb_print_stats();
    profiling_reset_readkey_hr();

    for(int i = 0; i < n; i++) {
        sprintf(keybuf, "key%d", i);
        sprintf(bodybuf, "body%d", i);
        fdb_doc_create(&doc, (void*)keybuf, strlen(keybuf),
            NULL, 0, (void*)bodybuf, strlen(bodybuf));
        status = fdb_set(db, doc);
        TEST_CHK(status == FDB_RESULT_SUCCESS);
        fdb_doc_free(doc);

        done++;

        if(done % commit_freq == 0) {
            fdb_commit(dbfile, FDB_COMMIT_NORMAL);
        }  
    }

    fdb_commit(dbfile, FDB_COMMIT_MANUAL_WAL_FLUSH);
    fdb_print_stats();
    profiling_reset_readkey_hr();

    gettimeofday(&tv2, NULL);
    printf ("Total time = %f seconds\n",
         (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
         (double) (tv2.tv_sec - tv1.tv_sec));

    // fdb_commit(dbfile, FDB_COMMIT_MANUAL_WAL_FLUSH);

    sleep(5);

    status = fdb_close(dbfile);
    TEST_CHK(status == FDB_RESULT_SUCCESS);
    status = fdb_shutdown();
    // TEST_CHK(status == FDB_RESULT_SUCCESS);

    // for (int i = 0; i < n; i++){
        // fdb_doc_free(doc);
    // }

    memleak_end();

    TEST_RESULT("basic test");   
}

int main(){
    // basic_test();
    commit_loop();
}
