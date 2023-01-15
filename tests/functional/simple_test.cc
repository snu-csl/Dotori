//
// Created by ShimJaeHoon on 2020/05/02.
//
#include <string>
#include <stdio.h>
#include <assert.h>
#include "libforestdb/forestdb.h"
#include "test_kvssd.h"
#include "common.h"
#include "fdb_internal.h"

#define RUN 262144
#define COMMIT 65536
#define RANDOM 0

void gen_random(char *s, const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = 0;
}

int main() {
    TEST_INIT();

    memleak_start();

    _reset_kvssd("/dev/nvme0n1", "forestKV");

    fdb_file_handle *fhandle;
    fdb_kvs_handle *kvhandle;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_status s;
    const char *db_name = "/tmp/db_filename";

    (void)s;

    config = fdb_get_default_config_kvssd();
    config.kv_cache_size = 1LU * 1024 * 1024;
    config.wal_threshold = 131072;
    config.block_reusing_threshold = 0;
    config.log_msg_level = 1;
    config.kv_cache_doc_writes = true;
    config.buffercache_size = 2LU * 1024 * 1024;
    config.wal_flush_preloading = false;
    config.num_bgflusher_threads = 0;
    config.multi_kv_instances = false;
    config.wal_flush_before_commit = false;
    config.logging = true;
    config.index_blocksize = config.blocksize;
    config.max_log_threshold = ((config.blocksize - sizeof(struct bnode))/16)*1;
    config.split_threshold = config.blocksize;

    kvs_config = fdb_get_default_kvs_config();

    fdb_doc *doc;
    std::string key = "keybdce";
    std::string value = "valuevaluevalue";
    char *ch_key = (char*)malloc(9);

    printf("RUN: %d\n", RUN);

    int indexes[RUN];
    // char value_buf[2097152];
    // int vlen = 1234;

    struct timeval  tv1, tv2;

    // write 4 sb
    s = fdb_open(&fhandle, db_name, &config);
    assert(s == FDB_RESULT_SUCCESS);

    // fdb_open에서도 만들고 또 만드는 이유는 1개 file에 여러 kvstore 존재 가능해서?....
    fdb_kvs_open_default(fhandle, &kvhandle, &kvs_config);
    assert(s == FDB_RESULT_SUCCESS);

    gettimeofday(&tv1, NULL);
    srand(7812);
    for (int i = 0; i < RUN; i++) {

        int idx;

        if(RANDOM) {
            idx = rand() % RUN;
            indexes[i] = idx;
        } else {
            idx  = i;
        }

        sprintf(ch_key, "%0*d", 8, idx);
        std::string tmpValue = value + std::to_string(idx);

        s = fdb_doc_create(&doc, (void*)ch_key, 8, NULL, 0, tmpValue.c_str(), tmpValue.length());
        assert(s == FDB_RESULT_SUCCESS);
        // fprintf(stderr, "set %d %s\n", i, ch_key);
        s = fdb_set(kvhandle, doc);
        assert(s == FDB_RESULT_SUCCESS);
        s = fdb_doc_free(doc);
        assert(s == FDB_RESULT_SUCCESS);

        if(i % COMMIT == 0) {
            // commit을 통해 roothandle이랑 kvhandle 맞춰짐?
            s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
            assert(s == FDB_RESULT_SUCCESS);
        }

        if(i % 10000 == 0) {
            printf("Done %d\n", i);
        }
    }
    s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
    assert(s == FDB_RESULT_SUCCESS);

    fdb_reset_stats();

//    srand(7812);
//    for (int i = 0; i < RUN; i++) {
//        int j = rand() % (i + 1);
//        // indexes[i] = j;
//
//        sprintf(ch_key, "%0*d", 7, j);
//        std::string tmpValue = value + std::to_string(j+i);
//
//        s = fdb_doc_create(&doc, (void*)ch_key, 8, NULL, 0, tmpValue.c_str(), tmpValue.length());
//        assert(s == FDB_RESULT_SUCCESS);
//        fprintf(stderr, "update %d %s\n", i, ch_key);
//        s = fdb_set(kvhandle, doc);
//        assert(s == FDB_RESULT_SUCCESS);
//        s = fdb_doc_free(doc);
//        assert(s == FDB_RESULT_SUCCESS);
//
//        if(i % COMMIT == 0) {
//            // commit을 통해 roothandle이랑 kvhandle 맞춰짐?
//            s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
//            assert(s == FDB_RESULT_SUCCESS);
//        }
//    }
//    s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
//    assert(s == FDB_RESULT_SUCCESS);

    srand(7812);
    for (int i = 0; i < RUN; i++) {
        
        int idx;

        if(RANDOM) {
            idx = indexes[i];
        } else {
            idx = i;
        }

        sprintf(ch_key, "%0*d", 8, idx);
        s = fdb_doc_create(&doc, (void*)ch_key, 8, NULL, 0, NULL, 0);
        assert(s == FDB_RESULT_SUCCESS);
        // fprintf(stderr, "get %d %s\n", i, ch_key);
        s = fdb_get(kvhandle, doc);
//        char val[256];
//        memset(val, 0, 256);
//        memcpy(val, doc->body, doc->bodylen);
//        fprintf(stderr, "value: %s\n", val);
        assert(s == FDB_RESULT_SUCCESS);
        s = fdb_doc_free(doc);
        assert(s == FDB_RESULT_SUCCESS);

        if(i % 10000 == 0) {
            printf("Done %d\n", i);
        }
    }

    s = fdb_kvs_close(kvhandle);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_close(fhandle);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_shutdown();
    assert(s == FDB_RESULT_SUCCESS);

    FILE * f;
    f = fopen(db_name, "r");
    if (f != NULL) {
        fseek(f, 0, SEEK_END);
        printf("File Offset: %ld\n", ftell(f));
        fclose(f);
    }

    memleak_end();

    fdb_print_stats();

    gettimeofday(&tv2, NULL);
    printf ("Total time = %f seconds\n",
         (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
         (double) (tv2.tv_sec - tv1.tv_sec));

    TEST_RESULT("kvssd simple test");

    return 0;
}
