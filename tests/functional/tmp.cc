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

#define RUN 1000000
#define COMMIT 65536
#define RANDOM 0
#define VLEN 1024

int *indexes;

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

atomic_uint32_t count;
void cb_fn(fdb_kvs_handle *handle, fdb_doc *doc, void *args, int rderrno)
{
    fdb_status s;
    assert(rderrno == FDB_RESULT_SUCCESS);
    atomic_incr_uint32_t(&count);
    s = fdb_doc_free(doc);
    assert(s == FDB_RESULT_SUCCESS);
}

int overwrite() {
    TEST_INIT();

    memleak_start();

    _reset_kvssd("/dev/nvme0n1", "forestKV");

    indexes = (int*) malloc(sizeof(int) * RUN);

    fdb_file_handle *fhandle;
    fdb_kvs_handle *kvhandle;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_status s;
    char keybuf[256], metabuf[256], bodybuf[256];
    const char *db_name = "./db_filename";

    (void)s;

    config = fdb_get_default_config_kvssd();
    config.kv_cache_size = 1024LU*.75*1024*1024;
    config.wal_threshold = 131072;
    config.block_reusing_threshold = 0;
    config.log_msg_level = 1;
    config.kv_cache_doc_writes = false;
    config.buffercache_size = 1024LU*.25*1024*1024;
    config.wal_flush_preloading = false;
    config.num_bgflusher_threads = 0;
    config.multi_kv_instances = false;
    config.wal_flush_before_commit = false;
    config.logging = true;
    config.index_blocksize = config.blocksize;
    config.max_log_threshold = ((config.blocksize - sizeof(struct bnode))/16)*1;
    config.split_threshold = config.blocksize;
    config.max_logs_per_node = 64;

    kvs_config = fdb_get_default_kvs_config();

    std::string key = "keybdce";
    std::string value = "valuevaluevalue";

    printf("RUN: %d\n", RUN);

    atomic_init_uint32_t(&count, 0);

    char value_buf[VLEN];
    gen_random(value_buf, VLEN - 1);

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
        fdb_doc *doc;

        int idx;

        if(RANDOM) {
            idx = rand() % RUN;
            indexes[i] = idx;
        } else {
            idx  = i;
        }

        sprintf(keybuf, "%0*d", 8, idx);
        sprintf(metabuf, "meta%128d", idx);
        sprintf(bodybuf, "body%128d", idx);
        s = fdb_doc_create(&doc, (void*)keybuf, strlen(keybuf),
                (void*)metabuf, strlen(metabuf), (void*)bodybuf, strlen(bodybuf));
        assert(s == FDB_RESULT_SUCCESS);
        s = fdb_set(kvhandle, doc);
        assert(s == FDB_RESULT_SUCCESS);
        s = fdb_doc_free(doc);
        assert(s == FDB_RESULT_SUCCESS);

        if(i % COMMIT == 0) {
            s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
            assert(s == FDB_RESULT_SUCCESS);
        }

        if(i % 10000 == 0) {
            printf("Done %d\n", i);
        }
    }

    gettimeofday(&tv2, NULL);
    double time = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
            (double) (tv2.tv_sec - tv1.tv_sec);
    printf("Insert TP %f ops/s\n", RUN / time);

    s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_kvs_close(kvhandle);
    assert(s == FDB_RESULT_SUCCESS);
    s = fdb_close(fhandle);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_open(&fhandle, db_name, &config);
    assert(s == FDB_RESULT_SUCCESS);
    fdb_kvs_open_default(fhandle, &kvhandle, &kvs_config);
    assert(s == FDB_RESULT_SUCCESS);

    fdb_reset_stats();

    gettimeofday(&tv1, NULL);
    gettimeofday(&tv2, NULL);
    double prev_time = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
        (double) (tv2.tv_sec - tv1.tv_sec);
    int prev_ops = 0;

    srand(7812);
    for (int i = 0; i < RUN * 10; i++) {
        fdb_doc *doc;

        int idx;

        if(RANDOM) {
            idx = rand() % RUN;
            indexes[i] = idx;
        } else {
            idx  = i;
        }

        idx = idx % RUN;

        sprintf(keybuf, "%0*d", 8, idx);
        sprintf(metabuf, "meta%128d", idx);
        sprintf(bodybuf, "body%128d", idx);
        s = fdb_doc_create(&doc, (void*)keybuf, strlen(keybuf),
                (void*)metabuf, strlen(metabuf), (void*)bodybuf, strlen(bodybuf));
        assert(s == FDB_RESULT_SUCCESS);
        s = fdb_set(kvhandle, doc);
        assert(s == FDB_RESULT_SUCCESS);
        s = fdb_doc_free(doc);
        assert(s == FDB_RESULT_SUCCESS);

        if(i % COMMIT == 0) {
            s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
            assert(s == FDB_RESULT_SUCCESS);
        }

        if(i % 50000 == 0) {
            printf("Done %d\n", i);
        }

        gettimeofday(&tv2, NULL);
        time = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
            (double) (tv2.tv_sec - tv1.tv_sec);

        if(time - prev_time > 1) {
            printf("%f ops/s\n", (i - prev_ops) / (time - prev_time));
            prev_time = time;
            prev_ops = i;
        }
    }

    gettimeofday(&tv2, NULL);
    time = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
        (double) (tv2.tv_sec - tv1.tv_sec);
    printf("Overwrite TP %f ops/s\n", (RUN * 10) / time);

    s = fdb_kvs_close(kvhandle);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_close(fhandle);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_shutdown();
    assert(s == FDB_RESULT_SUCCESS);

    memleak_end();

    fdb_print_stats();

    TEST_RESULT("kvssd simple test");

    _reset_kvssd("/dev/nvme0n1", "forestKV");    

    return 0;
}

int insert_then_read() {
    TEST_INIT();

    memleak_start();

    _reset_kvssd("/dev/nvme0n1", "forestKV");

    indexes = (int*) malloc(sizeof(int) * RUN);

    fdb_file_handle *fhandle;
    fdb_kvs_handle *kvhandle;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_status s;
    char keybuf[256], metabuf[256], bodybuf[256];
    const char *db_name = "./db_filename";

    (void)s;

    config = fdb_get_default_config_kvssd();
    config.kv_cache_size = 1024LU*.75*1024*1024;
    config.wal_threshold = 131072;
    config.block_reusing_threshold = 0;
    config.log_msg_level = 1;
    config.kv_cache_doc_writes = false;
    config.buffercache_size = 1024LU*.25*1024*1024;
    config.wal_flush_preloading = false;
    config.num_bgflusher_threads = 0;
    config.multi_kv_instances = false;
    config.wal_flush_before_commit = false;
    config.logging = true;
    config.index_blocksize = config.blocksize;
    config.max_log_threshold = ((config.blocksize - sizeof(struct bnode))/16)*1;
    config.split_threshold = config.blocksize;
    config.max_logs_per_node = 5;

    kvs_config = fdb_get_default_kvs_config();

    std::string key = "keybdce";
    std::string value = "valuevaluevalue";

    printf("RUN: %d\n", RUN);

    atomic_init_uint32_t(&count, 0);

    char value_buf[VLEN];
    gen_random(value_buf, VLEN - 1);

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
        fdb_doc *doc;

        int idx;

        if(RANDOM) {
            idx = rand() % RUN;
            indexes[i] = idx;
        } else {
            idx  = i;
        }

        sprintf(keybuf, "%0*d", 8, idx);
        sprintf(metabuf, "meta%128d", idx);
        sprintf(bodybuf, "body%128d", idx);
        s = fdb_doc_create(&doc, (void*)keybuf, strlen(keybuf),
                (void*)metabuf, strlen(metabuf), (void*)bodybuf, strlen(bodybuf));
        assert(s == FDB_RESULT_SUCCESS);
        s = fdb_set(kvhandle, doc);
        assert(s == FDB_RESULT_SUCCESS);
        s = fdb_doc_free(doc);
        assert(s == FDB_RESULT_SUCCESS);

        if(i % COMMIT == 0) {
            s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
            assert(s == FDB_RESULT_SUCCESS);
        }

        if(i % 10000 == 0) {
            printf("Done %d\n", i);
        }
    }

    gettimeofday(&tv2, NULL);
    double time = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
            (double) (tv2.tv_sec - tv1.tv_sec);
    printf("Insert TP %f ops/s\n", RUN / time);

    s = fdb_commit(fhandle, FDB_COMMIT_MANUAL_WAL_FLUSH);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_kvs_close(kvhandle);
    assert(s == FDB_RESULT_SUCCESS);
    s = fdb_close(fhandle);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_open(&fhandle, db_name, &config);
    assert(s == FDB_RESULT_SUCCESS);
    fdb_kvs_open_default(fhandle, &kvhandle, &kvs_config);
    assert(s == FDB_RESULT_SUCCESS);

    srand(7812);
    for (int i = 0; i < RUN; i++) {
        fdb_doc *doc;

        int idx;

        if(RANDOM) {
            idx = indexes[i];
        } else {
            idx = i;
        }

        sprintf(keybuf, "%0*d", 8, idx);
        sprintf(metabuf, "meta%128d", idx);
        sprintf(bodybuf, "body%128d", idx);
        s = fdb_doc_create(&doc, (void*)keybuf, strlen(keybuf),
                       NULL, 0, NULL, 0);
        assert(s == FDB_RESULT_SUCCESS);
        s = fdb_get_async(kvhandle, doc, cb_fn, NULL);
        assert(s == FDB_RESULT_SUCCESS);

        if(i % 10000 == 0) {
            printf("Done %d\n", i);
        }
    }

    while(atomic_get_uint32_t(&count) < RUN) {}

    s = fdb_kvs_close(kvhandle);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_close(fhandle);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_shutdown();
    assert(s == FDB_RESULT_SUCCESS);


    memleak_end();

    fdb_print_stats();

    gettimeofday(&tv2, NULL);
    printf ("Total time = %f seconds\n",
         (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
         (double) (tv2.tv_sec - tv1.tv_sec));

    TEST_RESULT("kvssd simple test");

    _reset_kvssd("/dev/nvme0n1", "forestKV");    

    return 0;
}


#define RATIO 50

int rw_mix() {
    TEST_INIT();

    memleak_start();

    _reset_kvssd("/dev/nvme0n1", "forestKV");

    fdb_file_handle *fhandle;
    fdb_kvs_handle *kvhandle;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_status s;
    const char *db_name = "./db_filename";
    char keybuf[9];
    memset(keybuf, 0x0, 9);

    (void)s;

    config = fdb_get_default_config_kvssd();
    config.kv_cache_size = 4096LU*1024*1024;
    config.wal_threshold = 1310720;
    config.block_reusing_threshold = 0;
    config.log_msg_level = 1;
    config.kv_cache_doc_writes = false;
    config.buffercache_size = 2048LU*1024*1024;
    config.wal_flush_preloading = false;
    config.num_bgflusher_threads = 0;
    config.multi_kv_instances = false;
    config.wal_flush_before_commit = false;
    config.logging = true;
    config.index_blocksize = config.blocksize;
    config.max_log_threshold = ((config.blocksize - sizeof(struct bnode))/16)*1;
    config.split_threshold = config.blocksize;
    config.max_logs_per_node = 5;

    kvs_config = fdb_get_default_kvs_config();

    std::string key = "keybdce";
    std::string value = "valuevaluevalue";

    printf("RUN: %d\n", RUN);

    atomic_init_uint32_t(&count, 0);

    char value_buf[VLEN];
    gen_random(value_buf, VLEN - 1);

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
        fdb_doc *doc;

        int idx;

        if(RANDOM) {
            idx = rand() % RUN;
            indexes[i] = idx;
        } else {
            idx  = i;
        }

        sprintf(keybuf, "%0*d", 8, idx);
        s = fdb_doc_create(&doc, (void*)keybuf, 8, NULL, 0, value_buf, VLEN);
        assert(s == FDB_RESULT_SUCCESS);
        s = fdb_set(kvhandle, doc);
        assert(s == FDB_RESULT_SUCCESS);
        s = fdb_doc_free(doc);
        assert(s == FDB_RESULT_SUCCESS);

        if(i % COMMIT == 0) {
            s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
            assert(s == FDB_RESULT_SUCCESS);
        }

        if(i % 10000 == 0) {
            printf("Done %d\n", i);
        }
    }
    s = fdb_commit(fhandle, FDB_COMMIT_MANUAL_WAL_FLUSH);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_kvs_close(kvhandle);
    assert(s == FDB_RESULT_SUCCESS);
    s = fdb_close(fhandle);
    assert(s == FDB_RESULT_SUCCESS);
    s = fdb_shutdown();
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_open(&fhandle, db_name, &config);
    assert(s == FDB_RESULT_SUCCESS);
    fdb_kvs_open_default(fhandle, &kvhandle, &kvs_config);
    assert(s == FDB_RESULT_SUCCESS);

    uint32_t writes_done = 0;
    uint32_t reads_done = 0;

    srand(7812);
    for (int i = 0; i < RUN; i++) {
        fdb_doc *doc;

        int idx;
        uint32_t op;

        if(RANDOM) {
            idx = indexes[i];
        } else {
            idx = i;
        }

        sprintf(keybuf, "%0*d", 8, idx);
        op = rand() % 100;

        if(op > RATIO) {
            s = fdb_doc_create(&doc, (void*)keybuf, 8, NULL, 0, value_buf, VLEN);
            assert(s == FDB_RESULT_SUCCESS);
            s = fdb_set(kvhandle, doc);
            assert(s == FDB_RESULT_SUCCESS);
            s = fdb_doc_free(doc);
            assert(s == FDB_RESULT_SUCCESS);

            writes_done++;
        } else {
            s = fdb_doc_create(&doc, (void*)keybuf, 8, NULL, 0, NULL, 0);
            assert(s == FDB_RESULT_SUCCESS);
            s = fdb_get_async(kvhandle, doc, cb_fn, NULL);

            if(s != FDB_RESULT_SUCCESS) {
                printf("failing for %s\n", keybuf);
                fflush(stdout);
            }

            assert(s == FDB_RESULT_SUCCESS);

            reads_done++;
        }

        if(writes_done % COMMIT == 0) {
            s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
            assert(s == FDB_RESULT_SUCCESS);
        }

        if(i % 10000 == 0) {
            printf("Done %d\n", i);
        }
    }

    while(atomic_get_uint32_t(&count) < reads_done) {}

    s = fdb_kvs_close(kvhandle);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_close(fhandle);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_shutdown();
    assert(s == FDB_RESULT_SUCCESS);

    memleak_end();

    fdb_print_stats();

    gettimeofday(&tv2, NULL);
    printf ("Total time = %f seconds\n",
         (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
         (double) (tv2.tv_sec - tv1.tv_sec));

    TEST_RESULT("kvssd simple test");

    _reset_kvssd("/dev/nvme0n1", "forestKV");    

    return 0;
}

int main()
{
    overwrite();
    //insert_then_read();
    //rw_mix();
}
