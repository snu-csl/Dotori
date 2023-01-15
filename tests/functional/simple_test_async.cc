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

#define VLEN 1024

int *indexes;
double *lats;

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

static int compare (const void * a, const void * b)
{
    if (*(double*)a > *(double*)b) return 1;
    else if (*(double*)a < *(double*)b) return -1;
    else return 0;  
}

struct _lat_args {
    uint32_t idx;
    timeval start;
};

void cb_fn_lat(fdb_kvs_handle *handle, fdb_doc *doc, void *voidargs, int rderrno)
{
    struct _lat_args *args;
    struct timeval end;

    args = (struct _lat_args*)voidargs;
    gettimeofday(&end, NULL);

    lats[args->idx] = ((end.tv_sec - args->start.tv_sec) * 1000000) + (end.tv_usec - args->start.tv_usec);

    fdb_status s;
    assert(rderrno == FDB_RESULT_SUCCESS);
    atomic_incr_uint32_t(&count);
    s = fdb_doc_free(doc);
    assert(s == FDB_RESULT_SUCCESS);
    free(args);
}

int overwrite(uint64_t load_count, uint64_t run_count, bool load_random,
              bool run_random, uint64_t cache_size_mb, uint32_t max_logs,
              uint32_t commit_freq, uint32_t wal_freq) 
{
    TEST_INIT();

    memleak_start();

    _reset_kvssd("/dev/nvme0n1", "forestKV");

    indexes = (int*) malloc(sizeof(int) * load_count);

    fdb_file_handle *fhandle;
    fdb_kvs_handle *kvhandle;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_status s;
    char keybuf[256], metabuf[256], bodybuf[256];
    const char *db_name = "./db_filename";

    (void)s;

    config = fdb_get_default_config_kvssd();
    config.kv_cache_size = cache_size_mb*.5*1024*1024;
    config.wal_threshold = wal_freq;
    config.block_reusing_threshold = 0;
    config.log_msg_level = 1;
    config.buffercache_size = cache_size_mb*.5*1024*1024;
    config.wal_flush_preloading = false;
    config.num_bgflusher_threads = 0;
    config.multi_kv_instances = false;
    config.wal_flush_before_commit = false;
    config.logging = true;
    config.index_blocksize = config.blocksize;
    config.max_log_threshold = ((config.blocksize - sizeof(struct bnode))/16)*1;
    config.split_threshold = config.blocksize;
    config.max_logs_per_node = max_logs;

    kvs_config = fdb_get_default_kvs_config();

    std::string key = "keybdce";
    std::string value = "valuevaluevalue";

    printf("overwrite\n");
    printf("load count : %lu KV pairs\n", load_count);
    printf("run count : %lu requests\n", run_count);
    printf("max_logs_per_node : %u\n", max_logs);
    printf("cache size MB : %lu\n", cache_size_mb);
    printf("commit : %u\n", commit_freq);
    printf("wal : %u\n", wal_freq);

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
    for (unsigned int i = 0; i < load_count; i++) {
        fdb_doc *doc;

        int idx;

        if(load_random) {
            idx = rand() % load_count;
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

        if(i % commit_freq == 0) {
            s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
            assert(s == FDB_RESULT_SUCCESS);
        }

        if(i % 10000 == 0) {
            printf("Done %d\n", i);
        }

        if(i % 250000 == 0) {
            fdb_print_stats();
        }
    }

    s = fdb_commit(fhandle, FDB_COMMIT_MANUAL_WAL_FLUSH);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_kvs_close(kvhandle);
    assert(s == FDB_RESULT_SUCCESS);
    s = fdb_close(fhandle);
    assert(s == FDB_RESULT_SUCCESS);

    gettimeofday(&tv2, NULL);
    double time = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
        (double) (tv2.tv_sec - tv1.tv_sec);
    printf("Insert TP %f ops/s\n", load_count / time);

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
    for (unsigned int i = 0; i < run_count; i++) {
        fdb_doc *doc;

        int idx;

        if(run_random) {
            idx = indexes[i % load_count];
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

        if(i % commit_freq == 0) {
            s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
            assert(s == FDB_RESULT_SUCCESS);
        }

        if(i % 10000 == 0) {
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
    printf("Overwrite TP %f ops/s\n", run_count / time);

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

int insert_then_read(uint64_t load_count, uint64_t run_count, bool load_random,
        bool run_random, uint64_t cache_size_mb, uint32_t max_logs,
        uint32_t commit_freq, uint32_t wal_freq) 
{
    TEST_INIT();

    memleak_start();

    _reset_kvssd("/dev/nvme0n1", "forestKV");

    indexes = (int*) malloc(sizeof(int) * load_count);
    lats = (double*) malloc(sizeof(double) * run_count);

    fdb_file_handle *fhandle;
    fdb_kvs_handle *kvhandle;
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_status s;
    char keybuf[256], metabuf[256], bodybuf[256];
    const char *db_name = "./db_filename";

    (void)s;

    config = fdb_get_default_config_kvssd();
    config.kv_cache_size = cache_size_mb*.5*1024*1024;
    config.wal_threshold = wal_freq;
    config.block_reusing_threshold = 0;
    config.log_msg_level = 1;
    config.buffercache_size = cache_size_mb*.5*1024*1024;
    config.wal_flush_preloading = false;
    config.num_bgflusher_threads = 0;
    config.multi_kv_instances = false;
    config.wal_flush_before_commit = false;
    config.logging = true;
    config.index_blocksize = config.blocksize;
    config.max_log_threshold = ((config.blocksize - sizeof(struct bnode))/16)*1;
    config.split_threshold = config.blocksize;
    config.max_logs_per_node = max_logs;
    config.num_aio_workers = 1;

    kvs_config = fdb_get_default_kvs_config();

    std::string key = "keybdce";
    std::string value = "valuevaluevalue";

    printf("insert_then_read\n");
    printf("load count : %lu KV pairs\n", load_count);
    printf("run count : %lu requests\n", run_count);
    printf("max_logs_per_node : %u\n", max_logs);
    printf("cache size MB : %lu\n", cache_size_mb);
    printf("commit freq : %u\n", commit_freq);
    printf("wal freq : %u\n", wal_freq);

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
    for (unsigned int i = 0; i < load_count; i++) {
        fdb_doc *doc;

        int idx;

        if(load_random) {
            idx = rand() % load_count;
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

        if(i % commit_freq == 0) {
            s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
            assert(s == FDB_RESULT_SUCCESS);
        }

        if(i % 250000 == 0) {
            fdb_print_stats();
        }
    }

    s = fdb_commit(fhandle, FDB_COMMIT_MANUAL_WAL_FLUSH);
    assert(s == FDB_RESULT_SUCCESS);

    gettimeofday(&tv2, NULL);
    double time = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
    (double) (tv2.tv_sec - tv1.tv_sec);
    printf("Insert TP %f ops/s\n", load_count / time);

    s = fdb_kvs_close(kvhandle);
    assert(s == FDB_RESULT_SUCCESS);
    s = fdb_close(fhandle);
    assert(s == FDB_RESULT_SUCCESS);
    s = fdb_shutdown();
    assert(s == FDB_RESULT_SUCCESS);

    config.buffercache_size = 8LU*1024*1024;

    s = fdb_open(&fhandle, db_name, &config);
    assert(s == FDB_RESULT_SUCCESS);
    fdb_kvs_open_default(fhandle, &kvhandle, &kvs_config);
    assert(s == FDB_RESULT_SUCCESS);

    int prev_ops = 0;
    srand(7812);

    struct _lat_args *args;
    fdb_reset_stats();

    gettimeofday(&tv1, NULL);

    gettimeofday(&tv1, NULL);
    gettimeofday(&tv2, NULL);
    double prev_time = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
        (double) (tv2.tv_sec - tv1.tv_sec);
    for (unsigned int i = 0; i < run_count; i++) {
        fdb_doc *doc;

        int idx;

        if(run_random) {
            idx = indexes[i % load_count];
        } else {
            idx = i;
        }

        args = (struct _lat_args*)malloc(sizeof(*args));
        args->idx = i;
        gettimeofday(&args->start, NULL);

        sprintf(keybuf, "%0*d", 8, idx);
        sprintf(metabuf, "meta%128d", idx);
        sprintf(bodybuf, "body%128d", idx);
        s = fdb_doc_create(&doc, (void*)keybuf, strlen(keybuf),
                       NULL, 0, NULL, 0);
        assert(s == FDB_RESULT_SUCCESS);

        if(i > run_count * .5) {
            s = fdb_get_async(kvhandle, doc, cb_fn_lat, args);
        } else {
            free(args);
            s = fdb_get_async(kvhandle, doc, cb_fn, NULL);
        }
        assert(s == FDB_RESULT_SUCCESS);

//        if(i % 10000 == 0) {
//            printf("Done %d\n", i);
//            fdb_print_stats();
//        }

        if(i % 250000 == 0) {
            fdb_print_stats();
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

    while(atomic_get_uint32_t(&count) < run_count) {}

    gettimeofday(&tv2, NULL);
    time = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
        (double) (tv2.tv_sec - tv1.tv_sec);
    printf("Read TP %f ops/s\n", (run_count) / time);

    qsort(lats, run_count, sizeof(double), compare); 

    printf("P50: %f P75: %f P90: %f P99: %f P999: %f\n",
           lats[(int) (run_count * .5)], lats[(int) (run_count * .75)],
           lats[(int) (run_count * .9)], lats[(int) (run_count * .99)],
           lats[(int) (run_count * .999)]);

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
int rw_mix(uint64_t load_count, uint64_t run_count, bool load_random,
        bool run_random, uint64_t cache_size_mb, uint32_t max_logs,
        uint32_t commit_freq, uint32_t wal_freq) 
{
    TEST_INIT();

    memleak_start();

    _reset_kvssd("/dev/nvme0n1", "forestKV");

    indexes = (int*) malloc(sizeof(int) * load_count);
    lats = (double*) malloc(sizeof(double) * run_count);

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
    config.kv_cache_size = cache_size_mb*.8*1024*1024;
    config.wal_threshold = wal_freq;
    config.block_reusing_threshold = 0;
    config.log_msg_level = 1;
    config.buffercache_size = cache_size_mb*.2*1024*1024;
    config.wal_flush_preloading = false;
    config.num_bgflusher_threads = 0;
    config.multi_kv_instances = false;
    config.wal_flush_before_commit = false;
    config.logging = true;
    config.index_blocksize = config.blocksize;
    config.max_log_threshold = ((config.blocksize - sizeof(struct bnode))/16)*1;
    config.split_threshold = config.blocksize;
    config.max_logs_per_node = max_logs;

    kvs_config = fdb_get_default_kvs_config();

    std::string key = "keybdce";
    std::string value = "valuevaluevalue";

    printf("rw_mix\n");
    printf("load count : %lu KV pairs\n", load_count);
    printf("run count : %lu requests\n", run_count);
    printf("max_logs_per_node : %u\n", max_logs);
    printf("cache size MB : %lu\n", cache_size_mb);
    printf("commit freq : %u\n", commit_freq);
    printf("wal freq : %u\n", wal_freq);

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
    for (unsigned int i = 0; i < load_count; i++) {
        fdb_doc *doc;

        int idx;

        if(load_random) {
            idx = rand() % load_count;
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

        if(i % commit_freq == 0) {
            s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
            assert(s == FDB_RESULT_SUCCESS);
        }

        if(i % 10000 == 0) {
            printf("Done %d\n", i);
        }

    }

    s = fdb_commit(fhandle, FDB_COMMIT_MANUAL_WAL_FLUSH);
    assert(s == FDB_RESULT_SUCCESS);

    //s = fdb_kvs_close(kvhandle);
    //assert(s == FDB_RESULT_SUCCESS);
    //s = fdb_close(fhandle);
    //assert(s == FDB_RESULT_SUCCESS);
    //s = fdb_shutdown();
    //assert(s == FDB_RESULT_SUCCESS);

    gettimeofday(&tv2, NULL);
    double time = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
        (double) (tv2.tv_sec - tv1.tv_sec);
    printf("Insert TP %f ops/s\n", load_count / time);

    //s = fdb_open(&fhandle, db_name, &config);
    //assert(s == FDB_RESULT_SUCCESS);
    //fdb_kvs_open_default(fhandle, &kvhandle, &kvs_config);
    //assert(s == FDB_RESULT_SUCCESS);

    uint32_t writes_done = 0;
    uint32_t reads_done = 0;

    double prev_time = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
        (double) (tv2.tv_sec - tv1.tv_sec);
    int prev_ops = 0;

    srand(7812);
    gettimeofday(&tv1, NULL);
    for (unsigned int i = 0; i < run_count; i++) {
        fdb_doc *doc;

        int idx;
        uint32_t op;

        if(run_random) {
            idx = indexes[i % load_count];
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

        if(writes_done % commit_freq == 0) {
            s = fdb_commit(fhandle, FDB_COMMIT_NORMAL);
            assert(s == FDB_RESULT_SUCCESS);
        }

        if(i % 10000 == 0) {
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

    while(atomic_get_uint32_t(&count) < reads_done) {}

    gettimeofday(&tv2, NULL);
    time = (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
        (double) (tv2.tv_sec - tv1.tv_sec);
    printf("RW TP %f ops/s\n", run_count / time);

    s = fdb_kvs_close(kvhandle);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_close(fhandle);
    assert(s == FDB_RESULT_SUCCESS);

    s = fdb_shutdown();

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

// Credit https://github.com/BLepers/KVell
void pin_me(int core) {
    cpu_set_t cpuset;
    pthread_t thread = pthread_self();

    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);

    int s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
    if (s != 0)
        assert(0);

}

//uint64_t load_count, uint64_t run_count, bool load_random,
//bool run_random, uint32_t cache_size_mb, uint32_t max_logs,
//uint32_t commit_freq, uint32_t wal_freq, char bench

int main(int argc, char *argv[])
{
    if(argc != 10) {
        exit(0);
    }

    uint64_t load_count = atoi(argv[1]);
    uint64_t run_count = atoi(argv[2]);
    bool load_random = atoi(argv[3]);
    bool run_random = atoi(argv[4]);
    uint32_t cache_size_mb = atoi(argv[5]);
    uint32_t max_logs = atoi(argv[6]);
    uint32_t commit_freq = atoi(argv[7]);
    uint32_t wal_freq = atoi(argv[8]);
    char bench = *argv[9];

    pin_me(0);
    
    if(bench == 'o') {
        overwrite(load_count, run_count, load_random, 
                run_random, cache_size_mb, max_logs,
                commit_freq, wal_freq);
    } else if(bench == 'r') {
        insert_then_read(load_count, run_count, load_random, 
                run_random, cache_size_mb, max_logs,
                commit_freq, wal_freq);
    } else if(bench == 'm') {
        rw_mix(load_count, run_count, load_random, 
                run_random, cache_size_mb, max_logs,
                commit_freq, wal_freq);
    }

    //insert_then_read(10000000, 10000000, true, true, 1024, 1);
    //insert_then_read(5000000, 5000000, true, true, 1024, 4);
    //insert_then_read(10000000, 10000000, true, true, 1024, 8);
    //insert_then_read(10000000, 10000000, true, true, 1024, 16);
    //insert_then_read(10000000, 10000000, true, true, 1024, 32);
    //insert_then_read(10000000, 10000000, true, true, 1024, 64);

    //insert_then_read(10000000, 10000000, true, true, 512, 8);
    //insert_then_read(10000000, 10000000, true, true, 256, 8);
    //insert_then_read(10000000, 10000000, true, true, 128, 8);
    //insert_then_read(10000000, 10000000, true, true, 64, 8);
    //insert_then_read(10000000, 10000000, true, true, 32, 8);
    //insert_then_read(10000000, 10000000, true, true, 16, 8);;

    //rw_mix(10000000, 10000000, true, true, 1024, 4);
    //rw_mix(10000000, 10000000, true, true, 1024, 4);
    //rw_mix(10000000, 10000000, true, true, 1024, 8);
    //rw_mix(10000000, 10000000, true, true, 1024, 16);
    //rw_mix(10000000, 10000000, true, true, 1024, 32);
    //rw_mix(10000000, 10000000, true, true, 1024, 64);
    //rw_mix();
}
