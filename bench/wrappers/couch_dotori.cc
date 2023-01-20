#include <deque>
#include <mutex>
#include <thread>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include <atomic>

#include "arch.h"
#include "libforestdb/forestdb.h"
#include "libcouchstore/couch_db.h"
#include "stopwatch.h"

#include "memleak.h"

#define META_BUF_MAXLEN (256)
#define SEQNUM_NOT_USED (0xffffffffffffffff)
#define MAX_KEYLEN (4096)
extern int64_t DATABUF_MAXLEN;

struct _db {
    fdb_file_handle *dbfile;
    fdb_kvs_handle *fdb;
    char *filename;
};

static uint64_t config_flags = 0x0;
static uint64_t kv_cache_size = 8192LU << 20;
static uint64_t bcache_size = 12288LU << 20;
static uint64_t index_bsize = 4096;
static int c_auto = 0;
static size_t c_threshold = 83;
static size_t br_threshold = 65;
static size_t wal_size = 4096;
static size_t c_period = 0;
static int compression = 0;
static int indexing_type = 0;
static int auto_compaction_threads = 0;
static int max_logs = 4;
static int retrieve_length = 4096;
static int num_aio_workers = 3;
static int max_outstanding = 24;
static int g_commit_wait = 50;
static bool cache_kv_pairs = true;
static bool deletes = false;

couchstore_error_t couchstore_set_flags(uint64_t flags) {
    config_flags = flags;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_idx_type(int type) {
    indexing_type = type;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_cache(uint64_t size) {
    kv_cache_size = size;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_bcache(uint64_t size) {
    bcache_size = size;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_index_bsize(uint64_t size) {
    index_bsize = size;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_cache_kv_pairs(bool _cache) {
    cache_kv_pairs = _cache;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_compaction(int mode,
                                             size_t compact_thres,
                                             size_t block_reuse_thres) {
    c_auto = mode;
    c_threshold = compact_thres;
    br_threshold = block_reuse_thres;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_auto_compaction_threads(int num_threads) {
    auto_compaction_threads = num_threads;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_wal_size(size_t size) {
    wal_size = size;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_close_conn() {
    fdb_shutdown();
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_chk_period(size_t seconds) {
    c_period = seconds;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_compression(int opt) {
    compression = opt;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_max_logs(uint32_t _max_logs) {
    max_logs = _max_logs;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_retrieve_length(uint32_t _ret_len) {
    retrieve_length = _ret_len;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_num_aio_workers(uint32_t num) {
    num_aio_workers = num;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_max_outstanding(uint32_t max) {
    max_outstanding = max;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_g_commit_wait(uint32_t _g_commit_wait) {
    g_commit_wait = _g_commit_wait;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_ycsbd(bool _d) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_deletes(bool _del) {
    deletes = _del;
    return COUCHSTORE_SUCCESS;
}

void logCallbackFunc(int err_code,
                     const char *err_msg,
                     void *pCtxData) {
    fprintf(stderr, "%s - error code: %d, error message: %s\n",
            (char *) pCtxData, err_code, err_msg);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_db(const char *filename,
                                      couchstore_open_flags flags,
                                      Db **pDb)
{
    return couchstore_open_db_ex(filename, flags,
                                 NULL, pDb);
}

bool closed = false;
LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_db_ex(const char *filename,
                                         couchstore_open_flags flags,
                                         FileOpsInterface *ops,
                                         Db **pDb)
{
    fdb_config config;
    fdb_kvs_config kvs_config;
    fdb_status status;
    fdb_file_handle *dbfile;
    fdb_kvs_handle *fdb;
    char *fname = (char *)filename;

    closed = false;

    memset(&config, 0, sizeof(fdb_config));
    config = fdb_get_default_config_kvssd();
    config.kvssd = true;
    config.kv_cache_doc_writes = cache_kv_pairs;
    if (c_auto) {
        config.compaction_mode = FDB_COMPACTION_AUTO;
        config.deletion_interval = c_period;
    } else {
        config.compaction_mode = FDB_COMPACTION_MANUAL;
    }
    config.block_reusing_threshold = 0;
    config.num_compactor_threads = 1;
    config.logging = true;
    config.num_aio_workers = 3;
    config.max_outstanding = 24;
    config.max_logs_per_node = max_logs;
    config.kvssd_retrieve_length = retrieve_length;
    config.blocksize = 4096;
    config.index_blocksize = index_bsize;
    config.chunksize = sizeof(uint64_t);
    config.kv_cache_size = kv_cache_size;
    config.buffercache_size = bcache_size;
    config.wal_threshold = wal_size;
    config.num_wal_partitions = 31;
    config.num_bcache_partitions = 31;
    config.seqtree_opt = FDB_SEQTREE_NOT_USE;
    config.num_bgflusher_threads = 0;
    if (flags & 0x10) {
        config.durability_opt = FDB_DRB_NONE;
    } else {
        config.durability_opt = FDB_DRB_ASYNC;
    }
    config.compress_document_body = (compression)?true:false;
    config.wal_flush_before_commit = false;
    config.auto_commit = false;

    config.prefetch_duration = 0;
    config.multi_kv_instances = false;

    kvs_config = fdb_get_default_kvs_config();

    *pDb = (Db*)calloc(1, sizeof(Db));
    (*pDb)->filename = (char *)malloc(strlen(filename)+1);
    strcpy((*pDb)->filename, filename);

    if (indexing_type == 1) {
        assert(0);
        // naive B+tree
        //char *kvs_names[] = {(char*)"default"};
        //fdb_custom_cmp_variable functions[] = {_bench_keycmp};
        //config.multi_kv_instances = true;
        //status = fdb_open_custom_cmp(&dbfile, fname, &config,
        //                             1, kvs_names, functions);
    } else {
        status = fdb_open(&dbfile, fname, &config);
        assert(status == FDB_RESULT_SUCCESS);
    }
    status = fdb_kvs_open_default(dbfile, &fdb, &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);

    (*pDb)->dbfile = dbfile;
    (*pDb)->fdb = fdb;

    if (status == FDB_RESULT_SUCCESS) {
        return COUCHSTORE_SUCCESS;
    } else {
        free((*pDb)->filename);
        free(*pDb);
        return COUCHSTORE_ERROR_OPEN_FILE;
    }
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_close_db(Db *db)
{
    fdb_commit(db->dbfile, FDB_COMMIT_MANUAL_WAL_FLUSH);
    fdb_close(db->dbfile);
    fdb_shutdown();

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_db_info(Db *db, DbInfo* info)
{
    fdb_file_info fdb_info;

    fdb_get_file_info(db->dbfile, &fdb_info);

    info->file_size = fdb_info.file_size;
    info->space_used = fdb_info.space_used;
    info->filename = fdb_info.filename;
    info->app_wa = fdb_info.app_write_amp;

    return COUCHSTORE_SUCCESS;
}

size_t _docinfo_to_buf(DocInfo *docinfo, void *buf)
{
    // [db_seq,] rev_seq, deleted, content_meta, rev_meta (size), rev_meta (buf)
    size_t offset = 0;

    memcpy((uint8_t*)buf + offset, &docinfo->rev_seq, sizeof(docinfo->rev_seq));
    offset += sizeof(docinfo->rev_seq);

    memcpy((uint8_t*)buf + offset, &docinfo->deleted, sizeof(docinfo->deleted));
    offset += sizeof(docinfo->deleted);

    memcpy((uint8_t*)buf + offset, &docinfo->content_meta, sizeof(docinfo->content_meta));
    offset += sizeof(docinfo->content_meta);

    memcpy((uint8_t*)buf + offset, &docinfo->rev_meta.size, sizeof(docinfo->rev_meta.size));
    offset += sizeof(docinfo->rev_meta.size);

    if (docinfo->rev_meta.size > 0) {
        memcpy((uint8_t*)buf + offset, docinfo->rev_meta.buf, docinfo->rev_meta.size);
        offset += docinfo->rev_meta.size;
    }

    return offset;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_documents(Db *db, Doc* const docs[], DocInfo *infos[],
        unsigned numdocs, couchstore_save_options options)
{
    unsigned i;
    fdb_doc _doc;
    fdb_status status = FDB_RESULT_SUCCESS;
    uint8_t buf[DATABUF_MAXLEN];

    memset(&_doc, 0, sizeof(_doc));
    for (i=0;i<numdocs;++i){
        _doc.key = docs[i]->id.buf;
        _doc.keylen = docs[i]->id.size;
        _doc.body = docs[i]->data.buf;
        _doc.bodylen = docs[i]->data.size;
        _doc.metalen = _docinfo_to_buf(infos[i], buf);
        _doc.meta = buf;
        _doc.metalen = 0;
        _doc.meta = NULL;
        _doc.deleted = 0;

        status = fdb_set(db->fdb, &_doc);
        memcpy(buf, docs[i]->data.buf, docs[i]->data.size);
        assert(status == FDB_RESULT_SUCCESS);
        infos[i]->db_seq = _doc.seqnum;
        infos[i]->bp = _doc.offset;
    }

    if (status == FDB_RESULT_SUCCESS)
        return COUCHSTORE_SUCCESS;
    else
        return COUCHSTORE_ERROR_ALLOC_FAIL;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_document(Db *db, const Doc *doc, DocInfo *info,
        couchstore_save_options options)
{
    return couchstore_save_documents(db, (Doc**)&doc, (DocInfo**)&info, 1, options);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_documents_rmw(Db *db, Doc* const docs[], 
                                                 DocInfo *infos[],
        unsigned numdocs, couchstore_save_options options)
{
    unsigned i;
    fdb_doc _doc;
    fdb_status status = FDB_RESULT_SUCCESS;
    uint8_t buf[META_BUF_MAXLEN];
    int idx;

    memset(&_doc, 0, sizeof(_doc));
    for (i=0;i<numdocs;++i){
        memset(&_doc, 0, sizeof(_doc));
        _doc.key = (void *)docs[i]->id.buf;
        _doc.keylen = docs[i]->id.size;
        _doc.seqnum = SEQNUM_NOT_USED;
        _doc.meta = _doc.body = NULL;

        status = fdb_get(db->fdb, &_doc);
        if (status != FDB_RESULT_SUCCESS) {
            assert(0);
            return COUCHSTORE_ERROR_DOC_NOT_FOUND;
        }

        idx = rand() % _doc.bodylen;
        ((char*) _doc.body)[idx] = 'A';

        status = fdb_set(db->fdb, &_doc);
        if (status != FDB_RESULT_SUCCESS) {
            assert(0);
            return COUCHSTORE_ERROR_ALLOC_FAIL;
        }

        free(_doc.body);
    }

    if (status == FDB_RESULT_SUCCESS)
        return COUCHSTORE_SUCCESS;
    else
        return COUCHSTORE_ERROR_ALLOC_FAIL;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_document_rmw(Db *db, const Doc *doc, DocInfo *info,
        couchstore_save_options options)
{
    return couchstore_save_documents_rmw(db, (Doc**)&doc, (DocInfo**)&info, 1, options);
}


void _buf_to_docinfo(void *buf, size_t size, DocInfo *docinfo)
{
    size_t offset = 0;

    memcpy(&docinfo->rev_seq, (uint8_t*)buf + offset, sizeof(docinfo->rev_seq));
    offset += sizeof(docinfo->rev_seq);

    memcpy(&docinfo->deleted, (uint8_t*)buf + offset, sizeof(docinfo->deleted));
    offset += sizeof(docinfo->deleted);

    memcpy(&docinfo->content_meta, (uint8_t*)buf + offset, sizeof(docinfo->content_meta));
    offset += sizeof(docinfo->content_meta);

    memcpy(&docinfo->rev_meta.size, (uint8_t*)buf + offset, sizeof(docinfo->rev_meta.size));
    offset += sizeof(docinfo->rev_meta.size);

    if (docinfo->rev_meta.size > 0) {
        //docinfo->rev_meta.buf = (char *)malloc(docinfo->rev_meta.size);
        docinfo->rev_meta.buf = ((char *)docinfo) + sizeof(DocInfo);
        memcpy(docinfo->rev_meta.buf, (uint8_t*)buf + offset, docinfo->rev_meta.size);
        offset += docinfo->rev_meta.size;
    }else{
        docinfo->rev_meta.buf = NULL;
    }
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_docinfo_by_id(Db *db, const void *id, size_t idlen, DocInfo **pInfo)
{
    fdb_doc _doc;
    fdb_status status; (void)status;
    size_t rev_meta_size;
    size_t meta_offset;

    meta_offset = sizeof(uint64_t)*1 + sizeof(int) + sizeof(couchstore_content_meta_flags);

    _doc.key = (void *)id;
    _doc.keylen = idlen;
    _doc.seqnum = SEQNUM_NOT_USED;
    _doc.meta = _doc.body = NULL;

    status = fdb_get_metaonly(db->fdb, &_doc);
    memcpy(&rev_meta_size, (uint8_t*)_doc.meta + meta_offset, sizeof(size_t));

    *pInfo = (DocInfo *)malloc(sizeof(DocInfo) + rev_meta_size);
    (*pInfo)->id.buf = (char *)id;
    (*pInfo)->id.size = idlen;
    (*pInfo)->size = _doc.bodylen;
    (*pInfo)->bp = _doc.offset;
    (*pInfo)->db_seq = _doc.seqnum;
    _buf_to_docinfo(_doc.meta, _doc.metalen, (*pInfo));

    free(_doc.meta);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_docinfos_by_id(Db *db, const sized_buf ids[], unsigned numDocs,
        couchstore_docinfos_options options, couchstore_changes_callback_fn callback, void *ctx)
{
    size_t i;
    fdb_doc _doc;
    fdb_status status;
    DocInfo *docinfo;
    size_t rev_meta_size, max_meta_size = 256;
    size_t meta_offset;

    (void)status;
    meta_offset = sizeof(uint64_t)*1 + sizeof(int) + sizeof(couchstore_content_meta_flags);

    docinfo = (DocInfo*)malloc(sizeof(DocInfo) + max_meta_size);

    for (i=0;i<numDocs;++i){
        _doc.key = (void*)ids[i].buf;
        _doc.keylen = ids[i].size;
        _doc.seqnum = SEQNUM_NOT_USED;
        _doc.meta = _doc.body = NULL;

        status = fdb_get_metaonly(db->fdb, &_doc);
        assert(status == FDB_RESULT_SUCCESS);

        memcpy(&rev_meta_size, (uint8_t*)_doc.meta + meta_offset, sizeof(size_t));
        if (rev_meta_size > max_meta_size) {
            max_meta_size = rev_meta_size;
            docinfo = (DocInfo*)realloc(docinfo, sizeof(DocInfo) + max_meta_size);
        }

        memset(docinfo, 0, sizeof(DocInfo));
        docinfo->id.buf = ids[i].buf;
        docinfo->id.size = ids[i].size;
        docinfo->size = _doc.bodylen;
        docinfo->bp = _doc.offset;
        docinfo->db_seq = _doc.seqnum;
        _buf_to_docinfo(_doc.meta, _doc.metalen, docinfo);
        free(_doc.meta);

        callback(db, docinfo, ctx);
    }

    free(docinfo);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_docinfos_by_sequence(Db *db,
                                                   const uint64_t sequence[],
                                                   unsigned numDocs,
                                                   couchstore_docinfos_options options,
                                                   couchstore_changes_callback_fn callback,
                                                   void *ctx)
{
    size_t i;
    fdb_doc _doc;
    fdb_status status;
    DocInfo *docinfo;
    size_t rev_meta_size, max_meta_size = 256;
    size_t meta_offset;
    uint8_t keybuf[MAX_KEYLEN];

    (void)status;
    meta_offset = sizeof(uint64_t)*1 + sizeof(int) + sizeof(couchstore_content_meta_flags);

    docinfo = (DocInfo*)malloc(sizeof(DocInfo) + max_meta_size);

    for (i=0;i<numDocs;++i){
        _doc.key = (void*)keybuf;
        _doc.seqnum = sequence[i];
        _doc.meta = _doc.body = NULL;

        status = fdb_get_metaonly_byseq(db->fdb, &_doc);
        assert(status == FDB_RESULT_SUCCESS);

        memcpy(&rev_meta_size, (uint8_t*)_doc.meta + meta_offset, sizeof(size_t));
        if (rev_meta_size > max_meta_size) {
            max_meta_size = rev_meta_size;
            docinfo = (DocInfo*)realloc(docinfo, sizeof(DocInfo) + max_meta_size);
        }

        memset(docinfo, 0, sizeof(DocInfo));
        docinfo->id.buf = (char *)keybuf;
        docinfo->id.size = _doc.keylen;
        docinfo->size = _doc.bodylen;
        docinfo->bp = _doc.offset;
        docinfo->db_seq = _doc.seqnum;
        _buf_to_docinfo(_doc.meta, _doc.metalen, docinfo);
        free(_doc.meta);

        callback(db, docinfo, ctx);
    }

    free(docinfo);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_document(Db *db,
                                            const void *id,
                                            size_t idlen,
                                            Doc **pDoc,
                                            couchstore_open_options options)
{
    fdb_doc _doc;
    fdb_status status;
    couchstore_error_t ret = COUCHSTORE_SUCCESS;

    memset(&_doc, 0, sizeof(_doc));
    _doc.key = (void *)id;
    _doc.keylen = idlen;
    _doc.seqnum = SEQNUM_NOT_USED;
    _doc.meta = _doc.body = NULL;

    status = fdb_get(db->fdb, &_doc);
    if (status != FDB_RESULT_SUCCESS) {
        printf("\nget error %.*s\n", (int)idlen, (char*)id);
        fflush(stdout);
        ret = COUCHSTORE_ERROR_DOC_NOT_FOUND;
        assert(0);
    }
    //assert(status == FDB_RESULT_SUCCESS);

    *pDoc = (Doc *)malloc(sizeof(Doc));
    (*pDoc)->id.buf = (char*)_doc.key;
    (*pDoc)->id.size = _doc.keylen;
    (*pDoc)->data.buf = (char*)_doc.body;
    (*pDoc)->data.size = _doc.bodylen;

    free(_doc.meta);

    return ret;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_walk_id_tree(Db *db,
                                           const sized_buf* startDocID,
                                           couchstore_docinfos_options options,
                                           couchstore_walk_tree_callback_fn callback,
                                           void *ctx)
{
    int c_ret = 0;
    fdb_iterator *fit = NULL;
    fdb_status fs;
    fdb_doc *doc;
    DocInfo doc_info;

    fs = fdb_iterator_init(db->fdb, &fit, startDocID->buf, startDocID->size, NULL, 0, 0x0);
    if (fs != FDB_RESULT_SUCCESS) {
        return COUCHSTORE_ERROR_DOC_NOT_FOUND;
    }

    do {
        doc = NULL;
        fs = fdb_iterator_get(fit, &doc);
        if (fs == FDB_RESULT_SUCCESS) {
            doc_info.id.buf = (char *)doc->key;
            doc_info.id.size = doc->keylen;
            c_ret = callback(db, 0, &doc_info, 0, NULL, ctx);
            fs = fdb_doc_free(doc);
            if (c_ret != 0) {
                break;
            }
        } else {
            break;
        }
    } while (fdb_iterator_next(fit) != FDB_RESULT_ITERATOR_FAIL);

    fs = fdb_iterator_close(fit);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
void couchstore_free_document(Doc *doc)
{
    if (doc->id.buf) free(doc->id.buf);
    if (doc->data.buf) free(doc->data.buf);
    free(doc);
}

LIBCOUCHSTORE_API
void couchstore_free_docinfo(DocInfo *docinfo)
{
    free(docinfo);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_commit(Db *db)
{
    fdb_commit(db->dbfile, FDB_COMMIT_NORMAL);
    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_compact_db_ex(Db* source, const char* target_filename,
        uint64_t flags, FileOpsInterface *ops)
{
    char *new_filename = (char *)target_filename;
    fdb_compact(source->dbfile, new_filename);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_compact_db(Db* source, const char* target_filename)
{
    return couchstore_compact_db_ex(source, target_filename, 0x0, NULL);
}

