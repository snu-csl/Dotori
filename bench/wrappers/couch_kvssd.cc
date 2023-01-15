#include <assert.h>
#include <atomic>
#include <errno.h>
#include <fcntl.h>
#include <mutex>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>

#include "city.h"
#include "linux_nvme_ioctl.h"
#include "kv_nvme.h"
#include "libcouchstore/couch_db.h"

#include "core/auto_ptr.h"
#include "core/faster.h"
#include "device/null_disk.h"

bool deletes = false;
int retr_len = 16384;
#define METABUF_MAXLEN (256)
extern int64_t DATABUF_MAXLEN;

#define NUM_SHARDS 31
struct cache_entry {
    char* key;
    void* buf;
    uint32_t len;
};

std::unordered_map<uint64_t, struct cache_entry> caches[NUM_SHARDS];
std::mutex shard_locks[NUM_SHARDS];
uint64_t used[NUM_SHARDS];
uint64_t cache_size = 1024LU << 20;

uint64_t _search_cache(const void *id, void* buf) {
    return 0;
    uint64_t hash = CityHash64((const char*) id, 8);
    uint32_t shard = hash % NUM_SHARDS;
    struct cache_entry e;

    shard_locks[shard].lock();
    auto item = caches[shard].find(hash);
    if(item == caches[shard].end()) {
        shard_locks[shard].unlock();
        return 0;
    } else if(!memcmp(id, item->second.key, 8)) {
        auto len = item->second.len;
        memcpy(buf, item->second.buf, len);
        shard_locks[shard].unlock();
        return len;
    } else {
        return 0;
    }
}

void _evict(uint32_t shard, uint64_t len) {
    while(used[shard] + len > cache_size / NUM_SHARDS) {
        auto victim = caches[shard].begin();
        assert(victim != caches[shard].end());
        auto victim_len = victim->second.len;
        free(victim->second.buf);
        free(victim->second.key);
        caches[shard].erase(victim);
        used[shard] -= victim_len;
    }
}

void _evict(uint32_t shard, uint64_t hash, struct cache_entry victim) {
    free(victim.buf);
    free(victim.key);
    used[shard] -= victim.len;
    auto entry = caches[shard].find(hash);
    caches[shard].erase(entry);
}

void _add_to_cache(const void *id, void *buf, uint64_t len) {
    return;
    uint64_t hash = CityHash64((const char*) id, 8);
    uint32_t shard = hash % NUM_SHARDS;

    void* item;
    bool dupe = false;
    struct cache_entry e;

    shard_locks[shard].lock();
    auto found = caches[shard].find(hash);
    if(found == caches[shard].end()) {
        if(used[shard] + len > cache_size / NUM_SHARDS) {
            _evict(shard, len);
        }
        item = malloc(len);
    } else if(!memcmp(id, found->second.key, 8) && found->second.len == len) {
        dupe = true;
        item = found->second.buf;
    } else {
        _evict(shard, hash, found->second);
        item = malloc(len);
    }

    memcpy(item, buf, len);
    if(!dupe) {
        e.key = (char*) malloc(8);
        memcpy(e.key, id, 8);
        e.buf = item;
        e.len = len;
        caches[shard].insert({hash, e});
        used[shard] += len;
    }
    shard_locks[shard].unlock();
}

char *dev = (char*) "/dev/nvme1n1";
struct _db {
    int fd;
    int nsid;
    char* filename = (char*) "KVSSD";
};

couchstore_error_t couchstore_set_flags(uint64_t flags) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_idx_type(int type) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_bcache(uint64_t size) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_index_bsize(uint64_t size) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_cache_kv_pairs(bool _cache) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_cache(uint64_t size) {
    cache_size = size;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_compaction(int mode,
                                             size_t compact_thres,
                                             size_t block_reuse_thres) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_auto_compaction_threads(int num_threads) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_wal_size(size_t size) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_close_conn() {
    for(int i = 0; i < NUM_SHARDS; i++) {
        for(auto entry : caches[i]) {
            free(entry.second.key);
            free(entry.second.buf);
        }
        caches[i].clear();
    }

    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_chk_period(size_t seconds) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_compression(int opt) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_max_logs(uint32_t _max_logs) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_g_commit_wait(uint32_t _g_commit_wait) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_num_aio_workers(uint32_t num) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_max_outstanding(uint32_t max) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_retrieve_length(uint32_t _ret_len) {
    printf("Setting to %u\n", _ret_len);
    retr_len = _ret_len;
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_ycsbd(bool _d) {
    return COUCHSTORE_SUCCESS;
}
couchstore_error_t couchstore_set_deletes(bool _del) {
    deletes = _del;
    return COUCHSTORE_SUCCESS;
}

couchstore_error_t couchstore_open_conn(const char *filename) {
    for(int i = 0; i < NUM_SHARDS; i++) {
        used[i] = 0;
    }
    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_db(const char *filename,
                                      couchstore_open_flags flags,
                                      Db **pDb)
{
    return couchstore_open_db_ex(filename, flags,
                                 NULL, pDb);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_db_ex(const char *filename,
                                         couchstore_open_flags flags,
                                         FileOpsInterface *ops,
                                         Db **pDb)
{
    Db *ppdb;
    char *err;

    *pDb = (Db*)malloc(sizeof(Db));
    ppdb = *pDb;
    ppdb->filename = (char*)"KVSSD";

    ppdb->fd = open(dev, O_RDWR);
	if (ppdb->fd < 0) {
		printf("Failed to open device %s.\n", dev);
        exit(1);
	}

	ppdb->nsid = ioctl(ppdb->fd, NVME_IOCTL_ID);
	if (ppdb->nsid == (unsigned) -1) {
		printf("Failed to get nsid for %s.\n", dev);
        exit(1);
	}

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_close_db(Db *db)
{
    close(db->fd);
    free(db);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_db_info(Db *db, DbInfo* info)
{
    struct stat filestat;

    info->filename = "KVSSD";
    info->doc_count = 0;
    info->deleted_count = 0;
    info->header_position = 0;
    info->last_sequence = 0;

    //stat(db->filename, &filestat);
    info->space_used = 0;

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

    memcpy((uint8_t*)buf + offset, &docinfo->content_meta,
           sizeof(docinfo->content_meta));
    offset += sizeof(docinfo->content_meta);

    memcpy((uint8_t*)buf + offset, &docinfo->rev_meta.size,
           sizeof(docinfo->rev_meta.size));
    offset += sizeof(docinfo->rev_meta.size);

    if (docinfo->rev_meta.size > 0) {
        memcpy((uint8_t*)buf + offset, docinfo->rev_meta.buf, docinfo->rev_meta.size);
        offset += docinfo->rev_meta.size;
    }

    return offset;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_documents_rmw(Db *db, Doc* const docs[], 
                                                 DocInfo *infos[],
        unsigned numdocs, couchstore_save_options options)
{
    unsigned i;
    uint8_t *buf;
    void *value = malloc(retr_len);
    int valuelen;
    uint32_t idx;
    int ret;

    for (i=0;i<numdocs;++i){
        if(!(valuelen = _search_cache(docs[i]->id.buf, value))) {
            ret = nvme_kv_retrieve(0, db->fd, db->nsid, 
                    (char*)docs[i]->id.buf, docs[i]->id.size,
                    (const char*)value, &valuelen,
                    0, RETRIEVE_OPTION_NOTHING);
            if(ret) {
                printf("Retrieve failed in rmw for key %s\n", (char*)docs[i]->id.buf);
                return COUCHSTORE_ERROR_DOC_NOT_FOUND;
            }
        }

        /*
         * Modify something in the value
         */

        idx = rand() % valuelen;
        ((char*) value)[idx] = 'A';

        ret = nvme_kv_store(0, db->fd, db->nsid, 
                            docs[i]->id.buf, docs[i]->id.size,
                            (char*)value, valuelen,
                            0, STORE_OPTION_NOTHING);
        if(ret) {
            printf("Store failed in rmw for key %s\n", (char*)docs[i]->id.buf);
            return COUCHSTORE_ERROR_DOC_NOT_FOUND;
        }
        _add_to_cache(docs[i]->id.buf, value, valuelen);

        infos[i]->db_seq = 0;
    }
    free(value);
    free(buf);

    return COUCHSTORE_SUCCESS;
}


LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_document_rmw(Db *db, const Doc *doc, DocInfo *info,
        couchstore_save_options options)
{
    return couchstore_save_documents_rmw(db, (Doc**)&doc, (DocInfo**)&info, 1, options);
}

std::string gen_random(const int len) {
    static const char alphanum[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::string tmp_s;
    tmp_s.reserve(len);

    for (int i = 0; i < len; ++i) {
        tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    return tmp_s;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_documents(Db *db, Doc* const docs[], DocInfo *infos[],
        unsigned numdocs, couchstore_save_options options)
{
    unsigned i;
    uint8_t *buf;
    int ret;
    buf = (uint8_t*) malloc(retr_len);

    for (i=0;i<numdocs;++i){
        if(deletes && rand() % 100 < 5) {
            ret = nvme_kv_delete(0, db->fd, db->nsid, 
                    docs[i]->id.buf, docs[i]->id.size, DELETE_OPTION_NOTHING);
            if(ret) {
                printf("Delete failed for key %s\n", docs[i]->id.buf);
                exit(1);
            }
        } else {
            memcpy(buf, docs[i]->data.buf, docs[i]->data.size);

            _add_to_cache(docs[i]->id.buf, buf, docs[i]->data.size);
            ret = nvme_kv_store(0, db->fd, db->nsid, 
                    docs[i]->id.buf, docs[i]->id.size,
                    (char*)buf, docs[i]->data.size,
                    0, STORE_OPTION_NOTHING);
            if(ret) {
                printf("Store failed for key %s\n", docs[i]->id.buf);
                exit(1);
            }
        }
        infos[i]->db_seq = 0;
    }
    free(buf);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_document(Db *db, const Doc *doc, DocInfo *info,
        couchstore_save_options options)
{
    return couchstore_save_documents(db, (Doc**)&doc, (DocInfo**)&info, 1, options);
}

void _buf_to_docinfo(void *buf, size_t size, DocInfo *docinfo)
{
    size_t offset = 0;

    memcpy(&docinfo->rev_seq, (uint8_t*)buf + offset, sizeof(docinfo->rev_seq));
    offset += sizeof(docinfo->rev_seq);

    memcpy(&docinfo->deleted, (uint8_t*)buf + offset, sizeof(docinfo->deleted));
    offset += sizeof(docinfo->deleted);

    memcpy(&docinfo->content_meta, (uint8_t*)buf + offset,
           sizeof(docinfo->content_meta));
    offset += sizeof(docinfo->content_meta);

    memcpy(&docinfo->rev_meta.size, (uint8_t*)buf + offset,
           sizeof(docinfo->rev_meta.size));
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
    assert(0);
    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_docinfos_by_id(Db *db, const sized_buf ids[], unsigned numDocs,
        couchstore_docinfos_options options, couchstore_changes_callback_fn callback, void *ctx)
{
    assert(0);
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
    // do nothing

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_document(Db *db,
                                            const void *id,
                                            size_t idlen,
                                            Doc **pDoc,
                                            couchstore_open_options options)
{
    char *err = NULL;
    void *value = malloc(retr_len);
    int valuelen = retr_len;
    int ret;

    if(!(valuelen = _search_cache(id, value))) {
        valuelen = retr_len;
        ret = nvme_kv_retrieve(0, db->fd, db->nsid, 
                (const char*)id, idlen,
                (const char*)value, &valuelen,
                0, RETRIEVE_OPTION_NOTHING);
        if(ret) {
            if(!deletes) {
                printf("Retrieve failed for key %s (%d) %s\n", (char*)id, ret, strerror(ret));
                assert(0);
            }
        } else {
            _add_to_cache(id, value, valuelen);
        }
    }

    *pDoc = (Doc *)malloc(sizeof(Doc));
    (*pDoc)->id.buf = (char*)id;
    (*pDoc)->id.size = idlen;
    (*pDoc)->data.buf = (char*)value;
    (*pDoc)->data.size = valuelen;

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_walk_id_tree(Db *db,
                                           const sized_buf* startDocID,
                                           couchstore_docinfos_options options,
                                           couchstore_walk_tree_callback_fn callback,
                                           void *ctx)
{
    assert(0);
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
    //free(docinfo->rev_meta.buf);
    free(docinfo);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_commit(Db *db)
{
    // do nothing (automatically performed at the end of each write batch)

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_compact_db_ex(Db* source, const char* target_filename,
        uint64_t flags, FileOpsInterface *ops)
{
    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_compact_db(Db* source, const char* target_filename)
{
    return couchstore_compact_db_ex(source, target_filename, 0x0, NULL);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_document_async(Db *db,
                                                  const void *id,
                                                  size_t idlen,
                                                  Doc **pDoc,
                                                  couchstore_open_options options,
                                                  void* lat_args)
{
    assert(0);
    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_walk_id_tree_async(Db *db,
                                                 const sized_buf* startDocID,
                                                 couchstore_docinfos_options options,
                                                 couchstore_walk_tree_callback_fn callback,
                                                 void *ctx)
{
    assert(0);
    return COUCHSTORE_SUCCESS;
}
