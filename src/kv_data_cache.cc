#if !defined(WIN32) && !defined(_WIN32)
#include <sys/time.h>
#endif

#include "atomic.h"
#include "fdb_internal.h"
#include "kv_data_cache.h"
#include "time_utils.h"
#include "limits.h"

// void* mempool;
// uint64_t mempool_idx;

static uint64_t kvcache_size;
static size_t kvcache_flush_unit;

atomic_uint64_t kvcache_occupied_space;

static kvcache_config kvcache_global_config;
static uint32_t kvcache_blocksize;

struct kvcache_shard {
    spin_t lock;
    // list for clean blocks
//    struct list cleanlist;
    // tree for dirty pairs
    struct avl_tree tree;
    // tree for index nodes
//    struct avl_tree tree_idx;
    // hash table for block lookup
    struct hash hashtable;
    // list elem for shard LRU
    struct list_elem le;
    struct list kvpair_lru;
    struct list dirty_pairs;
};

static spin_t space_lock;

static const size_t MAX_VICTIM_SELECTIONS = 5;

// file structure list
static size_t num_files;
static size_t file_array_capacity;
static fnamedic_item ** file_list;
static struct list file_zombies;
static fdb_rw_lock filelist_lock; // Reader-Writer lock for the file list.

// hash table for filename
static struct hash fnamedic;

// main hash table for cache
struct kv_pair *cache = NULL;

//struct list *lru;
static spin_t lru_lock;

static spin_t kvcache_lock;

struct fnamedic_item {
    char *filename;
    uint16_t filename_len;
    uint32_t hash;

    // current opened filemgr instance
    // (can be changed on-the-fly when file is closed and re-opened)
    struct filemgr *curfile;

    // Shards of the block cache for a file.
    struct kvcache_shard *shards;

    // list elem for FILE_ZOMBIE
    struct list_elem le;
    // hash elem for FNAMEDIC
    struct hash_elem hash_elem;

    atomic_uint32_t ref_count;
    atomic_uint64_t nvictim;
    atomic_uint64_t nitems;
    atomic_uint64_t nimmutable;
    atomic_uint64_t access_timestamp;
    size_t num_shards;
    
//    struct kv_pair *dirty_pairs;
};

#define KVCACHE_DIRTY (0x1)
#define KVCACHE_CLEAN (0x4)

INLINE struct kv_pair *_lru_get_oldest(struct list *lru)
{
    struct list_elem *elem;
    struct kv_pair *pair;
    
    elem = list_pop_back(lru);
    
    if(elem) {
        pair = _get_entry(elem, struct kv_pair, elem);
        return pair;
    }
    
    return NULL;
}

INLINE struct kv_pair *_lru_peek_oldest(struct list *lru)
{
    struct list_elem *elem;
    struct kv_pair *pair;
    
    elem = list_end(lru);
    
    if(elem) {
        pair = _get_entry(elem, struct kv_pair, elem);
        return pair;
    }
    
    return NULL;
}

INLINE void _lru_bump(struct list *lru, struct list_elem *elem)
{
    list_remove(lru, elem);
    list_push_front(lru, elem);
}

INLINE void _lru_remove(struct list *lru, struct list_elem *elem)
{
    list_remove(lru, elem);
}

INLINE void _lru_add(struct list *lru, struct list_elem *elem)
{
    list_push_front(lru, elem);
}

struct dirty_item {
    struct kv_pair *pair;
    struct avl_node avl;
};

INLINE int _dirty_cmp(struct avl_node *a, struct avl_node *b, void *aux)
{
    struct dirty_item *aa, *bb;
    aa = _get_entry(a, struct dirty_item, avl);
    bb = _get_entry(b, struct dirty_item, avl);

    #ifdef __BIT_CMP
        return _CMP_U64(aa->pair->vernum , bb->pair->vernum);

    #else
        if (aa->pair->vernum < bb->pair->vernum) return -1;
        else if (aa->pair->vernum > bb->pair->vernum) return 1;
        else return 0;

    #endif
}

INLINE uint32_t _fname_hash(struct hash *hash, struct hash_elem *e)
{
    struct fnamedic_item *item;
    item = _get_entry(e, struct fnamedic_item, hash_elem);
    return item->hash % ((unsigned)(KVCACHE_NDICBUCKET));
}

INLINE int _kvcache_fname_cmp(struct hash_elem *a, struct hash_elem *b)
{
    size_t len;
    struct fnamedic_item *aa, *bb;
    aa = _get_entry(a, struct fnamedic_item, hash_elem);
    bb = _get_entry(b, struct fnamedic_item, hash_elem);

    if (aa->filename_len == bb->filename_len) {
        return memcmp(aa->filename, bb->filename, aa->filename_len);
    }else {
        len = MIN(aa->filename_len , bb->filename_len);
        int cmp = memcmp(aa->filename, bb->filename, len);
        if (cmp != 0) return cmp;
        else {
            return (int)((int)aa->filename_len - (int)bb->filename_len);
        }
    }
}

static struct fnamedic_item *_next_dead_fname_zombie(void) {
    struct list_elem *e;
    struct fnamedic_item *fname_item;
    bool found = false;
    if (writer_lock(&filelist_lock) == 0) {
        e = list_begin(&file_zombies);
        while (e) {
            fname_item = _get_entry(e, struct fnamedic_item, le);
            if (atomic_get_uint32_t(&fname_item->ref_count) == 0) {
                list_remove(&file_zombies, e);
                found = true;
                break;
            } else {
                e = list_next(e);
            }
        }
        writer_unlock(&filelist_lock);
    } else {
        fprintf(stderr, "Error in _next_dead_fname_zombie(): "
                        "Failed to acquire WriterLock on filelist_lock!\n");
    }
    return found ? fname_item : NULL;
}

static bool _file_empty(struct fnamedic_item *fname) {
    return atomic_get_uint64_t(&kvcache_occupied_space) == 0 ? true : false;
}

void _clear_kvcache(struct fnamedic_item *item)
{
    // struct kv_pair *pair, *tmp;
    struct kv_pair *pair;
    for (unsigned int i = 0; i < item->num_shards; i++) {
        pair = _lru_get_oldest(&item->shards[i].kvpair_lru);
        while(pair != NULL) {
            free(pair->value);
            free(pair);
            pair = _lru_get_oldest(&item->shards[i].kvpair_lru);
        }

        pair = _lru_get_oldest(&item->shards[i].dirty_pairs);
        while(pair != NULL) {
            free(pair->value);
            free(pair);
            pair = _lru_get_oldest(&item->shards[i].dirty_pairs);
        }
    }
}

static void _fname_free(struct fnamedic_item *fname)
{
    // file must be empty
    if (!_file_empty(fname)) {
        DBG("Warning: failed to free fnamedic_item instance for a file '%s' "
            "because the fnamedic_item instance is not empty!\n",
            fname->filename);
        return;
    }
    uint32_t ref_count = atomic_get_uint32_t(&fname->ref_count);
    if (ref_count != 0) {
        DBG("Warning: failed to free fnamedic_item instance for a file '%s' "
            "because its ref counter is not zero!\n",
            fname->filename);
        return;
    }

    // free hash
    _clear_kvcache(fname);

    // free hash
    size_t i = 0;
    for (; i < fname->num_shards; ++i) {
        hash_free(&fname->shards[i].hashtable);
        spin_destroy(&fname->shards[i].lock);
    }

    free(fname->shards);
    free(fname->filename);
    free(fname);
}

static void _garbage_collect_zombie_fnames(void) {
    struct fnamedic_item *fname_item = _next_dead_fname_zombie();
    while (fname_item) {
        _fname_free(fname_item);
        fname_item = _next_dead_fname_zombie();
    }
}

INLINE int _kvcache_cmp(struct hash_elem *a, struct hash_elem *b)
{
    struct kv_pair *aa, *bb;
    aa = _get_entry(a, struct kv_pair, hash_elem);
    bb = _get_entry(b, struct kv_pair, hash_elem);

    #ifdef __BIT_CMP

        return _CMP_U64(aa->vernum, bb->vernum);

    #else

        if (aa->vernum == bb->vernum) return 0;
        else if (aa->vernum < bb->vernum) return -1;
        else return 1;

    #endif
}

INLINE uint32_t _kvcache_hash(struct hash *hash, struct hash_elem *e)
{
    struct kv_pair *pair = _get_entry(e, struct kv_pair, hash_elem);
    return (pair->vernum) % ((uint32_t)BCACHE_NBUCKET);
}

static struct fnamedic_item * _fname_create(struct filemgr *file) {
    // TODO: we MUST NOT directly read file sturcture

    struct fnamedic_item *fname;
    // Before we create a new filename entry, garbage collect zombies
    _garbage_collect_zombie_fnames();
    fname = (struct fnamedic_item *)malloc(sizeof(struct fnamedic_item));

    fname->filename_len = strlen(file->filename);
    fname->filename = (char *)malloc(fname->filename_len + 1);
    memcpy(fname->filename, file->filename, fname->filename_len);
    fname->filename[fname->filename_len] = 0;

    // calculate hash value
    fname->hash = get_checksum(reinterpret_cast<const uint8_t*>(fname->filename),
                                   fname->filename_len,
                                   file->crc_mode);
    fname->curfile = file;
    atomic_init_uint64_t(&fname->nvictim, 0);
    atomic_init_uint64_t(&fname->nitems, 0);
    atomic_init_uint64_t(&fname->nimmutable, 0);
    atomic_init_uint32_t(&fname->ref_count, 0);
    atomic_init_uint64_t(&fname->access_timestamp, 0);
    if (file->config->num_bcache_shards) {
        fname->num_shards = file->config->num_bcache_shards;
    } else {
        fname->num_shards = DEFAULT_NUM_BCACHE_PARTITIONS;
    }
    randomize();
    
    fname->shards = (kvcache_shard *)
        malloc(sizeof(struct kvcache_shard) * fname->num_shards);
    size_t i = 0;
    for (; i < fname->num_shards; ++i) {
        // initialize tree
        // avl_init(&fname->shards[i].tree, NULL);
//        avl_init(&fname->shards[i].tree_idx, NULL);
        // initialize clean list
//        list_init(&fname->shards[i].cleanlist);
        // initialize hash table
        hash_init(&fname->shards[i].hashtable, BCACHE_NBUCKET,
                  _kvcache_hash, _kvcache_cmp);
        list_init(&fname->shards[i].kvpair_lru);
        list_init(&fname->shards[i].dirty_pairs);
        spin_init(&fname->shards[i].lock);
    }

    // insert into fname dictionary
    hash_insert(&fnamedic, &fname->hash_elem);
    file->kvcache.store(fname, std::memory_order_relaxed);

    if (writer_lock(&filelist_lock) == 0) {
        if (num_files == file_array_capacity) {
            file_array_capacity *= 2;
            file_list = (struct fnamedic_item **) realloc(file_list,
                                                          file_array_capacity);
        }
        file_list[num_files++] = fname;
        writer_unlock(&filelist_lock);
    } else {
        fprintf(stderr, "Error in _fname_create(): "
                        "Failed to acquire WriterLock on filelist_lock!\n");
    }

    return fname;
}

void kvcache_init(uint64_t size, const kvcache_config& kvcconfig)
{ 
    hash_init(&fnamedic, KVCACHE_NDICBUCKET, _fname_hash, _kvcache_fname_cmp);

    kvcache_size = size;
    kvcache_flush_unit = KVCACHE_FLUSH_UNIT;
    kvcache_occupied_space = 0;
    kvcache_global_config = kvcconfig;
    
    int rv = init_rw_lock(&filelist_lock);
    if (rv != 0) {
        fdb_log(NULL, FDB_LOG_ERROR, FDB_RESULT_ALLOC_FAIL,
                "Error in kvcache_init(): "
                "RW Lock initialization failed; ErrorCode: %d", rv);
    }

    fdb_log(NULL, FDB_LOG_INFO, FDB_RESULT_SUCCESS,
            "Forestdb KV cache size %" _F64
            " initialized",
            (uint64_t)kvcache_size);

    spin_init(&lru_lock);
    spin_init(&space_lock);
    spin_init(&kvcache_lock);
    
    num_files = 0;
    file_array_capacity = 65536; // Initial capacity of file list array.
    file_list = (fnamedic_item **) calloc(file_array_capacity, sizeof(fnamedic_item *));

    kvcache_blocksize = kvcconfig.blocksize;
}

uint64_t kvcache_get_used_space()
{
    return kvcache_occupied_space;
}

struct fnamedic_item *_kvcache_get_victim()
{
    struct fnamedic_item *ret = NULL;
    uint64_t min_timestamp = (uint64_t) -1;
    uint64_t victim_timestamp;
    int victim_idx;
    size_t num_attempts;

    if (reader_lock(&filelist_lock) == 0) {
        // Pick the victim that has the smallest access timestamp
        // among files randomly selected.
        num_attempts = num_files / 10 + 1;
        if (num_attempts > MAX_VICTIM_SELECTIONS) {
            num_attempts = MAX_VICTIM_SELECTIONS;
        } else {
            if(num_attempts == 1 && num_files > 1) {
                ++num_attempts;
            }
        }
        for (size_t i = 0; i < num_attempts && num_files; ++i) {
            victim_idx = rand() % num_files;
            victim_timestamp =
                atomic_get_uint64_t(&file_list[victim_idx]->access_timestamp,
                                    std::memory_order_relaxed);
            if (victim_timestamp < min_timestamp &&
                    atomic_get_uint64_t(&file_list[victim_idx]->nitems)) {
                min_timestamp = victim_timestamp;
                ret = file_list[victim_idx];
            }
        }

        if (ret) {
            atomic_incr_uint32_t(&ret->ref_count);
        }
        reader_unlock(&filelist_lock);
    } else {
        fprintf(stderr, "Error in _bcache_get_victim(): "
                        "Failed to acquire ReaderLock on filelist_lock!\n");
    }

    return ret;
}

static fdb_status _flush_kv_cache(struct fnamedic_item *fname_item,
                                      bool sync, bool flush_all,
                                      bool immutables_only)
{
    void *buf = NULL;
    uint64_t count = 0;
    bool o_direct = false;
    struct kv_pair *pair;
    uint32_t blocksize;
    struct list_elem *e;
    unsigned int i;
    void *ptr = NULL;
    uint8_t marker = 0x0;
    uint32_t write_len = 0;

    if (fname_item->curfile->config->flag & _ARCH_O_DIRECT) {
        o_direct = true;
    }

    // scan and write back dirty blocks sequentially for O_DIRECT option.
    if (sync && o_direct) {
        malloc_align(buf, FDB_SECTOR_SIZE, kvcache_flush_unit);
    }

    count = 0;

    blocksize = fname_item->curfile->blocksize;
    
    bool all_empty = true;
    
    while(1) {
       for (i = 0; i < fname_item->num_shards; ++i) {
           if (!(sync && o_direct)) {
               spin_lock(&fname_item->shards[i].lock);
           }
           
           e = list_pop_back(&fname_item->shards[i].dirty_pairs);
           
           if(!e) {
               spin_unlock(&fname_item->shards[i].lock);
               continue;
           }
           
           all_empty = false;
           
           pair = _get_entry(e, struct kv_pair, dirty_pair_list_elem);

           if(pair->vlen == blocksize) {
               ptr = pair->value;
               marker = *((uint8_t*)(ptr) + pair->vlen - 1);
               
               #ifdef __CRC32
                   if (marker == BLK_MARKER_BNODE) {
                        // b-tree node .. calculate crc32 and put it into the block
                        memset((uint8_t *)(ptr) + BTREE_CRC_OFFSET,
                               0xff, BTREE_CRC_FIELD_LEN);
                        uint32_t crc = get_checksum(reinterpret_cast<const uint8_t*>(ptr),
                                                    write_len,
                                                    fname_item->curfile->crc_mode);
                        crc = _endian_encode(crc);
                        memcpy((uint8_t *)(ptr) + BTREE_CRC_OFFSET, &crc, sizeof(crc));
                   }
               #endif
           }

           if(write_len == 0) {
                write_len = pair->vlen;
           }
           
           if(pair->flag & KVCACHE_DIRTY) {
               size_t ret;
               if(fname_item->curfile->config->kvssd) {
                   ret = filemgr_write_pair_kvssd(fname_item->curfile,
                                                  pair->value,
                                                  write_len,
                                                  pair->vernum);
                   if(marker != BLK_MARKER_BNODE) {
                   } else {
                   }

                   if(ret != write_len) {
                          assert(0);
                   }
               } else {
                   ret = filemgr_write_blocks(fname_item->curfile,
                                        pair->value,
                                        1,
                                        pair->vernum);
                   if(marker != BLK_MARKER_BNODE) {
                   } else {
                   }
                   if(ret != fname_item->curfile->blocksize) {
                          assert(0);
                   }
               }
               pair->flag = 0x0 | KVCACHE_CLEAN;
               count += pair->vlen;
           }

           write_len = 0;
           
           if (!(sync && o_direct)) {
               spin_unlock(&fname_item->shards[i].lock);
           }
        
           if (count >= kvcache_flush_unit && sync) {
               if (flush_all) {
                   if (o_direct) {
                       // unsupported
                       assert(0);
                   }
               } else {
                   return FDB_RESULT_SUCCESS;;
               }
           }
        }
        
        if(all_empty) {
            break;
        }
  
        all_empty = true;
    }
    
    return FDB_RESULT_SUCCESS;
}

static struct list_elem * _kvcache_evict(struct fnamedic_item *curfile)
{
    size_t n_evict;
    struct kv_pair *pair;
    struct fnamedic_item *victim = NULL;

    victim = curfile;
    fdb_assert(victim, victim, NULL);

    n_evict = 0;
    while(n_evict < KVCACHE_EVICT_UNIT) {
        size_t num_shards = victim->num_shards;
        size_t i = random(num_shards);
        kvcache_shard *kvshard = NULL;

        for (size_t to_visit = num_shards; to_visit; --to_visit) {
            i = (i + 1) % num_shards;
            size_t shard_num = i;
            kvshard = &victim->shards[shard_num];
            spin_lock(&kvshard->lock);

            pair = _lru_get_oldest(&kvshard->kvpair_lru);
            
            if(pair == NULL) {
                spin_unlock(&kvshard->lock);
                continue;
            }

            hash_remove(&kvshard->hashtable, &pair->hash_elem);
            
            if(!(pair->flag & KVCACHE_CLEAN)) {
                assert(0);
            }
  
            atomic_sub_uint64_t(&kvcache_occupied_space, pair->vlen + sizeof(struct kv_pair));
            free(pair->value);
            free(pair);
            n_evict++;

            spin_unlock(&kvshard->lock);
            
            if(n_evict == KVCACHE_EVICT_UNIT) {
                break;
            }
            
            // untested
            if(n_evict < KVCACHE_EVICT_UNIT) {
                pair = _lru_get_oldest(&kvshard->kvpair_lru);
            }
        }
    }

    atomic_decr_uint32_t(&victim->ref_count);
    return &pair->elem;
}

void _free_space(struct kv_pair *pair)
{
    atomic_sub_uint64_t(&kvcache_occupied_space, pair->vlen);
    free(pair->value);
    free(pair);
}

struct kv_pair *_alloc_space(size_t vlen, uint64_t vernum)
{
    struct kv_pair *pair;
    
    if(atomic_get_uint64_t(&kvcache_occupied_space) + (vlen + sizeof(struct kv_pair)) > kvcache_size) {
        return NULL;
    }
    
    pair = (struct kv_pair*) malloc(sizeof(struct kv_pair));
    
    pair->value = malloc(vlen);
    pair->flag = 0x0 | KVCACHE_CLEAN;
    pair->vlen = vlen;
    pair->vernum = vernum;
    pair->score = 0;
    atomic_add_uint64_t(&kvcache_occupied_space, vlen + sizeof(struct kv_pair));
    
    return pair;
}

void _copy_pair(struct kv_pair* new_pair, struct kv_pair* old_pair)
{
    new_pair->hash_elem = old_pair->hash_elem;
    new_pair->elem = old_pair->elem;
    new_pair->dirty_pair_list_elem = old_pair->dirty_pair_list_elem;
    new_pair->flag = new_pair->flag | old_pair->flag;
}

INLINE void _kvcache_set_score(struct kv_pair *pair)
{
    return;
#ifdef __CRC32
    uint8_t marker;

    // set PTR and get block MARKER
    marker = *((uint8_t*)(pair->value) + pair->vlen - 1);
    if (marker == BLK_MARKER_BNODE ) {
        // b-tree node .. set pair's score to 1
        pair->score = 1;
    } else {
        pair->score = 0;
    }
#endif
}

/*
 * We convert a version number + log number pair
 * to one uint64_t and use the top N_BITS bits
 * as the log number
 */

uint8_t UINT_BITS = CHAR_BIT * sizeof(uint64_t);
uint8_t N_BITS = 7;
uint64_t _get_log_vernum(uint64_t vernum, uint32_t log_num)
{
    return (((uint64_t) log_num + 1) << (UINT_BITS - N_BITS)) | vernum; 
}

/*
 * Store logs in the KV cache as they aren't multiples
 * of the block size. It is OK for us to use the version
 * number of the corresponding index nodes as the key here,
 * as the index node data will be in the block cache and won't
 * be overwritten. The value is a pointer to a std::string log,
 * which is added to a heap-allocated vector inside the kv_pair.
 */

int kvcache_wipe_log(struct filemgr *file, uint64_t vernum, uint32_t log_num)
{
    struct hash_elem *h;
    struct fnamedic_item *fname;
    struct kv_pair *pair = NULL;
    struct kv_pair query;

    vernum = _get_log_vernum(vernum, log_num);
    fname = file->kvcache.load(std::memory_order_relaxed);

    if (fname) {
        query.vernum = vernum;
        struct timeval tp;
        gettimeofday(&tp, NULL); // TODO: Need to implement a better way of
        // getting the timestamp to avoid the overhead of
        // gettimeofday()
        atomic_store_uint64_t(&fname->access_timestamp,
                (uint64_t) (tp.tv_sec * 1000000 + tp.tv_usec),
                std::memory_order_relaxed);

        size_t shard_num = vernum % fname->num_shards;
        spin_lock(&fname->shards[shard_num].lock);

        // search shard hash table
        h = hash_find(&fname->shards[shard_num].hashtable, &query.hash_elem);
        if(h != NULL) {
            pair = _get_entry(h, struct kv_pair, hash_elem);
            assert(pair->log);

            if (!(pair->flag & KVCACHE_DIRTY)) {
                atomic_decr_uint64_t(&fname->nitems);
                hash_remove(&fname->shards[shard_num].hashtable, &pair->hash_elem);
                atomic_sub_uint64_t(&kvcache_occupied_space, pair->vlen);
                _lru_remove(&fname->shards[shard_num].kvpair_lru, &pair->elem);
            } else {
                assert(0);
            }

            atomic_decr_uint64_t(&fname->nitems);
            free(pair->value);
            free(pair);

            spin_unlock(&fname->shards[shard_num].lock);

            return 0;
        } else {
            spin_unlock(&fname->shards[shard_num].lock);
        }
    }

    return 0;
}

int kvcache_write_log(struct filemgr *file, uint64_t vernum,
                      void *val, uint32_t vlen, uint32_t log_num, 
                      kvcache_dirty_t dirty)
{
    vernum = _get_log_vernum(vernum, log_num);
    return kvcache_write(file, vernum, val, vlen, LOG);
}

int kvcache_write(struct filemgr *file, uint64_t vernum,
                  void *val, uint32_t vlen, kvcache_dirty_t dirty)
{
    struct hash_elem *h = NULL;
    struct kv_pair query;
    struct kv_pair *pair = NULL;
    struct fnamedic_item *fname;
    bool found = false;

    fname = file->kvcache;
    if (fname == NULL) {
        spin_lock(&kvcache_lock);
        fname = file->kvcache;
        if (fname == NULL) {
            // filename doesn't exist in filename dictionary .. create
                fname = _fname_create(file);
        }
        spin_unlock(&kvcache_lock);
    }
    
    // Update the access timestamp.
    struct timeval tp;
    gettimeofday(&tp, NULL);
    atomic_store_uint64_t(&fname->access_timestamp,
                          (uint64_t) (tp.tv_sec * 1000000 + tp.tv_usec),
                          std::memory_order_relaxed);
    
    size_t shard_num = vernum % fname->num_shards;
    spin_lock(&fname->shards[shard_num].lock);

    query.vernum = vernum;
    h = hash_find(&fname->shards[shard_num].hashtable, &query.hash_elem);

    if(h == NULL) {
        while((pair = _alloc_space(vlen, vernum)) == NULL) {
            spin_unlock(&fname->shards[shard_num].lock);
            
            _kvcache_evict(fname);

            spin_lock(&fname->shards[shard_num].lock);
        }

        h = hash_find(&fname->shards[shard_num].hashtable, &query.hash_elem);
        if(h == NULL) {
            hash_insert(&fname->shards[shard_num].hashtable, &pair->hash_elem);
            h = &pair->hash_elem;
        } else {
            pair = _get_entry(h, struct kv_pair, hash_elem);
            found = true;
        }
    } else {
        pair = _get_entry(h, struct kv_pair, hash_elem);
        found = true;
    }
    
     if(found && vlen != pair->vlen) {
         assert(0);
         pair->value = realloc(pair->value, vlen);
         assert(pair->value);
         memset(pair->value, 0x0, vlen);
         pair->vlen = vlen;
     }

    memcpy(pair->value, val, vlen);

    if (dirty == KVCACHE_REQ_DIRTY) {
        if(pair->flag & KVCACHE_CLEAN) {
            _lru_add(&fname->shards[shard_num].dirty_pairs, &pair->dirty_pair_list_elem);
        }
        
        pair->flag &= ~KVCACHE_CLEAN;
        pair->flag |= KVCACHE_DIRTY;
    }

    if(dirty == LOG) {
        pair->log = true;
    }
    
    if(!found) {
        atomic_incr_uint64_t(&fname->nitems);
        _lru_add(&fname->shards[shard_num].kvpair_lru, &pair->elem);
    } else {
        _lru_bump(&fname->shards[shard_num].kvpair_lru, &pair->elem);
    }

    _kvcache_set_score(pair);
    spin_unlock(&fname->shards[shard_num].lock);
    
    return vlen;
}

int kvcache_write_partial(struct filemgr *file, uint64_t vernum, size_t offset,
                  void *val, uint32_t vlen, kvcache_dirty_t dirty)
{
    struct hash_elem *h = NULL;
    struct kv_pair query;
    struct kv_pair *pair, *dummy_pair;
    struct fnamedic_item *fname;
    void* new_value;
    bool evicted = false;

    fname = file->kvcache;
    if (fname == NULL) {
        spin_lock(&kvcache_lock);
        fname = file->kvcache;
        if (fname == NULL) {
            // filename doesn't exist in filename dictionary .. create
            fname = _fname_create(file);
        }
        spin_unlock(&kvcache_lock);
    }

    // Update the access timestamp.
    struct timeval tp;
    gettimeofday(&tp, NULL);
    atomic_store_uint64_t(&fname->access_timestamp,
                          (uint64_t) (tp.tv_sec * 1000000 + tp.tv_usec),
                          std::memory_order_relaxed);

    size_t shard_num = vernum % fname->num_shards;
    spin_lock(&fname->shards[shard_num].lock);

    query.vernum = vernum;
    h = hash_find(&fname->shards[shard_num].hashtable, &query.hash_elem);
    if(h == NULL) {
        spin_unlock(&fname->shards[shard_num].lock);
        return 0;
    }

    pair = _get_entry(h, struct kv_pair, hash_elem);

    if(pair->vlen != vlen) {
        void* tmp = malloc(pair->vlen);
        uint32_t tmp_len = pair->vlen;
        memcpy(tmp, pair->value, pair->vlen);

        while((dummy_pair = _alloc_space(vlen, vernum)) == NULL) {
            spin_unlock(&fname->shards[shard_num].lock);
            
            _kvcache_evict(fname);

            spin_lock(&fname->shards[shard_num].lock);
        }

        h = hash_find(&fname->shards[shard_num].hashtable, &query.hash_elem);

        if(h != NULL) {
            new_value = realloc(pair->value, vlen);

            if(!new_value) {
                fprintf(stderr, "New value allocation failed in kvcache_write_partial\n");
                exit(0);
            }

            pair->value = new_value;
            dummy_pair->vlen = pair->vlen;
            pair->vlen = vlen;
            _free_space(dummy_pair);
        } else {
            evicted = true;
            memcpy(dummy_pair->value, tmp, tmp_len);
            pair = dummy_pair;
        }
        free(tmp);
    }

    memcpy((uint8_t*) pair->value + offset, val, vlen);
    
    if (dirty == KVCACHE_REQ_DIRTY) {
        if(pair->flag & KVCACHE_CLEAN) {
            _lru_add(&fname->shards[shard_num].dirty_pairs, &pair->dirty_pair_list_elem);
        }
        
        pair->flag &= ~KVCACHE_CLEAN;
        pair->flag |= KVCACHE_DIRTY;
    }
    
    if(!evicted) {
        _lru_bump(&fname->shards[shard_num].kvpair_lru, &pair->elem);
    } else {
        atomic_incr_uint64_t(&fname->nitems);
        _lru_add(&fname->shards[shard_num].kvpair_lru, &pair->elem);
    }


    _kvcache_set_score(pair);
    spin_unlock(&fname->shards[shard_num].lock);

    return vlen;
}

int kvcache_read_log(struct filemgr *file, uint64_t vernum, 
                     void *buf, uint32_t log_num)
{
    vernum = _get_log_vernum(vernum, log_num);
    return kvcache_read(file, vernum, buf);
}

int kvcache_read(struct filemgr *file, uint64_t vernum, void *buf)
{
    struct hash_elem *h;
    struct fnamedic_item *fname;
    struct kv_pair *pair = NULL;
    struct kv_pair query;
    int ret;
    
    fname = file->kvcache.load(std::memory_order_relaxed);

    if (fname) {
        query.vernum = vernum;
        struct timeval tp;
        gettimeofday(&tp, NULL); // TODO: Need to implement a better way of
                                 // getting the timestamp to avoid the overhead of
                                 // gettimeofday()
        atomic_store_uint64_t(&fname->access_timestamp,
                              (uint64_t) (tp.tv_sec * 1000000 + tp.tv_usec),
                              std::memory_order_relaxed);
        
        size_t shard_num = vernum % fname->num_shards;
        spin_lock(&fname->shards[shard_num].lock);
        
        // search shard hash table
        h = hash_find(&fname->shards[shard_num].hashtable, &query.hash_elem);
        if(h != NULL) {
            pair = _get_entry(h, struct kv_pair, hash_elem);

            assert(pair->vlen > 0);
            memcpy(buf, pair->value, pair->vlen);
            ret = pair->vlen;
            _lru_bump(&fname->shards[shard_num].kvpair_lru, &pair->elem);
            _kvcache_set_score(pair);
            spin_unlock(&fname->shards[shard_num].lock);
            return ret;
        } else {
            spin_unlock(&fname->shards[shard_num].lock);
        }
    }
    return 0;
}

static bool _fname_try_free(struct fnamedic_item *fname)
{
    bool ret = true;

    if (writer_lock(&filelist_lock) == 0) {
        // Remove from the file list array
        bool found = false;
        for (size_t i = 0; i < num_files; ++i) {
            if (file_list[i] == fname) {
                found = true;
            }
            if (found && (i+1 < num_files)) {
                file_list[i] = file_list[i+1];
            }
        }
        if (!found) {
            writer_unlock(&filelist_lock);
            DBG("Error: fnamedic_item instance for a file '%s' can't be "
                    "found in the buffer cache's file list.\n", fname->filename);
            return false;
        }

        file_list[num_files - 1] = NULL;
        --num_files;
        if (atomic_get_uint32_t(&fname->ref_count) != 0) {
            // This item is a victim by another thread's _bcache_evict()
            list_push_front(&file_zombies, &fname->le);
            ret = false; // Delay deletion
        }

        writer_unlock(&filelist_lock);
    } else {
        fprintf(stderr, "Error in _fname_try_free(): "
                        "Failed to acquire WriterLock on filelist_lock!\n");
    }

    return ret;
}

// remove all clean pairs of the FILE
void kvcache_remove_clean_blocks(struct filemgr *file)
{
    struct fnamedic_item *fname_item;

    fname_item = file->kvcache;

    if (fname_item) {
        // Note that this function is only invoked as part of database file close or
        // removal when there are no database handles for a given file. Therefore,
        // we don't need to grab all the shard locks at once.

        // remove all clean blocks from each shard in a file.
        size_t i = 0;
        struct kv_pair *pair;
        for (; i < fname_item->num_shards; ++i) {
            pair = _lru_get_oldest(&fname_item->shards[i].kvpair_lru);
            while(pair != NULL) {
                hash_remove(&fname_item->shards[i].hashtable, &pair->hash_elem);
                _free_space(pair);
                pair = _lru_get_oldest(&fname_item->shards[i].kvpair_lru);
            }
        }
    }
}

// remove file from filename dictionary
// MUST sure that there is no dirty block belongs to this FILE
// (or memory leak occurs)
bool kvcache_remove_file(struct filemgr *file)
{
    bool rv = false;
    struct fnamedic_item *fname_item;

    // Before proceeding with deletion, garbage collect zombie files
    _garbage_collect_zombie_fnames();
    fname_item = file->kvcache;

    if (fname_item) {
        // acquire lock
        spin_lock(&kvcache_lock);
        // file must be empty
        if (!_file_empty(fname_item)) {
            spin_unlock(&kvcache_lock);
            DBG("Warning: failed to remove fnamedic_item instance for a file '%s' "
                "because the fnamedic_item instance is not empty!\n",
                file->filename);
            return rv;
        }

        // remove from fname dictionary hash table
        hash_remove(&fnamedic, &fname_item->hash_elem);
        spin_unlock(&kvcache_lock);

        // We don't need to grab the file buffer cache's partition locks
        // at once because this function is only invoked when there are
        // no database handles that access the file.
        if (_fname_try_free(fname_item)) {
            _fname_free(fname_item); // no other callers accessing this file
            rv = true;
        } // else fnamedic_item is in use by _bcache_evict. Deletion delayed
    }
    return rv;
}

fdb_status kvcache_flush(struct filemgr *file)
{
    struct fnamedic_item *fname_item;
    fdb_status status = FDB_RESULT_SUCCESS;

    fname_item = file->kvcache;

    if (fname_item &&
        file->config->kv_cache_doc_writes) {
        // Note that this function is invoked as part of a commit operation while
        // the filemgr's lock is already grabbed by a committer.
        // Therefore, we don't need to grab all the shard locks at once.
        status = _flush_kv_cache(fname_item, true, true, false);
    }
    return status;
}

// LCOV_EXCL_START
INLINE void _kvcache_free_bcache_item(struct hash_elem *h)
{
    struct kv_pair *pair = _get_entry(h, struct kv_pair, hash_elem);
    _free_space(pair);
}
// LCOV_EXCL_STOP

// LCOV_EXCL_START
INLINE void _kvcache_free_fnamedic(struct hash_elem *h)
{
    size_t i = 0;
    struct fnamedic_item *item;
    item = _get_entry(h, struct fnamedic_item, hash_elem);

    for (; i < item->num_shards; ++i) {
        hash_free_active(&item->shards[i].hashtable, _kvcache_free_bcache_item);
        spin_destroy(&item->shards[i].lock);
    }

    free(item->shards);
    free(item->filename);

    free(item);
}
// LCOV_EXCL_STOP

void kvcache_shutdown()
{
//    struct kv_pair *pair;
    struct list_elem *e;

    writer_lock(&filelist_lock);
    // Force clean zombies if any
    e = list_begin(&file_zombies);
    while (e) {
        struct fnamedic_item *fname = _get_entry(e, struct fnamedic_item, le);
        e = list_remove(&file_zombies, e);
        _fname_free(fname);
    }
    // Free the file list array
    free(file_list);
    writer_unlock(&filelist_lock);

    spin_lock(&kvcache_lock);
    hash_free_active(&fnamedic, _kvcache_free_fnamedic);
    spin_unlock(&kvcache_lock);

    spin_destroy(&kvcache_lock);

    int rv = destroy_rw_lock(&filelist_lock);
    if (rv != 0) {
        fprintf(stderr, "Error in bcache_shutdown(): "
                        "RW Lock's destruction failed; ErrorCode: %d\n", rv);
    }
}
