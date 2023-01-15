
#include "filemgr.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    KVCACHE_REQ_CLEAN,
    KVCACHE_REQ_DIRTY,
    LOG
} kvcache_dirty_t;

struct kv_pair {
    uint64_t vernum;
    void* value;
    struct hash_elem hash_elem;
    uint32_t vlen;
    struct list_elem elem;
    struct list_elem dirty_pair_list_elem;
    atomic_uint8_t flag;
    uint8_t score;
    bool log = false;
};

struct kvcache_config {
    uint32_t size;
    uint32_t blocksize;
};

void kvcache_init(uint64_t size, const kvcache_config& kvcconfig);
int kvcache_wipe_log(struct filemgr *file, uint64_t vernum, uint32_t log_num);
int kvcache_read_log(struct filemgr *file, uint64_t vernum, 
        void *buf, uint32_t log_num);
int kvcache_read(struct filemgr *file, uint64_t vernum, void *buf);
bool kvcache_invalidate_pair(struct filemgr *file, bid_t bid);
int kvcache_write_log(struct filemgr *file, uint64_t vernum,
        void *val, uint32_t vlen, uint32_t log_num, kvcache_dirty_t dirty);
int kvcache_write(struct filemgr *file, uint64_t vernum,
                  void *val, uint32_t vlen, kvcache_dirty_t dirty);
int kvcache_write_partial(struct filemgr *file, uint64_t vernum, size_t offset,
                          void *val, uint32_t vlen, kvcache_dirty_t dirty);
void kvcache_remove_dirty_blocks(struct filemgr *file);
void kvcache_remove_clean_blocks(struct filemgr *file);
bool kvcache_remove_file(struct filemgr *file);
uint64_t kvcache_get_num_pairs(struct filemgr *file);
fdb_status kvcache_flush(struct filemgr *file);
uint64_t kvcache_get_num_immutable(struct filemgr *file);
fdb_status kvcache_flush_immutable(struct filemgr *file);
void kvcache_shutdown();
uint64_t kvcache_get_free_space();
uint64_t kvcache_get_used_space();
void kvcache_print_items();

extern uint8_t UINT_BITS;
extern uint8_t N_BITS;

#ifdef __cplusplus
}
#endif
