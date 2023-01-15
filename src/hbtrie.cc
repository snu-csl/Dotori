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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hbtrie.h"
#include "list.h"
#include "btree.h"
#include "btree_kv.h"
#include "btree_fast_str_kv.h"
#include "internal_types.h"
#include "log_message.h"
#include "btreeblock.h"

#include "memleak.h"

#ifdef __DEBUG
#ifndef __DEBUG_HBTRIE
    #undef DBG
    #undef DBGCMD
    #undef DBGSW
    #define DBG(...)
    #define DBGCMD(...)
    #define DBGSW(n, ...)
#endif
#endif

#define HBTRIE_EOK (0xF0)

#define CHUNK_FLAG (0x8000)
typedef uint16_t chunkno_t;
struct hbtrie_meta {
    chunkno_t chunkno;
    uint16_t prefix_len;
    void *value;
    void *prefix;
};

#define _l2c(trie, len) (( (len) ) / (trie)->chunksize)

// MUST return same value to '_get_nchunk(_hbtrie_reform_key(RAWKEY))'
INLINE int _get_nchunk_raw(struct hbtrie *trie, void *rawkey, int rawkeylen)
{
    return _l2c(trie, rawkeylen);
}

INLINE int _get_nchunk(struct hbtrie *trie, void *key, int keylen)
{
    return (keylen) / trie->chunksize;
}

int _hbtrie_reform_key(struct hbtrie *trie, void *rawkey,
                       int rawkeylen, void *outkey)
{
    int outkeylen;
    int nchunk;
    int i;
    uint8_t rsize;
    size_t csize = trie->chunksize;

    nchunk = _get_nchunk_raw(trie, rawkey, rawkeylen);
    if (nchunk == 0)
        nchunk = 1;
    outkeylen = nchunk * csize;

    if (nchunk > 2) {
        // copy chunk[0] ~ chunk[nchunk-2]
        rsize = rawkeylen - ((nchunk - 2) * csize);
    } else {
        rsize = rawkeylen;
    }
    if ( !(rsize && rsize <= trie->chunksize) ) {
        // Return error instead of abort.
        return -1;
#if 0
        fdb_assert(rsize && rsize <= trie->chunksize, rsize, trie);
#endif
    }
    memcpy((uint8_t*)outkey, (uint8_t*)rawkey, rawkeylen);

    if (rsize < csize) {
        // zero-fill rest space
        i = nchunk - 2;
        memset((uint8_t*) outkey + rsize, 0x0, csize - rsize);
        //memset((uint8_t*)outkey + (i*csize) + rsize, 0x0, 2*csize - rsize);
    } else if (rsize == csize) {
    } else {
        // zero-fill the last chunk
        i = nchunk - 1;
        memset((uint8_t*)outkey + i * csize, 0x0, csize);
    }

    // assign rsize at the end of the outkey
//    *((uint8_t*)outkey + outkeylen - 1) = rsize;

    return outkeylen;
}

// this function only returns (raw) key length
static int _hbtrie_reform_key_reverse(struct hbtrie *trie,
                                      void *key,
                                      int keylen)
{
    uint8_t rsize;
    rsize = *((uint8_t*)key + keylen - 1);
    if (!rsize) {
        return -1;
#if 0
        fdb_assert(rsize, rsize, trie);
#endif
    }

    if (rsize == trie->chunksize) {
        return keylen - trie->chunksize;
    } else {
        // rsize: 1 ~ chunksize-1
        return keylen - (trie->chunksize * 2) + rsize;
    }
}

#define _get_leaf_kv_ops btree_fast_str_kv_get_kb64_vb64
#define _get_leaf_key btree_fast_str_kv_get_key
#define _set_leaf_key btree_fast_str_kv_set_key
#define _set_leaf_inf_key btree_fast_str_kv_set_inf_key
#define _free_leaf_key btree_fast_str_kv_free_key

void hbtrie_init(struct hbtrie *trie, int chunksize, int valuelen,
                 int btree_nodesize, bid_t root_bid, void *btreeblk_handle,
                 struct btree_blk_ops *btree_blk_ops, void *doc_handle,
                 hbtrie_func_readkey *readkey)
{
    struct btree_kv_ops *btree_kv_ops, *btree_leaf_kv_ops;

    trie->chunksize = chunksize;
    trie->valuelen = valuelen;
    trie->btree_nodesize = btree_nodesize;
    trie->btree_blk_ops = btree_blk_ops;
    trie->btreeblk_handle = btreeblk_handle;
    trie->doc_handle = doc_handle;
    trie->root_bid = root_bid;
    trie->flag = 0x0;
    trie->leaf_height_limit = 0;
    trie->cmp_args.chunksize = chunksize;
    trie->cmp_args.cmp_func = NULL;
    trie->cmp_args.user_param = NULL;
    trie->aux = &trie->cmp_args;

    // assign key-value operations
    btree_kv_ops = (struct btree_kv_ops *)malloc(sizeof(struct btree_kv_ops));
    btree_leaf_kv_ops = (struct btree_kv_ops *)malloc(sizeof(struct btree_kv_ops));

    fdb_assert(valuelen == 8, valuelen, trie);
    fdb_assert((size_t)chunksize >= sizeof(void *), chunksize, trie);

    if (chunksize == 8 && valuelen == 8){
        btree_kv_ops = btree_kv_get_kb64_vb64(btree_kv_ops);
        btree_leaf_kv_ops = _get_leaf_kv_ops(btree_leaf_kv_ops);
    } else if (chunksize == 4 && valuelen == 8) {
        btree_kv_ops = btree_kv_get_kb32_vb64(btree_kv_ops);
        btree_leaf_kv_ops = _get_leaf_kv_ops(btree_leaf_kv_ops);
    } else {
        btree_kv_ops = btree_kv_get_kbn_vb64(btree_kv_ops);
        btree_leaf_kv_ops = _get_leaf_kv_ops(btree_leaf_kv_ops);
    }

    trie->btree_kv_ops = btree_kv_ops;
    trie->btree_leaf_kv_ops = btree_leaf_kv_ops;
    trie->readkey = readkey;
    trie->map = NULL;
    trie->last_map_chunk = (void *)malloc(chunksize);
    memset(trie->last_map_chunk, 0xff, chunksize); // set 0xffff...
}

void hbtrie_free(struct hbtrie *trie)
{
    free(trie->btree_kv_ops);
    free(trie->btree_leaf_kv_ops);
    free(trie->last_map_chunk);
}

void hbtrie_set_flag(struct hbtrie *trie, uint8_t flag)
{
    trie->flag = flag;
    if (trie->leaf_height_limit == 0) {
        trie->leaf_height_limit = 1;
    }
}

void hbtrie_set_leaf_height_limit(struct hbtrie *trie, uint8_t limit)
{
    trie->leaf_height_limit = limit;
}

void hbtrie_set_leaf_cmp(struct hbtrie *trie, btree_cmp_func *cmp)
{
    trie->btree_leaf_kv_ops->cmp = cmp;
}

void hbtrie_set_map_function(struct hbtrie *trie,
                             hbtrie_cmp_map *map_func)
{
    trie->map = map_func;
}

hbtrie_cmp_map* hbtrie_get_map_function(struct hbtrie* trie) {
    return trie->map;
}

// IMPORTANT: hbmeta doesn't have own allocated memory space (pointers only)
static void _hbtrie_fetch_meta(struct hbtrie *trie, int metasize,
                               struct hbtrie_meta *hbmeta, void *buf)
{
    // read hbmeta from buf
    int offset = 0;
    uint32_t valuelen = 0;

    memcpy(&hbmeta->chunkno, buf, sizeof(hbmeta->chunkno));
    hbmeta->chunkno = _endian_decode(hbmeta->chunkno);
    offset += sizeof(hbmeta->chunkno);

    memcpy(&valuelen, (uint8_t*)buf+offset, sizeof(trie->valuelen));
    offset += sizeof(trie->valuelen);

    if (valuelen > 0) {
        hbmeta->value = (uint8_t*)buf + offset;
        offset += trie->valuelen;
    } else {
        hbmeta->value = NULL;
    }

    if (metasize - offset > 0) {
        //memcpy(hbmeta->prefix, buf+offset, metasize - offset);
        hbmeta->prefix = (uint8_t*)buf + offset;
        hbmeta->prefix_len = metasize - offset;
    } else {
        hbmeta->prefix = NULL;
        hbmeta->prefix_len = 0;
    }
}

typedef enum {
    HBMETA_NORMAL,
    HBMETA_LEAF,
} hbmeta_opt;
/* << raw hbtrie meta structure >>
 * [Total meta length]: 2 bytes
 * [Chunk number]:      2 bytes
 * [Value length]:      1 bytes
 * [Value (optional)]:  x bytes
 * [Prefix (optional)]: y bytes
 */
static void _hbtrie_store_meta(struct hbtrie *trie,
                               metasize_t *metasize_out,
                               chunkno_t chunkno,
                               hbmeta_opt opt,
                               void *prefix,
                               int prefixlen,
                               void *value,
                               void *buf)
{
    chunkno_t _chunkno;

    // write hbmeta to buf
    *metasize_out = 0;

    if (opt == HBMETA_LEAF) {
        chunkno |= CHUNK_FLAG;
    }

    _chunkno = _endian_encode(chunkno);
    memcpy(buf, &_chunkno, sizeof(chunkno));
    *metasize_out += sizeof(chunkno);

    if (value) {
        memcpy((uint8_t*)buf + *metasize_out,
               &trie->valuelen, sizeof(trie->valuelen));
        *metasize_out += sizeof(trie->valuelen);
        memcpy((uint8_t*)buf + *metasize_out,
               value, trie->valuelen);
        *metasize_out += trie->valuelen;
    } else {
        memset((uint8_t*)buf + *metasize_out, 0x0, sizeof(trie->valuelen));
        *metasize_out += sizeof(trie->valuelen);
    }

    if (prefixlen > 0) {
        memcpy((uint8_t*)buf + *metasize_out, prefix, prefixlen);
        *metasize_out += prefixlen;
    }
}

INLINE int _hbtrie_find_diff_chunk(struct hbtrie *trie,
                                   void *key1,
                                   void *key2,
                                   int start_chunk,
                                   int end_chunk)
{
    int i;
    for (i=start_chunk; i < end_chunk; ++i) {
        if (memcmp((uint8_t*)key1 + trie->chunksize*i,
                   (uint8_t*)key2 + trie->chunksize*i,
                   trie->chunksize)) {
             return i;
        }
    }
    return i;
}

//3 ASSUMPTION: 'VALUE' should be based on same endian to hb+trie

#if defined(__ENDIAN_SAFE) || defined(_BIG_ENDIAN)
// endian safe option is turned on, OR,
// the architecture is based on big endian
INLINE void _hbtrie_set_msb(struct hbtrie *trie, void *value)
{
    *((uint8_t*)value) |= (uint8_t)0x80;
}
INLINE void _hbtrie_clear_msb(struct hbtrie *trie, void *value)
{
    *((uint8_t*)value) &= ~((uint8_t)0x80);
}
INLINE int _hbtrie_is_msb_set(struct hbtrie *trie, void *value)
{
    return *((uint8_t*)value) & ((uint8_t)0x80);
}
#else
// little endian
INLINE void _hbtrie_set_msb(struct hbtrie *trie, void *value)
{
    *((uint8_t*)value + (trie->valuelen-1)) |= (uint8_t)0x80;
}
INLINE void _hbtrie_clear_msb(struct hbtrie *trie, void *value)
{
    *((uint8_t*)value + (trie->valuelen-1)) &= ~((uint8_t)0x80);
}
INLINE int _hbtrie_is_msb_set(struct hbtrie *trie, void *value)
{
    return *((uint8_t*)value + (trie->valuelen-1)) & ((uint8_t)0x80);
}
#endif

struct btreelist_item {
    struct btree btree;
    chunkno_t chunkno;
    bid_t child_rootbid;
    struct list_elem e;
    uint8_t leaf;
};

struct btreeit_item {
    struct btree_iterator btree_it;
    chunkno_t chunkno;
    struct list_elem le;
    uint8_t leaf;
};

#define _is_leaf_btree(chunkno) ((chunkno) & CHUNK_FLAG)
#define _get_chunkno(chunkno) ((chunkno) & (~(CHUNK_FLAG)))

hbtrie_result hbtrie_iterator_init(struct hbtrie *trie,
                                   struct hbtrie_iterator *it,
                                   void *initial_key,
                                   size_t keylen)
{
    it->trie = *trie;

    // MUST NOT affect the original trie due to sharing the same memory segment
    it->trie.last_map_chunk = (void *)malloc(it->trie.chunksize);
    memset(it->trie.last_map_chunk, 0xff, it->trie.chunksize);

    it->curkey = (void *)malloc(HBTRIE_MAX_KEYLEN);

    if (initial_key) {
        it->keylen = _hbtrie_reform_key(trie, initial_key, keylen, it->curkey);
        if (it->keylen >= HBTRIE_MAX_KEYLEN) {
            free(it->curkey);
            DBG("Error: HBTrie iterator init fails because the init key length %d is "
                "greater than the max key length %d\n", it->keylen, HBTRIE_MAX_KEYLEN);
            return HBTRIE_RESULT_FAIL;
        }
        memset((uint8_t*)it->curkey + it->keylen, 0, trie->chunksize);
    }else{
        it->keylen = 0;
        memset(it->curkey, 0, trie->chunksize);
    }
    list_init(&it->btreeit_list);
    it->flags = 0;

    return HBTRIE_RESULT_SUCCESS;
}

hbtrie_result hbtrie_iterator_free(struct hbtrie_iterator *it)
{
    struct list_elem *e;
    struct btreeit_item *item;
    e = list_begin(&it->btreeit_list);
    while(e){
        item = _get_entry(e, struct btreeit_item, le);
        e = list_remove(&it->btreeit_list, e);
        btree_iterator_free(&item->btree_it);
        mempool_free(item);
    }
    free(it->trie.last_map_chunk);
    if (it->curkey) free(it->curkey);
    return HBTRIE_RESULT_SUCCESS;
}

// move iterator's cursor to the end of the key range.
// hbtrie_prev() call after hbtrie_last() will return the last key.
hbtrie_result hbtrie_last(struct hbtrie_iterator *it)
{
    struct hbtrie_iterator temp;

    temp = *it;
    hbtrie_iterator_free(it);

    it->trie = temp.trie;
    // MUST NOT affect the original trie due to sharing the same memory segment
    it->trie.last_map_chunk = (void *)malloc(it->trie.chunksize);
    memset(it->trie.last_map_chunk, 0xff, it->trie.chunksize);

    it->curkey = (void *)malloc(HBTRIE_MAX_KEYLEN);
    // init with the infinite (0xff..) key without reforming
    memset(it->curkey, 0xff, it->trie.chunksize);
    it->keylen = it->trie.chunksize;

    list_init(&it->btreeit_list);
    it->flags = 0;

    return HBTRIE_RESULT_SUCCESS;
}

// recursive function
#define HBTRIE_PREFIX_MATCH_ONLY (0x1)
#define HBTRIE_PARTIAL_MATCH (0x2)
static hbtrie_result _hbtrie_prev(struct hbtrie_iterator *it,
                                  struct btreeit_item *item,
                                  void *key_buf,
                                  size_t *keylen,
                                  void *value_buf,
                                  uint8_t flag)
{
    struct hbtrie *trie = &it->trie;
    struct list_elem *e;
    struct btreeit_item *item_new;
    struct btree btree;
    hbtrie_result hr = HBTRIE_RESULT_FAIL;
    btree_result br;
    struct hbtrie_meta hbmeta;
    struct btree_meta bmeta;
    void *chunk;
    uint8_t *k = alca(uint8_t, trie->chunksize);
    uint8_t *v = alca(uint8_t, trie->valuelen);
    memset(k, 0, trie->chunksize);
    memset(k, 0, trie->valuelen);
    bid_t bid;
    uint64_t offset;

    if (item == NULL) {
        // this happens only when first call
        // create iterator for root b-tree
        if (it->trie.root_bid == BLK_NOT_FOUND) return HBTRIE_RESULT_FAIL;
        // set current chunk (key for b-tree)
        chunk = it->curkey;
        // load b-tree
        btree_init_from_bid(
            &btree, trie->btreeblk_handle, trie->btree_blk_ops,
            trie->btree_kv_ops,
            trie->btree_nodesize, trie->root_bid);
        btree.aux = trie->aux;
        if (btree.ksize != trie->chunksize || btree.vsize != trie->valuelen) {
            if (((trie->chunksize << 4) | trie->valuelen) == btree.ksize) {
                // this is an old meta format
                return HBTRIE_RESULT_INDEX_VERSION_NOT_SUPPORTED;
            }
            // B+tree root node is corrupted.
            return HBTRIE_RESULT_INDEX_CORRUPTED;
        }

        item = (struct btreeit_item *)mempool_alloc(sizeof(
                                                    struct btreeit_item));
        item->chunkno = 0;
        item->leaf = 0;

        br = btree_iterator_init(&btree, &item->btree_it, chunk);
        if (br == BTREE_RESULT_FAIL) return HBTRIE_RESULT_FAIL;

        list_push_back(&it->btreeit_list, &item->le);
    }

    e = list_next(&item->le);
    if (e) {
        // if prev sub b-tree exists
        item_new = _get_entry(e, struct btreeit_item, le);
        hr = _hbtrie_prev(it, item_new, key_buf, keylen, value_buf, flag);
        if (hr == HBTRIE_RESULT_SUCCESS) return hr;
        it->keylen = (item->chunkno+1) * trie->chunksize;
    }

    while (hr != HBTRIE_RESULT_SUCCESS) {
        // get key-value from current b-tree iterator
        memset(k, 0, trie->chunksize);
        br = btree_prev(&item->btree_it, k, v);
        if (item->leaf) {
            _free_leaf_key(k);
        } else {
            chunk = (uint8_t*)it->curkey + item->chunkno * trie->chunksize;
            if (item->btree_it.btree.kv_ops->cmp(k, chunk,
                    item->btree_it.btree.aux) != 0) {
                // not exact match key .. the rest of string is not necessary anymore
                it->keylen = (item->chunkno+1) * trie->chunksize;
                HBTRIE_ITR_SET_MOVED(it);
            }
        }

        if (br == BTREE_RESULT_FAIL) {
            // no more KV pair in the b-tree
            btree_iterator_free(&item->btree_it);
            list_remove(&it->btreeit_list, &item->le);
            mempool_free(item);
            return HBTRIE_RESULT_FAIL;
        }

        // check whether v points to doc or sub b-tree
        if (_hbtrie_is_msb_set(trie, v)) {
            // MSB is set -> sub b-tree

            // load sub b-tree and create new iterator for the b-tree
            _hbtrie_clear_msb(trie, v);
            bid = trie->btree_kv_ops->value2bid(v);
            bid = _endian_decode(bid);
            btree_init_from_bid(&btree, trie->btreeblk_handle,
                                trie->btree_blk_ops, trie->btree_kv_ops,
                                trie->btree_nodesize, bid);

            // get sub b-tree's chunk number
            bmeta.data = (void *)mempool_alloc(trie->btree_nodesize);
            bmeta.size = btree_read_meta(&btree, bmeta.data);
            _hbtrie_fetch_meta(trie, bmeta.size, &hbmeta, bmeta.data);

            item_new = (struct btreeit_item *)
                       mempool_alloc(sizeof(struct btreeit_item));
            if (_is_leaf_btree(hbmeta.chunkno)) {
                hbtrie_cmp_func *void_cmp;

                if (trie->map) { // custom cmp functions exist
                    if (!memcmp(trie->last_map_chunk, it->curkey, trie->chunksize)) {
                        // same custom function was used in the last call ..
                        // do nothing
                    } else {
                        // get cmp function corresponding to the key
                        void* user_param;
                        trie->map(it->curkey, (void *)trie, &void_cmp, &user_param);
                        if (void_cmp) {
                            memcpy(trie->last_map_chunk, it->curkey, trie->chunksize);
                            // set aux for _fdb_custom_cmp_wrap()
                            trie->cmp_args.cmp_func = void_cmp;
                            trie->cmp_args.user_param = user_param;
                            trie->aux = &trie->cmp_args;
                        }
                    }
                }

                btree.kv_ops = trie->btree_leaf_kv_ops;
                item_new->leaf = 1;
            } else {
                item_new->leaf = 0;
            }
            btree.aux = trie->aux;
            hbmeta.chunkno = _get_chunkno(hbmeta.chunkno);
            item_new->chunkno = hbmeta.chunkno;

            // Note: if user's key is exactly aligned to chunk size, then the
            //       dummy chunk will be a zero-filled value, and it is used
            //       as a key in the next level of B+tree. Hence, there will be
            //       no problem to assign the dummy chunk to the 'chunk' variable.
            if ( (unsigned)((item_new->chunkno+1) * trie->chunksize) <=
                 it->keylen) {
                // happen only once for the first call (for each level of b-trees)
                chunk = (uint8_t*)it->curkey +
                        item_new->chunkno*trie->chunksize;
                if (item->chunkno+1 < item_new->chunkno) {
                    // skipped prefix exists
                    // Note: all skipped chunks should be compared using the default
                    //       cmp function
                    int i, offset_meta = 0, offset_key = 0, chunkcmp = 0;
                    for (i=item->chunkno+1; i<item_new->chunkno; ++i) {
                        offset_meta = trie->chunksize * (i - (item->chunkno+1));
                        offset_key = trie->chunksize * i;
                        chunkcmp = trie->btree_kv_ops->cmp(
                            (uint8_t*)it->curkey + offset_key,
                            (uint8_t*)hbmeta.prefix + offset_meta,
                            trie->aux);
                        if (chunkcmp < 0) {
                            // start_key's prefix is smaller than the skipped prefix
                            // we have to go back to parent B+tree and pick prev entry
                            mempool_free(bmeta.data);
                            mempool_free(item_new);
                            it->keylen = offset_key;
                            hr = HBTRIE_RESULT_FAIL;
                            HBTRIE_ITR_SET_MOVED(it);
                            break;
                        } else if (chunkcmp > 0 && trie->chunksize > 0) {
                            // start_key's prefix is gerater than the skipped prefix
                            // set largest key for next B+tree
                            chunk = alca(uint8_t, trie->chunksize);
                            memset(chunk, 0xff, trie->chunksize);
                            break;
                        }
                    }
                    if (chunkcmp < 0) {
                        // go back to parent B+tree
                        continue;
                    }
                }

            } else {
                // chunk number of the b-tree is shorter than current iterator's key
                if (!HBTRIE_ITR_IS_MOVED(it)) {
                    // The first prev call right after iterator init call.
                    // This means that the init key is smaller than
                    // the smallest key of the current tree, and larger than
                    // the largest key of the previous tree.
                    // So we have to go back to the parent tree, and
                    // return the largest key of the previous tree.
                    mempool_free(bmeta.data);
                    mempool_free(item_new);
                    it->keylen = (item->chunkno+1) * trie->chunksize;
                    HBTRIE_ITR_SET_MOVED(it);
                    continue;
                }
                // set largest key
                chunk = alca(uint8_t, trie->chunksize);
                memset(chunk, 0xff, trie->chunksize);
            }

            if (item_new->leaf && chunk && trie->chunksize > 0) {
                uint8_t *k_temp = alca(uint8_t, trie->chunksize);
                size_t _leaf_keylen, _leaf_keylen_raw = 0;

                _leaf_keylen = it->keylen - (item_new->chunkno * trie->chunksize);
                if (_leaf_keylen) {
                    _leaf_keylen_raw = _hbtrie_reform_key_reverse(
                                           trie, chunk, _leaf_keylen);
                    _set_leaf_key(k_temp, chunk, _leaf_keylen_raw);
                    if (_leaf_keylen_raw) {
                        btree_iterator_init(&btree, &item_new->btree_it, k_temp);
                    } else {
                        btree_iterator_init(&btree, &item_new->btree_it, NULL);
                    }
                } else {
                    // set initial key as the largest key
                    // for reverse scan from the end of the B+tree
                    _set_leaf_inf_key(k_temp);
                    btree_iterator_init(&btree, &item_new->btree_it, k_temp);
                }
                _free_leaf_key(k_temp);
            } else {
                btree_iterator_init(&btree, &item_new->btree_it, chunk);
            }
            list_push_back(&it->btreeit_list, &item_new->le);

            if (hbmeta.value && chunk == NULL) {
                // NULL key exists .. the smallest key in this tree .. return first
                offset = trie->btree_kv_ops->value2bid(hbmeta.value);
                if (!(flag & HBTRIE_PREFIX_MATCH_ONLY)) {
                    *keylen = trie->readkey(trie->doc_handle, offset, key_buf);
                    int _len = _hbtrie_reform_key( trie, key_buf, *keylen,
                                                   it->curkey );
                    if (_len < 0) {
                        // Critical error, return.
                        fdb_log(nullptr, FDB_LOG_FATAL,
                                FDB_RESULT_FILE_CORRUPTION,
                                "hb-trie corruption, btree %lx, trie %lx, "
                                "offset %lx",
                                item->btree_it.btree.root_bid,
                                trie->root_bid,
                                offset);
                        btree_iterator_free(&item->btree_it);
                        list_remove(&it->btreeit_list, &item->le);
                        mempool_free(item);
                        return HBTRIE_RESULT_INDEX_CORRUPTED;
                    }
                    it->keylen = _len;
                }
                memcpy(value_buf, &offset, trie->valuelen);
                hr = HBTRIE_RESULT_SUCCESS;
            } else {
                hr = _hbtrie_prev(it, item_new, key_buf, keylen, value_buf,
                                  flag);
            }
            mempool_free(bmeta.data);
            if (hr == HBTRIE_RESULT_SUCCESS)
                return hr;

            // fail searching .. get back to parent tree
            // (this happens when the initial key is smaller than
            // the smallest key in the current tree (ITEM_NEW) ..
            // so return back to ITEM and retrieve next child)
            it->keylen = (item->chunkno+1) * trie->chunksize;
            HBTRIE_ITR_SET_MOVED(it);

        } else {
            // MSB is not set -> doc
            // read entire key and return the doc offset
            offset = trie->btree_kv_ops->value2bid(v);
            if (!(flag & HBTRIE_PREFIX_MATCH_ONLY)) {
                *keylen = trie->readkey(trie->doc_handle, offset, key_buf);
                int _len = _hbtrie_reform_key(trie, key_buf, *keylen, it->curkey);
                if (_len < 0) {
                    // Critical error, return.
                    fdb_log(nullptr, FDB_LOG_FATAL,
                            FDB_RESULT_FILE_CORRUPTION,
                            "hb-trie corruption, btree %lx, trie %lx, "
                            "offset %lx",
                            item->btree_it.btree.root_bid,
                            trie->root_bid,
                            offset);
                    btree_iterator_free(&item->btree_it);
                    list_remove(&it->btreeit_list, &item->le);
                    mempool_free(item);
                    return HBTRIE_RESULT_INDEX_CORRUPTED;
                }
                it->keylen = _len;
            }
            memcpy(value_buf, &offset, trie->valuelen);

            return HBTRIE_RESULT_SUCCESS;
        }
    }
    return HBTRIE_RESULT_FAIL;
}

hbtrie_result hbtrie_prev(struct hbtrie_iterator *it,
                          void *key_buf,
                          size_t *keylen,
                          void *value_buf)
{
    hbtrie_result hr;

    if (HBTRIE_ITR_IS_REV(it) && HBTRIE_ITR_IS_FAILED(it)) {
        return HBTRIE_RESULT_FAIL;
    }

    struct list_elem *e = list_begin(&it->btreeit_list);
    struct btreeit_item *item = NULL;
    if (e) item = _get_entry(e, struct btreeit_item, le);

    hr = _hbtrie_prev(it, item, key_buf, keylen, value_buf, 0x0);
    HBTRIE_ITR_SET_REV(it);
    if (hr == HBTRIE_RESULT_SUCCESS) {
        HBTRIE_ITR_CLR_FAILED(it);
        HBTRIE_ITR_SET_MOVED(it);
    } else {
        HBTRIE_ITR_SET_FAILED(it);
    }
    return hr;
}

// recursive function
static hbtrie_result _hbtrie_next(struct hbtrie_iterator *it,
                                  struct btreeit_item *item,
                                  void *key_buf,
                                  size_t *keylen,
                                  void *value_buf,
                                  uint8_t flag)
{
    struct hbtrie *trie = &it->trie;
    struct list_elem *e;
    struct btreeit_item *item_new;
    struct btree btree;
    hbtrie_result hr = HBTRIE_RESULT_FAIL;
    btree_result br;
    struct hbtrie_meta hbmeta;
    struct btree_meta bmeta;
    void *chunk;
    uint8_t *k = alca(uint8_t, trie->chunksize);
    uint8_t *v = alca(uint8_t, trie->valuelen);
    bid_t bid;
    uint64_t offset;

    if (item == NULL) {
        // this happens only when first call
        // create iterator for root b-tree
        if (it->trie.root_bid == BLK_NOT_FOUND) return HBTRIE_RESULT_FAIL;
        // set current chunk (key for b-tree)
        chunk = it->curkey;
        // load b-tree
        btree_init_from_bid(
            &btree, trie->btreeblk_handle, trie->btree_blk_ops, trie->btree_kv_ops,
            trie->btree_nodesize, trie->root_bid);
        btree.aux = trie->aux;
        if (btree.ksize != trie->chunksize || btree.vsize != trie->valuelen) {
            if (((trie->chunksize << 4) | trie->valuelen) == btree.ksize) {
                // this is an old meta format
                return HBTRIE_RESULT_INDEX_VERSION_NOT_SUPPORTED;
            }
            // B+tree root node is corrupted.
            return HBTRIE_RESULT_INDEX_CORRUPTED;
        }

        item = (struct btreeit_item *)mempool_alloc(sizeof(struct btreeit_item));
        item->chunkno = 0;
        item->leaf = 0;

        br = btree_iterator_init(&btree, &item->btree_it, chunk);
        if (br == BTREE_RESULT_FAIL) return HBTRIE_RESULT_FAIL;

        list_push_back(&it->btreeit_list, &item->le);
    }

    e = list_next(&item->le);
    if (e) {
        // if next sub b-tree exists
        item_new = _get_entry(e, struct btreeit_item, le);
        hr = _hbtrie_next(it, item_new, key_buf, keylen, value_buf, flag);
        if (hr != HBTRIE_RESULT_SUCCESS) {
            it->keylen = (item->chunkno+1) * trie->chunksize;
        }
    }

    while (hr != HBTRIE_RESULT_SUCCESS) {
        // get key-value from current b-tree iterator
        memset(k, 0, trie->chunksize);
        br = btree_next(&item->btree_it, k, v);
        if (item->leaf) {
            _free_leaf_key(k);
        } else {
            chunk = (uint8_t*)it->curkey + item->chunkno * trie->chunksize;
            if (item->btree_it.btree.kv_ops->cmp(k, chunk,
                    item->btree_it.btree.aux) != 0) {
                // not exact match key .. the rest of string is not necessary anymore
                it->keylen = (item->chunkno+1) * trie->chunksize;
                HBTRIE_ITR_SET_MOVED(it);
            }
        }

        if (br == BTREE_RESULT_FAIL) {
            // no more KV pair in the b-tree
            btree_iterator_free(&item->btree_it);
            list_remove(&it->btreeit_list, &item->le);
            mempool_free(item);
            return HBTRIE_RESULT_FAIL;
        }

        if (flag & HBTRIE_PARTIAL_MATCH) {
            // in partial match mode, we don't read actual doc key,
            // and just store & return indexed part of key.
            memcpy((uint8_t*)it->curkey + item->chunkno * trie->chunksize,
                   k, trie->chunksize);
        }

        // check whether v points to doc or sub b-tree
        if (_hbtrie_is_msb_set(trie, v)) {
            // MSB is set -> sub b-tree

            // load sub b-tree and create new iterator for the b-tree
            _hbtrie_clear_msb(trie, v);
            bid = trie->btree_kv_ops->value2bid(v);
            bid = _endian_decode(bid);
            btree_init_from_bid(&btree, trie->btreeblk_handle,
                                trie->btree_blk_ops, trie->btree_kv_ops,
                                trie->btree_nodesize, bid);

            // get sub b-tree's chunk number
            bmeta.data = (void *)mempool_alloc(trie->btree_nodesize);
            bmeta.size = btree_read_meta(&btree, bmeta.data);
            _hbtrie_fetch_meta(trie, bmeta.size, &hbmeta, bmeta.data);

            item_new = (struct btreeit_item *)
                       mempool_alloc(sizeof(struct btreeit_item));
            if (_is_leaf_btree(hbmeta.chunkno)) {
                hbtrie_cmp_func *void_cmp;

                if (trie->map) { // custom cmp functions exist
                    if (!memcmp(trie->last_map_chunk, it->curkey, trie->chunksize)) {
                        // same custom function was used in the last call ..
                        // do nothing
                    } else {
                        // get cmp function corresponding to the key
                        void* user_param;
                        trie->map(it->curkey, (void *)trie, &void_cmp, &user_param);
                        if (void_cmp) {
                            memcpy(trie->last_map_chunk, it->curkey, trie->chunksize);
                            // set aux for _fdb_custom_cmp_wrap()
                            trie->cmp_args.cmp_func = void_cmp;
                            trie->cmp_args.user_param = user_param;
                            trie->aux = &trie->cmp_args;
                        }
                    }
                }

                btree.kv_ops = trie->btree_leaf_kv_ops;
                item_new->leaf = 1;
            } else {
                item_new->leaf = 0;
            }
            btree.aux = trie->aux;
            hbmeta.chunkno = _get_chunkno(hbmeta.chunkno);
            item_new->chunkno = hbmeta.chunkno;

            // Note: if user's key is exactly aligned to chunk size, then the
            //       dummy chunk will be a zero-filled value, and it is used
            //       as a key in the next level of B+tree. Hence, there will be
            //       no problem to assign the dummy chunk to the 'chunk' variable.
            if ( (unsigned)((item_new->chunkno+1) * trie->chunksize)
                 <= it->keylen) {
                // happen only once for the first call (for each level of b-trees)
                chunk = (uint8_t*)it->curkey +
                        item_new->chunkno*trie->chunksize;
                if (item->chunkno+1 < item_new->chunkno) {
                    // skipped prefix exists
                    // Note: all skipped chunks should be compared using the default
                    //       cmp function
                    int i, offset_meta = 0, offset_key = 0, chunkcmp = 0;
                    for (i=item->chunkno+1; i<item_new->chunkno; ++i) {
                        offset_meta = trie->chunksize * (i - (item->chunkno+1));
                        offset_key = trie->chunksize * i;
                        chunkcmp = trie->btree_kv_ops->cmp(
                            (uint8_t*)it->curkey + offset_key,
                            (uint8_t*)hbmeta.prefix + offset_meta,
                            trie->aux);
                        if (chunkcmp < 0) {
                            // start_key's prefix is smaller than the skipped prefix
                            // set smallest key for next B+tree
                            it->keylen = offset_key;
                            chunk = NULL;
                            break;
                        } else if (chunkcmp > 0) {
                            // start_key's prefix is gerater than the skipped prefix
                            // we have to go back to parent B+tree and pick next entry
                            mempool_free(bmeta.data);
                            mempool_free(item_new);
                            it->keylen = offset_key;
                            hr = HBTRIE_RESULT_FAIL;
                            HBTRIE_ITR_SET_MOVED(it);
                            break;
                        }
                    }
                    if (chunkcmp > 0) {
                        // go back to parent B+tree
                        continue;
                    }
                }
            } else {
                // chunk number of the b-tree is longer than current iterator's key
                // set smallest key
                chunk = NULL;
            }

            if (item_new->leaf && chunk && trie->chunksize > 0) {
                uint8_t *k_temp = alca(uint8_t, trie->chunksize);
                memset(k_temp, 0, trie->chunksize * sizeof(uint8_t));
                size_t _leaf_keylen, _leaf_keylen_raw = 0;

                _leaf_keylen = it->keylen - (item_new->chunkno * trie->chunksize);
                if (_leaf_keylen > 0) {
                    _leaf_keylen_raw = _hbtrie_reform_key_reverse(
                                           trie, chunk, _leaf_keylen);
                }
                if (_leaf_keylen_raw) {
                    _set_leaf_key(k_temp, chunk, _leaf_keylen_raw);
                    btree_iterator_init(&btree, &item_new->btree_it, k_temp);
                    _free_leaf_key(k_temp);
                } else {
                    btree_iterator_init(&btree, &item_new->btree_it, NULL);
                }
            } else {
                bool null_btree_init_key = false;
                if (!HBTRIE_ITR_IS_MOVED(it) && chunk && trie->chunksize > 0 &&
                    ((uint64_t)item_new->chunkno+1) * trie->chunksize == it->keylen) {
                    // Next chunk is the last chunk of the current iterator key
                    // (happens only on iterator_init(), it internally calls next()).
                    uint8_t *k_temp = alca(uint8_t, trie->chunksize);
                    memset(k_temp, 0x0, trie->chunksize);
                    k_temp[trie->chunksize - 1] = trie->chunksize;
                    if (!memcmp(k_temp, chunk, trie->chunksize)) {
                        // Extra chunk is same to the specific pattern
                        // ([0x0] [0x0] ... [trie->chunksize])
                        // which means that given iterator key is exactly aligned
                        // to chunk size and shorter than the position of the
                        // next chunk.
                        // To guarantee lexicographical order between
                        // NULL and zero-filled key (NULL < 0x0000...),
                        // we should init btree iterator with NULL key.
                        null_btree_init_key = true;
                    }
                }
                if (null_btree_init_key) {
                    btree_iterator_init(&btree, &item_new->btree_it, NULL);
                } else {
                    btree_iterator_init(&btree, &item_new->btree_it, chunk);
                }
            }
            list_push_back(&it->btreeit_list, &item_new->le);

            if (hbmeta.value && chunk == NULL) {
                // NULL key exists .. the smallest key in this tree .. return first
                offset = trie->btree_kv_ops->value2bid(hbmeta.value);
                if (flag & HBTRIE_PARTIAL_MATCH) {
                    // return indexed key part only
                    *keylen = (item->chunkno+1) * trie->chunksize;
                    memcpy(key_buf, it->curkey, *keylen);
                } else if (!(flag & HBTRIE_PREFIX_MATCH_ONLY)) {
                    // read entire key from doc's meta
                    *keylen = trie->readkey(trie->doc_handle, offset, key_buf);
                    int _len = _hbtrie_reform_key(trie, key_buf, *keylen, it->curkey);
                    if (_len < 0) {
                        // Critical error, return.
                        fdb_log(nullptr, FDB_LOG_FATAL,
                                FDB_RESULT_FILE_CORRUPTION,
                                "hb-trie corruption, btree %lx, trie %lx, "
                                "offset %lx",
                                item->btree_it.btree.root_bid,
                                trie->root_bid,
                                offset);
                        btree_iterator_free(&item->btree_it);
                        list_remove(&it->btreeit_list, &item->le);
                        mempool_free(item);
                        return HBTRIE_RESULT_INDEX_CORRUPTED;
                    }
                    it->keylen = _len;
                }
                memcpy(value_buf, &offset, trie->valuelen);
                hr = HBTRIE_RESULT_SUCCESS;
            } else {
                hr = _hbtrie_next(it, item_new, key_buf, keylen, value_buf, flag);
            }
            mempool_free(bmeta.data);
            if (hr == HBTRIE_RESULT_SUCCESS) {
                return hr;
            }

            // fail searching .. get back to parent tree
            // (this happens when the initial key is greater than
            // the largest key in the current tree (ITEM_NEW) ..
            // so return back to ITEM and retrieve next child)
            it->keylen = (item->chunkno+1) * trie->chunksize;

        } else {
            // MSB is not set -> doc
            // read entire key and return the doc offset
            offset = trie->btree_kv_ops->value2bid(v);
            if (flag & HBTRIE_PARTIAL_MATCH) {
                // return indexed key part only
                *keylen = (item->chunkno+1) * trie->chunksize;
                memcpy(key_buf, it->curkey, *keylen);
            } else if (!(flag & HBTRIE_PREFIX_MATCH_ONLY)) {
                // read entire key from doc's meta
                *keylen = trie->readkey(trie->doc_handle, offset, key_buf);
                int _len = _hbtrie_reform_key(trie, key_buf, *keylen, it->curkey);
                if (_len < 0) {
                    // Critical error, return.
                    fdb_log(nullptr, FDB_LOG_FATAL,
                            FDB_RESULT_FILE_CORRUPTION,
                            "hb-trie corruption, btree %lx, trie %lx, "
                            "offset %lx",
                            item->btree_it.btree.root_bid,
                            trie->root_bid,
                            offset);
                    btree_iterator_free(&item->btree_it);
                    list_remove(&it->btreeit_list, &item->le);
                    mempool_free(item);
                    return HBTRIE_RESULT_INDEX_CORRUPTED;
                }
                it->keylen = _len;
            }
            memcpy(value_buf, &offset, trie->valuelen);

            return HBTRIE_RESULT_SUCCESS;
        }
    }

    return hr;
}

hbtrie_result hbtrie_next(struct hbtrie_iterator *it,
                          void *key_buf,
                          size_t *keylen,
                          void *value_buf)
{
    hbtrie_result hr;

    if (HBTRIE_ITR_IS_FWD(it) && HBTRIE_ITR_IS_FAILED(it)) {
        return HBTRIE_RESULT_FAIL;
    }

    struct list_elem *e = list_begin(&it->btreeit_list);
    struct btreeit_item *item = NULL;
    if (e) item = _get_entry(e, struct btreeit_item, le);

    hr = _hbtrie_next(it, item, key_buf, keylen, value_buf, 0x0);
    HBTRIE_ITR_SET_FWD(it);
    if (hr == HBTRIE_RESULT_SUCCESS) {
        HBTRIE_ITR_CLR_FAILED(it);
        HBTRIE_ITR_SET_MOVED(it);
    } else {
        HBTRIE_ITR_SET_FAILED(it);
    }
    return hr;
}

hbtrie_result hbtrie_next_partial(struct hbtrie_iterator *it,
                                  void *key_buf,
                                  size_t *keylen,
                                  void *value_buf)
{
    hbtrie_result hr;

    if (HBTRIE_ITR_IS_FWD(it) && HBTRIE_ITR_IS_FAILED(it)) {
        return HBTRIE_RESULT_FAIL;
    }

    struct list_elem *e = list_begin(&it->btreeit_list);
    struct btreeit_item *item = NULL;
    if (e) item = _get_entry(e, struct btreeit_item, le);

    hr = _hbtrie_next(it, item, key_buf, keylen, value_buf, HBTRIE_PARTIAL_MATCH);
    HBTRIE_ITR_SET_FWD(it);
    if (hr == HBTRIE_RESULT_SUCCESS) {
        HBTRIE_ITR_CLR_FAILED(it);
        HBTRIE_ITR_SET_MOVED(it);
    } else {
        HBTRIE_ITR_SET_FAILED(it);
    }
    return hr;
}

hbtrie_result hbtrie_next_value_only(struct hbtrie_iterator *it,
                                     void *value_buf)
{
    hbtrie_result hr;

    if (it->curkey == NULL) return HBTRIE_RESULT_FAIL;

    struct list_elem *e = list_begin(&it->btreeit_list);
    struct btreeit_item *item = NULL;
    if (e) item = _get_entry(e, struct btreeit_item, le);

    hr = _hbtrie_next(it, item, NULL, 0, value_buf, HBTRIE_PREFIX_MATCH_ONLY);
    if (hr != HBTRIE_RESULT_SUCCESS) {
        // this iterator reaches the end of hb-trie
        free(it->curkey);
        it->curkey = NULL;
    }
    return hr;
}

static void _hbtrie_free_btreelist(struct list *btreelist)
{
    struct btreelist_item *btreeitem;
    struct list_elem *e;

    // free all items on list
    e = list_begin(btreelist);
    while(e) {
        btreeitem = _get_entry(e, struct btreelist_item, e);
        e = list_remove(btreelist, e);
        mempool_free(btreeitem);
    }
}

#ifdef __ENDIAN_SAFE
INLINE struct docio_length _docio_length_encode(struct docio_length length)
{
    struct docio_length ret;
    ret = length;
    ret.keylen = _endian_encode(length.keylen);
    ret.metalen = _endian_encode(length.metalen);
    ret.bodylen = _endian_encode(length.bodylen);
    ret.bodylen_ondisk = _endian_encode(length.bodylen_ondisk);
    return ret;
}
INLINE struct docio_length _docio_length_decode(struct docio_length length)
{
    struct docio_length ret;
    ret = length;
    ret.keylen = _endian_decode(length.keylen);
    ret.metalen = _endian_decode(length.metalen);
    ret.bodylen = _endian_decode(length.bodylen);
    ret.bodylen_ondisk = _endian_decode(length.bodylen_ondisk);
    return ret;
}
#else
#define _docio_length_encode(a)
#define _docio_length_decode(a)
#endif

static hbtrie_result _hbtrie_find(struct hbtrie *trie, void *key, int keylen,
        void *valuebuf, struct list *btreelist, uint8_t flag,
        bool async_read, struct async_read_ctx_t *ctx)
{
    struct btree *btree = NULL;
    struct btree btree_static;
    btree_result r;
    struct btreelist_item *btreeitem = NULL;
    struct btreeblk_handle *bhandle;

    if(async_read) {
        bhandle = ctx->bhandle;
    } else {
        bhandle = (struct btreeblk_handle*) trie->btreeblk_handle;
    }

    if (btreelist) {
        list_init(btreelist);
        btreeitem = (struct btreelist_item *)mempool_alloc(sizeof(struct btreelist_item));
        list_push_back(btreelist, &btreeitem->e);
        btree = &btreeitem->btree;
    } else {
        if(async_read) {
            btree = (struct btree*)malloc(sizeof(*btree));
        } else {
            btree = &btree_static;
        }
    }

    if (trie->root_bid == BLK_NOT_FOUND) {
        // retrieval fail
        return HBTRIE_RESULT_FAIL;
    } else {
        // read from root_bid
        r = btree_init_from_bid(btree, bhandle, trie->btree_blk_ops,
                                trie->btree_kv_ops, trie->btree_nodesize,
                                trie->root_bid);
        if (r != BTREE_RESULT_SUCCESS) {
            return HBTRIE_RESULT_FAIL;
        }
        btree->aux = trie->aux;
        if (btree->ksize != trie->chunksize || btree->vsize != trie->valuelen) {
            if (((trie->chunksize << 4) | trie->valuelen) == btree->ksize) {
                // this is an old meta format
                return HBTRIE_RESULT_INDEX_VERSION_NOT_SUPPORTED;
            }
            // B+tree root node is corrupted.
            return HBTRIE_RESULT_INDEX_CORRUPTED;
        }
    }

    if(async_read) {
		assert(0);
    } else {
        r = btree_find(btree, key, valuebuf);
        btreeblk_end(bhandle);
    }

    if (r == BTREE_RESULT_FAIL) {
        return HBTRIE_RESULT_FAIL;
    }

    return HBTRIE_RESULT_SUCCESS;
}

hbtrie_result hbtrie_find(struct hbtrie *trie, void *rawkey,
                          int rawkeylen, void *valuebuf)
{
    //int nchunk = _get_nchunk_raw(trie, rawkey, rawkeylen);
    int nchunk = 1;
    uint8_t *key = alca(uint8_t, nchunk * trie->chunksize);
    int keylen;

    keylen = _hbtrie_reform_key(trie, rawkey, rawkeylen, key);
    return _hbtrie_find(trie, key, keylen, valuebuf, NULL, 0x0, false, NULL);
}

hbtrie_result hbtrie_find_offset(struct hbtrie *trie, void *rawkey,
                                 int rawkeylen, void *valuebuf)
{
    int nchunk = _get_nchunk_raw(trie, rawkey, rawkeylen);
    uint8_t *key = alca(uint8_t, nchunk * trie->chunksize);
    int keylen;

    keylen = _hbtrie_reform_key(trie, rawkey, rawkeylen, key);
    return _hbtrie_find(trie, key, keylen, valuebuf, NULL,
                        HBTRIE_PREFIX_MATCH_ONLY, false, NULL);
}

hbtrie_result hbtrie_find_partial(struct hbtrie *trie, void *rawkey,
                                  int rawkeylen, void *valuebuf)
{
    int nchunk = _get_nchunk_raw(trie, rawkey, rawkeylen);
    uint8_t *key = alca(uint8_t, nchunk * trie->chunksize);
    int keylen;

    keylen = _hbtrie_reform_key(trie, rawkey, rawkeylen, key);
    return _hbtrie_find(trie, key, keylen, valuebuf, NULL,
                        HBTRIE_PARTIAL_MATCH, false, NULL);
}

hbtrie_result hbtrie_find_async(struct hbtrie *trie, void *rawkey,
                                int rawkeylen, void *valuebuf, 
                                struct async_read_ctx_t *ctx)
{
    return _hbtrie_find(trie, rawkey, rawkeylen, valuebuf, NULL, 0x0, true, ctx);
}

INLINE hbtrie_result _hbtrie_remove(struct hbtrie *trie,
                                    void *rawkey, int rawkeylen,
                                    uint8_t flag)
{
    //int nchunk = _get_nchunk_raw(trie, rawkey, rawkeylen);
    //int keylen;
    //uint8_t *key = alca(uint8_t, nchunk * trie->chunksize);
    //uint8_t *valuebuf = alca(uint8_t, trie->valuelen);
    //hbtrie_result r;
    //btree_result br = BTREE_RESULT_SUCCESS;
    //struct list btreelist;
    //struct btreelist_item *btreeitem;
    //struct list_elem *e;

    hbtrie_result ret_result = HBTRIE_RESULT_SUCCESS;
    btree_result r;
    int keylen;

    struct btree *btree = NULL;
    struct btree btree_static;

    keylen = rawkeylen;
    if (keylen != 8) {
        fprintf(stderr, "Invalid keylen\n");
        assert(0);
    }

    auto bhandle = (struct btreeblk_handle*)trie->btreeblk_handle;

    btree = &btree_static;
    if (trie->root_bid == BLK_NOT_FOUND) {
        // create root b-tree
        r = btree_init(btree, trie->btreeblk_handle,
                trie->btree_blk_ops, trie->btree_kv_ops,
                trie->btree_nodesize, trie->chunksize,
                trie->valuelen, 0x0, NULL);
        btree->aux = trie->aux;
        if (r != BTREE_RESULT_SUCCESS) {
            assert(0);
            return HBTRIE_RESULT_FAIL;
        }

        btreeblk_end(bhandle);
    } else {
        // read from root_bid
        r = btree_init_from_bid(btree, trie->btreeblk_handle,
                trie->btree_blk_ops, trie->btree_kv_ops,
                trie->btree_nodesize, trie->root_bid);
        btree->aux = trie->aux;
        if (r != BTREE_RESULT_SUCCESS) {
            assert(0);
            return HBTRIE_RESULT_FAIL;
        }
        if (btree->ksize != trie->chunksize || btree->vsize != trie->valuelen) {
            if (((trie->chunksize << 4) | trie->valuelen) == btree->ksize) {
                // this is an old meta format
                return HBTRIE_RESULT_INDEX_VERSION_NOT_SUPPORTED;
            }
            // B+tree root node is corrupted.
            assert(0);
            return HBTRIE_RESULT_INDEX_CORRUPTED;
        }
    }

    btree->aux = trie->aux;
    r = btree_remove(btree, rawkey);
    if (r == BTREE_RESULT_FAIL) {
        assert(0);
        ret_result = HBTRIE_RESULT_FAIL;
    }

    return ret_result;
}

hbtrie_result hbtrie_remove(struct hbtrie *trie,
                            void *rawkey,
                            int rawkeylen)
{
    int nchunk = 1;
    uint8_t *key = alca(uint8_t, nchunk * trie->chunksize);
    int keylen;

    keylen = _hbtrie_reform_key(trie, rawkey, rawkeylen, key);
    return _hbtrie_remove(trie, key, keylen, 0x0);
}

hbtrie_result hbtrie_remove_partial(struct hbtrie *trie,
                                    void *rawkey,
                                    int rawkeylen)
{
    return _hbtrie_remove(trie, rawkey, rawkeylen,
                          HBTRIE_PARTIAL_MATCH);
}

struct _key_item {
    size_t keylen;
    void *key;
    void *value;
    struct list_elem le;
};

// suppose that VALUE and OLDVALUE_OUT are based on the same endian in hb+trie
#define HBTRIE_PARTIAL_UPDATE (0x1)
INLINE hbtrie_result _hbtrie_insert(struct hbtrie *trie,
                                    void *rawkey, int rawkeylen,
                                    void *value, void *oldvalue_out,
                                    uint8_t flag)
{
    int keylen;

    hbtrie_result ret_result = HBTRIE_RESULT_SUCCESS;
    btree_result r;

    struct btree *btree = NULL;
    struct btree btree_static;

    keylen = rawkeylen;
    if (keylen != 8) {
        fprintf(stderr, "Invalid keylen\n");
        assert(0);
    }

    auto bhandle = (struct btreeblk_handle*)trie->btreeblk_handle;

    btree = &btree_static;
    if (trie->root_bid == BLK_NOT_FOUND) {
        // create root b-tree
        r = btree_init(btree, trie->btreeblk_handle,
                       trie->btree_blk_ops, trie->btree_kv_ops,
                       trie->btree_nodesize, trie->chunksize,
                       trie->valuelen, 0x0, NULL);
        btree->aux = trie->aux;
        if (r != BTREE_RESULT_SUCCESS) {
            assert(0);
            return HBTRIE_RESULT_FAIL;
        }

        btreeblk_end(bhandle);
    } else {
        // read from root_bid
        r = btree_init_from_bid(btree, trie->btreeblk_handle,
                                trie->btree_blk_ops, trie->btree_kv_ops,
                                trie->btree_nodesize, trie->root_bid);
        btree->aux = trie->aux;
        if (r != BTREE_RESULT_SUCCESS) {
            assert(0);
            return HBTRIE_RESULT_FAIL;
        }
        if (btree->ksize != trie->chunksize || btree->vsize != trie->valuelen) {
            if (((trie->chunksize << 4) | trie->valuelen) == btree->ksize) {
                // this is an old meta format
                return HBTRIE_RESULT_INDEX_VERSION_NOT_SUPPORTED;
            }
            // B+tree root node is corrupted.
            assert(0);
            return HBTRIE_RESULT_INDEX_CORRUPTED;
        }
    }

    btree->aux = trie->aux;

    // set 'oldvalue_out' to 0xff..
    if (oldvalue_out) {
        memset(oldvalue_out, 0xff, trie->valuelen);
    }
    r = btree_insert_get_old(btree, rawkey, value, oldvalue_out);
    if (r == BTREE_RESULT_FAIL) {
        assert(0);
        ret_result = HBTRIE_RESULT_FAIL;
    }
    trie->root_bid = btree->root_bid;

    return ret_result;
}

hbtrie_result hbtrie_insert(struct hbtrie *trie,
                            void *rawkey, int rawkeylen,
                            void *value, void *oldvalue_out) {
    int nchunk = 1;
    uint8_t *key = alca(uint8_t, nchunk * trie->chunksize);
    int keylen;

    keylen = _hbtrie_reform_key(trie, rawkey, rawkeylen, key);
    return _hbtrie_insert(trie, key, keylen, value, oldvalue_out, 0x0);
}

hbtrie_result hbtrie_insert_partial(struct hbtrie *trie,
                                    void *rawkey, int rawkeylen,
                                    void *value, void *oldvalue_out) {
    assert(0);
    return _hbtrie_insert(trie, rawkey, rawkeylen,
                          value, oldvalue_out, HBTRIE_PARTIAL_UPDATE);
}
