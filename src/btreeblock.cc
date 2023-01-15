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

#include "common.h"
#include "btreeblock.h"
#include "fdb_internal.h"

#include "memleak.h"

#ifdef __DEBUG
#ifndef __DEBUG_BTREEBLOCK
    #undef DBG
    #undef DBGCMD
    #undef DBGSW
    #define DBG(...)
    #define DBGCMD(...)
    #define DBGSW(n, ...)
#endif
#endif


struct btreeblk_addr{
    void *addr;
    struct list_elem le;
};

struct btreeblk_block {
    bid_t bid;
    int sb_no;
    uint32_t pos;
    uint8_t dirty;
    uint8_t age;
    void *addr;
    struct list_elem le;
    struct avl_node avl;
#ifdef __BTREEBLK_BLOCKPOOL
    struct btreeblk_addr *addr_item;
#endif
};

INLINE void _btreeblk_get_aligned_block(struct btreeblk_handle *handle,
                                        struct btreeblk_block *block)
{
#ifdef __BTREEBLK_BLOCKPOOL
    struct list_elem *e;

    e = list_pop_front(&handle->blockpool);
    if (e) {
        block->addr_item = _get_entry(e, struct btreeblk_addr, le);
        block->addr = block->addr_item->addr;
        memset(block->addr, 0x0, handle->file->index_blocksize);
        return;
    }
    // no free addr .. create
    block->addr_item = (struct btreeblk_addr *)
                       calloc(1, sizeof(struct btreeblk_addr));
#endif

    malloc_align(block->addr, FDB_SECTOR_SIZE, handle->file->index_blocksize);
    memset(block->addr, 0x0, handle->file->index_blocksize);
}

INLINE void _btreeblk_free_aligned_block(struct btreeblk_handle *handle,
                                         struct btreeblk_block *block)
{
#ifdef __BTREEBLK_BLOCKPOOL
    if (!block->addr_item) {
        // TODO: Need to log the corresponding error message.

        return;
    }
    // sync addr & insert into pool
    block->addr_item->addr = block->addr;
    list_push_front(&handle->blockpool, &block->addr_item->le);
    block->addr_item = NULL;
    return;

#endif

    free_align(block->addr);
}

INLINE void * _btreeblk_alloc(void *voidhandle, bid_t *bid, int sb_no)
{
    struct btreeblk_handle *handle = (struct btreeblk_handle *)voidhandle;
    struct btreeblk_block *block;
    uint32_t curpos;

    struct list_elem *e = list_end(&handle->alc_list);
    if (e) {
        block = _get_entry(e, struct btreeblk_block, le);
        if (block->pos <= (handle->file->index_blocksize) - (handle->nodesize)) {
            if (filemgr_is_writable(handle->file, block->bid)) {
                curpos = block->pos;
                block->pos += (handle->nodesize);
                *bid = block->bid * handle->nnodeperblock + curpos /
                    (handle->nodesize);
                return ((uint8_t *)block->addr + curpos);
            }
        }
    }

    // allocate new block from file manager
    block = (struct btreeblk_block *)calloc(1, sizeof(struct btreeblk_block));
    _btreeblk_get_aligned_block(handle, block);
    block->sb_no = sb_no;
    block->pos = handle->nodesize;
    block->bid = filemgr_alloc(handle->file, handle->log_callback);
    block->dirty = 1;
    block->age = 0;

    // If a block is allocated but not written back into file (due to
    // various reasons), the corresponding byte offset in the file is filled
    // with garbage data so that it causes various unexpected behaviors.
    // To avoid this issue, populate block cache for the given BID before use it.
    uint8_t marker = BLK_MARKER_BNODE;

    memset((uint8_t *) block->addr + handle->nodesize - BLK_MARKER_SIZE,
           marker, BLK_MARKER_SIZE);

    // btree bid differs to filemgr bid
    *bid = block->bid * handle->nnodeperblock;

    list_push_back(&handle->alc_list, &block->le);

    handle->nlivenodes++;
    handle->ndeltanodes++;

    return block->addr;
}
void * btreeblk_alloc(void *voidhandle, bid_t *bid) {
    return _btreeblk_alloc(voidhandle, bid, -1);
}


#ifdef __ENDIAN_SAFE
INLINE void _btreeblk_encode(struct btreeblk_handle *handle,
                             struct btreeblk_block *block)
{
    size_t i, nsb, sb_size, offset;
    void *addr;
    struct bnode *node;

    for (offset=0; offset<handle->nnodeperblock; ++offset) {
        if (block->sb_no > -1) {
            nsb = handle->sb[block->sb_no].nblocks;
            sb_size = handle->sb[block->sb_no].sb_size;
        } else {
            nsb = 1;
            sb_size = 0;
        }

        for (i=0;i<nsb;++i) {
            addr = (uint8_t*)block->addr +
                   (handle->nodesize) * offset +
                   sb_size * i;
#ifdef _BTREE_HAS_MULTIPLE_BNODES
            size_t j, n;
            struct bnode **node_arr;
            node_arr = btree_get_bnode_array(addr, &n);
            for (j=0;j<n;++j){
                node = node_arr[j];
                node->kvsize = _endian_encode(node->kvsize);
                node->flag = _endian_encode(node->flag);
                node->level = _endian_encode(node->level);
                node->nentry = _endian_encode(node->nentry);
            }
            free(node_arr);
#else
            node = btree_get_bnode(addr);
            node->kvsize = _endian_encode(node->kvsize);
            node->flag = _endian_encode(node->flag);
            node->level = _endian_encode(node->level);
            node->nentry = _endian_encode(node->nentry);
#endif
        }
    }
}
INLINE void _btreeblk_decode(struct btreeblk_handle *handle,
                             struct btreeblk_block *block)
{
    size_t i, nsb, sb_size, offset;
    void *addr;
    struct bnode *node;

    for (offset=0; offset<handle->nnodeperblock; ++offset) {
        if (block->sb_no > -1) {
            nsb = handle->sb[block->sb_no].nblocks;
            sb_size = handle->sb[block->sb_no].sb_size;
        } else {
            nsb = 1;
            sb_size = 0;
        }

        for (i=0;i<nsb;++i) {
            addr = (uint8_t*)block->addr +
                   (handle->nodesize) * offset +
                   sb_size * i;
#ifdef _BTREE_HAS_MULTIPLE_BNODES
            size_t j, n;
            struct bnode **node_arr;
            node_arr = btree_get_bnode_array(addr, &n);
            for (j=0;j<n;++j){
                node = node_arr[j];
                node->kvsize = _endian_decode(node->kvsize);
                node->flag = _endian_decode(node->flag);
                node->level = _endian_decode(node->level);
                node->nentry = _endian_decode(node->nentry);
            }
            free(node_arr);
#else
            node = btree_get_bnode(addr);
            node->kvsize = _endian_decode(node->kvsize);
            node->flag = _endian_decode(node->flag);
            node->level = _endian_decode(node->level);
            node->nentry = _endian_decode(node->nentry);
#endif
        }
    }
}
#else
#define _btreeblk_encode(a,b)
#define _btreeblk_decode(a,b)
#endif

INLINE void _btreeblk_free_dirty_block(struct btreeblk_handle *handle,
                                       struct btreeblk_block *block);

INLINE void * _btreeblk_read(void *voidhandle, bid_t bid, int sb_no)
{
    struct list_elem *elm = NULL;
    struct btreeblk_block *block = NULL;
    struct btreeblk_handle *handle = (struct btreeblk_handle *)voidhandle;
    bid_t filebid;
    int offset;

    filebid = bid / handle->nnodeperblock;
    offset = bid % handle->nnodeperblock;

    // check whether the block is in current lists
    // read list (clean or dirty)
    for (elm = list_begin(&handle->read_list); elm; elm = list_next(elm)) {
        block = _get_entry(elm, struct btreeblk_block, le);
        if (block->bid == filebid) {
            block->age = 0;
            return (uint8_t *)block->addr +
                (handle->nodesize) * offset;
        }
    }

    // allocation list (dirty)
    for (elm = list_begin(&handle->alc_list); elm; elm = list_next(elm)) {
        block = _get_entry(elm, struct btreeblk_block, le);
        if (block->bid == filebid &&
                block->pos >= (handle->nodesize) * offset) {
            block->age = 0;

            fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                    "Found %lu in alc list", bid);

            return (uint8_t *)block->addr +
                (handle->nodesize) * offset;
        }
    }

    // there is no block in lists
    // if miss, read from file and add item into read list
    block = (struct btreeblk_block *)calloc(1, sizeof(struct btreeblk_block));
    block->sb_no = -1;
    block->pos = (handle->file->index_blocksize);
    block->bid = filebid;
    block->dirty = 0;
    block->age = 0;

    _btreeblk_get_aligned_block(handle, block);

    fdb_status status;
    if (handle->dirty_update || handle->dirty_update_writer) {
        // read from the given dirty update entry
        status = filemgr_read_dirty(handle->file, block->bid, block->addr,
                                    handle->dirty_update, handle->dirty_update_writer,
                                    handle->log_callback, true);
    } else {
        uint64_t len = handle->file->index_blocksize;
        // normal read
        status = filemgr_read(handle->file, block->bid, block->addr,
                              &len, handle->log_callback, true);
    }
    if (status != FDB_RESULT_SUCCESS) {
        fdb_log(handle->log_callback, FDB_LOG_ERROR, status,
                "Failed to read the B+-Tree block (block id: %" _F64
                ", block address: %p)", block->bid, block->addr);
        _btreeblk_free_aligned_block(handle, block);
        mempool_free(block);
        return NULL;
    }

    _btreeblk_decode(handle, block);

    list_push_front(&handle->read_list, &block->le);

    return (uint8_t *)block->addr + (handle->nodesize) * offset;
}

void * btreeblk_read(void *voidhandle, bid_t bid)
{
    return _btreeblk_read(voidhandle, bid, -1);
}

INLINE void * _btreeblk_read_leaf(void *voidhandle, bid_t bid, int sb_no)
{
    struct list_elem *elm = NULL;
    struct btreeblk_block *block = NULL;
    struct btreeblk_handle *handle = (struct btreeblk_handle *)voidhandle;
    bid_t filebid;
    int offset;

    filebid = bid / handle->nnodeperblock;
    offset = bid % handle->nnodeperblock;

    // check whether the block is in current lists
    // read list (clean or dirty)
    for (elm = list_begin(&handle->read_list); elm; elm = list_next(elm)) {
        block = _get_entry(elm, struct btreeblk_block, le);
        if (block->bid == filebid) {
            block->age = 0;

            return (uint8_t *) block->addr +
                (handle->nodesize) * offset;
        }
    }

    // allocation list (dirty)
    for (elm = list_begin(&handle->alc_list); elm; elm = list_next(elm)) {
        block = _get_entry(elm, struct btreeblk_block, le);
        if (block->bid == filebid &&
                block->pos >= (handle->nodesize) * offset) {
            block->age = 0;

            return (uint8_t *)block->addr +
                (handle->nodesize) * offset;
        }
    }

    // there is no block in lists
    // if miss, read from file and add item into read list
    block = (struct btreeblk_block *)calloc(1, sizeof(struct btreeblk_block));
    block->sb_no = -1;
    block->pos = (handle->file->index_blocksize);
    block->bid = filebid;
    block->dirty = 0;
    block->age = 0;

    _btreeblk_get_aligned_block(handle, block);

    fdb_status status;
    if (handle->dirty_update || handle->dirty_update_writer) {
        // read from the given dirty update entry
        status = filemgr_read_dirty(handle->file, block->bid, block->addr,
                                    handle->dirty_update, handle->dirty_update_writer,
                                    handle->log_callback, false);
    } else {
        uint64_t len = handle->file->index_blocksize;
        // normal read
        status = filemgr_read(handle->file, block->bid, block->addr,
                              &len, handle->log_callback, false);
    }
    if (status != FDB_RESULT_SUCCESS) {
        _btreeblk_free_aligned_block(handle, block);
        free(block);
        return NULL;
    } else {
        _btreeblk_decode(handle, block);
    }

    list_push_front(&handle->read_list, &block->le);

    return (uint8_t *)block->addr + (handle->nodesize) * offset;
}

void * btreeblk_read_leaf(void *voidhandle, bid_t bid)
{
    return _btreeblk_read_leaf(voidhandle, bid, -1);
}

INLINE void _btreeblk_add_stale_block(struct btreeblk_handle *handle,
                                      uint64_t pos,
                                      uint32_t len)
{
    if(handle->file->config->kvssd) {
        for(unsigned int i = 0; i < handle->file->config->max_logs_per_node; i++) {
            del(handle->file, pos, i, NULL);
        }
    } else {
        filemgr_add_stale_block(handle->file, pos, len);
    }
}

void btreeblk_set_dirty(void *voidhandle, bid_t bid);
void * btreeblk_move(void *voidhandle, bid_t bid, bid_t *new_bid)
{
    struct btreeblk_handle *handle = (struct btreeblk_handle *)voidhandle;
    void *old_addr, *new_addr;

    old_addr = new_addr = NULL;

    // normal block
    old_addr = btreeblk_read(voidhandle, bid);
    new_addr = btreeblk_alloc(voidhandle, new_bid);
    handle->nlivenodes--;

    // move
    memcpy(new_addr, old_addr, (handle->nodesize));

    struct bnode *new_node = btree_get_bnode(new_addr);
    new_node->bid = *new_bid;


    // the entire block becomes stale
    if(!handle->file->config->kvssd) {
        _btreeblk_add_stale_block(handle, bid * handle->nodesize, handle->nodesize);
    } else {
        uint32_t bnode_len = _get_bnode_write_len(old_addr, NULL, false);
        assert(bnode_len <= handle->nodesize);
        _btreeblk_add_stale_block(handle, bid, bnode_len);
    }

    return new_addr;
}

// LCOV_EXCL_START
void btreeblk_remove(void *voidhandle, bid_t bid)
{
    struct btreeblk_handle *handle = (struct btreeblk_handle *)voidhandle;

    // normal block
    handle->nlivenodes--;
    _btreeblk_add_stale_block(handle,
                              bid * handle->nodesize,
                              handle->nodesize);
}
// LCOV_EXCL_STOP

int btreeblk_is_writable(void *voidhandle, bid_t bid)
{
    struct btreeblk_handle *handle = (struct btreeblk_handle *)voidhandle;
    bid_t filebid;

    filebid = bid / handle->nnodeperblock;

    return filemgr_is_writable(handle->file, filebid);
}

void btreeblk_set_dirty(void *voidhandle, bid_t bid)
{
    struct btreeblk_handle *handle = (struct btreeblk_handle *)voidhandle;
    struct list_elem *e;
    struct btreeblk_block *block;
    bid_t filebid;

    filebid = bid / handle->nnodeperblock;

    // list
    e = list_begin(&handle->read_list);
    while(e){
        block = _get_entry(e, struct btreeblk_block, le);
        if (block->bid == filebid) {
            block->dirty = 1;
            break;
        }
        e = list_next(e);
    }
}

size_t btreeblk_get_size(void *voidhandle, bid_t bid)
{
    struct btreeblk_handle *handle = (struct btreeblk_handle *)voidhandle;
    return handle->nodesize;
}

INLINE void _btreeblk_free_dirty_block(struct btreeblk_handle *handle,
                                       struct btreeblk_block *block)
{
    _btreeblk_free_aligned_block(handle, block);
    mempool_free(block);
}

INLINE fdb_status _btreeblk_write_dirty_block(struct btreeblk_handle *handle,
                                        struct btreeblk_block *block)
{
    fdb_status status;
    //2 MUST BE modified to support multiple nodes in a block

    _btreeblk_encode(handle, block);
    if ( handle->dirty_update_writer &&
         !handle->dirty_update_writer->bulk_load_mode ) {
        // dirty update is in-progress
        status = filemgr_write_dirty(handle->file, block->bid, block->addr,
                                     handle->dirty_update_writer,
                                     handle->log_callback);
    } else {
        // normal write into file
        if(handle->file->config->kvssd) {
            status = filemgr_write_offset(handle->file, block->bid, 0, 
                                          UINT64_MAX, block->addr, false,
                                          handle->log_callback);
        } else {
            status = filemgr_write(handle->file, NULL, block->bid, block->addr,
                                   handle->log_callback);
        }

    }

    if (status != FDB_RESULT_SUCCESS) {
        fdb_log(NULL, FDB_LOG_FATAL, status,
                "Failed to write the B+-Tree block (block id: %" _F64
                ", block address: %p, file %s)",
                block->bid, block->addr,
                handle->file->filename);
        assert(0);
    }
    _btreeblk_decode(handle, block);
    return status;
}

fdb_status btreeblk_operation_end(void *voidhandle)
{
    // flush and write all items in allocation list
    struct btreeblk_handle *handle = (struct btreeblk_handle *)voidhandle;
    struct list_elem *e;
    struct btreeblk_block *block;
    int writable;
    fdb_status status = FDB_RESULT_SUCCESS;

    // write and free items in allocation list
    e = list_begin(&handle->alc_list);
    while(e){
        block = _get_entry(e, struct btreeblk_block, le);
        writable = filemgr_is_writable(handle->file, block->bid);
        if (writable) {
            status = _btreeblk_write_dirty_block(handle, block);
            if (status != FDB_RESULT_SUCCESS) {
                assert(0);

                return status;
            }
        } else {
            fdb_log(nullptr, FDB_LOG_FATAL,
                    FDB_RESULT_WRITE_FAIL,
                    "b+tree node write fail, BID %zu, file %s",
                    block->bid,
                    handle->file->filename);
            assert(0);
            return FDB_RESULT_WRITE_FAIL;
        }

        if (block->pos + (handle->nodesize) > (handle->file->index_blocksize) || !writable) {
            // remove from alc_list and insert into read list
            e = list_remove(&handle->alc_list, &block->le);
            block->dirty = 0;
            list_push_front(&handle->read_list, &block->le);

            fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                    "%llu moving from alc list to read list\n", block->bid);
        }else {
            // reserve the block when there is enough space and the block is writable
            e = list_next(e);
        }
    }

    // free items in read list
    // list
    e = list_begin(&handle->read_list);
    while(e){
        block = _get_entry(e, struct btreeblk_block, le);

        if (block->dirty) {
            // write back only when the block is modified
            status = _btreeblk_write_dirty_block(handle, block);
            if (status != FDB_RESULT_SUCCESS) {
                assert(0);
                return status;
            }
            block->dirty = 0;
        }

        if (block->age >= BTREEBLK_AGE_LIMIT) {
            e = list_remove(&handle->read_list, &block->le);

            fdb_log(NULL, FDB_LOG_DEBUG, FDB_RESULT_SUCCESS,
                    "Freeing %llu from read list\n", block->bid);


            _btreeblk_free_dirty_block(handle, block);
        } else {
            block->age++;
            e = list_next(e);
        }
    }

    return status;
}

void btreeblk_discard_blocks(struct btreeblk_handle *handle)
{
    // discard all writable blocks in the read list
    struct list_elem *e;
    struct btreeblk_block *block;

    // free items in read list
    // list
    e = list_begin(&handle->read_list);
    while(e){
        block = _get_entry(e, struct btreeblk_block, le);
        e = list_next(&block->le);

        list_remove(&handle->read_list, &block->le);
        _btreeblk_free_dirty_block(handle, block);
    }
}

#ifdef __BTREEBLK_SUBBLOCK
    struct btree_blk_ops btreeblk_ops = {
        btreeblk_alloc,
        btreeblk_alloc_sub,
        btreeblk_enlarge_node,
        btreeblk_read,
        btreeblk_read_async,
        btreeblk_read_leaf,
        btreeblk_move,
        btreeblk_move_async,
        btreeblk_remove,
        btreeblk_is_writable,
        btreeblk_get_size,
        btreeblk_set_dirty,
        NULL
    };
#else
    struct btree_blk_ops btreeblk_ops = {
        btreeblk_alloc,
        NULL,
        NULL,
        btreeblk_read,
        btreeblk_read_leaf,
        btreeblk_move,
        btreeblk_remove,
        btreeblk_is_writable,
        btreeblk_get_size,
        btreeblk_set_dirty,
        NULL
    };
#endif

struct btree_blk_ops *btreeblk_get_ops()
{
    return &btreeblk_ops;
}

void btreeblk_init(struct btreeblk_handle *handle, struct filemgr *file,
                   uint32_t nodesize)
{
    handle->file = file;
    handle->nodesize = nodesize;
    handle->nnodeperblock = handle->file->index_blocksize / handle->nodesize;
    handle->nlivenodes = 0;
    handle->ndeltanodes = 0;
    handle->dirty_update = NULL;
    handle->dirty_update_writer = NULL;

    list_init(&handle->alc_list);
    list_init(&handle->read_list);

#ifdef __BTREEBLK_BLOCKPOOL
    list_init(&handle->blockpool);
#endif


    handle->sb = NULL;
}

// shutdown
void btreeblk_free(struct btreeblk_handle *handle)
{
    struct list_elem *e;
    struct btreeblk_block *block;

    // free all blocks in alc list
    e = list_begin(&handle->alc_list);
    while(e) {
        block = _get_entry(e, struct btreeblk_block, le);
        e = list_remove(&handle->alc_list, &block->le);
        _btreeblk_free_dirty_block(handle, block);
    }

    // free all blocks in read list
    // linked list
    e = list_begin(&handle->read_list);
    while(e) {
        block = _get_entry(e, struct btreeblk_block, le);
        e = list_remove(&handle->read_list, &block->le);
        _btreeblk_free_dirty_block(handle, block);
    }

#ifdef __BTREEBLK_BLOCKPOOL
    // free all blocks in the block pool
    struct btreeblk_addr *item;

    e = list_begin(&handle->blockpool);
    while(e){
        item = _get_entry(e, struct btreeblk_addr, le);
        e = list_next(e);

        free_align(item->addr);
        mempool_free(item);
    }
#endif
}

fdb_status btreeblk_end(struct btreeblk_handle *handle)
{
    struct list_elem *e;
    struct btreeblk_block *block;
    fdb_status status = FDB_RESULT_SUCCESS;

    // flush all dirty items
    status = btreeblk_operation_end((void *)handle);
    if (status != FDB_RESULT_SUCCESS) {
        return status;
    }

    // remove all items in lists
    e = list_begin(&handle->alc_list);
    while(e) {
        block = _get_entry(e, struct btreeblk_block, le);
        e = list_remove(&handle->alc_list, &block->le);

        block->dirty = 0;
        list_push_front(&handle->read_list, &block->le);
    }

    return status;
}

fdb_status btreeblk_free_readlist(struct btreeblk_handle *handle, bool only_leaf)
{
    struct list_elem *e;
    struct btreeblk_block *block;
    fdb_status status = FDB_RESULT_SUCCESS;

    // remove all items in lists
    e = list_begin(&handle->read_list);
    while(e) {
        block = _get_entry(e, struct btreeblk_block, le);
        if (only_leaf) {
            struct bnode * node = btree_get_bnode(block->addr);
            if (node->level == 1)
                e = list_remove(&handle->read_list, &block->le);
            else
                e = list_next(e);
        }
        else {
            e = list_remove(&handle->read_list, &block->le);
        }
    }

    return status;
}
