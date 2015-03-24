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
#include <stdint.h>
#include <time.h>
#if !defined(WIN32) && !defined(_WIN32)
#include <unistd.h>
#endif

#include "libforestdb/forestdb.h"
#include "test.h"
#include "e2espec.h"

void load_persons(storage_t *st)
{
    int i, n=100;
    person_t p;

    // store and index person docs
    for(i=0;i<n;++i){
        gen_person(&p);
        e2e_fdb_set_person(st, &p);
    }

#ifdef __DEBUG_E2E
    printf("load persons: %03d docs created",n);
#endif
}

void delete_persons(storage_t *st)
{
    TEST_INIT();

    fdb_doc *rdoc = NULL;
    fdb_status status;
    fdb_iterator *it;
    person_t *p;
    int i, n = 0;
    char rbuf[256];

    // delete every 5th doc
    status = fdb_iterator_sequence_init(st->all_docs, &it, 0, 0,
                                        FDB_ITR_NO_DELETES);
    TEST_CHK(status == FDB_RESULT_SUCCESS);

    i = 0;
    do {
        status = fdb_iterator_get(it, &rdoc);
        TEST_CHK (status == FDB_RESULT_SUCCESS);
        if((i % 5) == 0){
            p = (person_t *)rdoc->body;
            e2e_fdb_del_person(st, p);
            n++;
        }
        fdb_doc_free(rdoc);
        rdoc=NULL;
        i++;
    } while (fdb_iterator_next(it) != FDB_RESULT_ITERATOR_FAIL);

    fdb_iterator_close(it);

    sprintf(rbuf, "delete persons: %03d docs deleted",n);
#ifdef __DEBUG_E2E
    TEST_RESULT(rbuf);
#endif
}

/*
 * reset params used to index storage
 * delete old docs that are part of new index
 * so that they are not included at verification time
 */
void update_index(storage_t *st){

    TEST_INIT();

    fdb_iterator *it;
    person_t *p;
    fdb_doc *rdoc = NULL;
    fdb_status status;
    int n = 0;
    size_t vallen;
    char *mink = st->index_params->min;
    char *maxk = st->index_params->max;
    char rbuf[256];

    // change storage index range
    reset_storage_index(st);

    status = fdb_iterator_init(st->index1, &it, mink, 12,
                               maxk, 12, FDB_ITR_NO_DELETES);
    if (status != FDB_RESULT_SUCCESS){
        // no items within min max range
        TEST_CHK(status == FDB_RESULT_ITERATOR_FAIL);
    }

    start_checkpoint(st);
    do {
        status = fdb_iterator_get(it, &rdoc);
        if (status == FDB_RESULT_SUCCESS){
            status = fdb_get_kv(st->all_docs,
                                rdoc->body, rdoc->bodylen,
                                (void **)&p, &vallen);
            TEST_CHK(status == FDB_RESULT_SUCCESS);
            e2e_fdb_del_person(st, p);
            fdb_doc_free(rdoc);
            free(p);
            p=NULL;
            rdoc=NULL;
            n++;
        }
    } while (fdb_iterator_next(it) != FDB_RESULT_ITERATOR_FAIL);
    end_checkpoint(st);
    fdb_iterator_close(it);

    // reset verification chkpoint
    st->v_chk->num_indexed = 0;
    st->v_chk->sum_age_indexed = 0;

    sprintf(rbuf, "update index: %03d docs deleted", n);
#ifdef __DEBUG_E2E
    TEST_RESULT(rbuf);
#endif
}

// --- verify ----
// 1. check that num of keys within index are correct
// 2. check that age index total is correct for specified range
// 3. check that doc count is as expected
void verify_db(storage_t *st){

    TEST_INIT();

    checkpoint_t *db_checkpoint = create_checkpoint(st, END_CHECKPOINT);
    int val1, val2;
    int db_ndocs = db_checkpoint->ndocs;
    int exp_ndocs = st->v_chk->ndocs;
    int db_nidx = db_checkpoint->num_indexed;
    int exp_nidx = st->v_chk->num_indexed;
    int db_suma = db_checkpoint->sum_age_indexed;
    int exp_suma = st->v_chk->sum_age_indexed;
    fdb_kvs_info info;
    fdb_iterator *it;
    fdb_doc *rdoc = NULL;
    char rbuf[256];

    e2e_fdb_commit(st->main, st->walflush);

    fdb_get_kvs_info(st->index1, &info);

    if (db_ndocs != exp_ndocs){
        // for debugging
        fdb_get_kvs_info(st->all_docs, &info);
        val1 = info.doc_count;
        val2 = 0;
        fdb_iterator_init(st->index1, &it, NULL, 0,
                          NULL, 0, FDB_ITR_NONE);
        do {
            fdb_iterator_get(it, &rdoc);
            if (!rdoc->deleted){
                val2++;
            }
            fdb_doc_free(rdoc);
            rdoc=NULL;
        } while (fdb_iterator_next(it) != FDB_RESULT_ITERATOR_FAIL);
#ifdef __DEBUG_E2E
        printf("ndocs_debug: kvs_info(%d) == exp_ndocs(%d) ?\n", val1,exp_ndocs);
        printf("ndocs_debug: kvs_info(%d) == itr_count(%d) ?\n", val1, val2);
#endif
        fdb_iterator_close(it);
    }

    free(db_checkpoint);
    db_checkpoint=NULL;
    // MB-13254: kvs_info doc_count incorrect with normal commit
    // TEST_CHK(db_ndocs==exp_ndocs);
    TEST_CHK(db_nidx==exp_nidx);
    TEST_CHK(db_suma==exp_suma);

    sprintf(rbuf, "verifydb: ndocs(%d=%d), nidx(%d=%d), sumage(%d=%d)\n",
            db_ndocs, exp_ndocs,
            db_nidx, exp_nidx,
            db_suma, exp_suma);
#ifdef __DEBUG_E2E
    TEST_RESULT(rbuf);
#endif
}


/*
 * compares a db where src is typically from
 * a rollback state of live data and replay is a
 * db to use for comparison  of expected data
 */
void db_compare(fdb_kvs_handle *src, fdb_kvs_handle *replay) {

    TEST_INIT();

    int ndoc1, ndoc2;
    fdb_kvs_info info;
    fdb_iterator *it;
    fdb_doc *rdoc = NULL;
    fdb_doc *vdoc = NULL;
    fdb_status status;
    char rbuf[256];

    fdb_get_kvs_info(src, &info);
    ndoc1 = info.doc_count;
    fdb_get_kvs_info(replay, &info);
    ndoc2 = info.doc_count;

    TEST_CHK(ndoc1 == ndoc2);

    // all docs in replay db must be in source db with same status
    status = fdb_iterator_sequence_init(replay, &it, 0, 0, FDB_ITR_NONE);
    TEST_CHK(status == FDB_RESULT_SUCCESS);
    do {
        status = fdb_iterator_get_metaonly(it, &rdoc);
        TEST_CHK(status == FDB_RESULT_SUCCESS);

        fdb_doc_create(&vdoc, rdoc->key, rdoc->keylen,
                              rdoc->meta, rdoc->metalen,
                              rdoc->body, rdoc->bodylen);
        // lookup by key
        status = fdb_get(src, vdoc);

        if (rdoc->deleted){
            TEST_CHK(status == FDB_RESULT_KEY_NOT_FOUND);
        } else {
            TEST_CHK(status == FDB_RESULT_SUCCESS);
        }

        fdb_doc_free(rdoc);
        fdb_doc_free(vdoc);
        rdoc=NULL;
        vdoc=NULL;

    } while (fdb_iterator_next(it) != FDB_RESULT_ITERATOR_FAIL);
    fdb_iterator_close(it);

    sprintf(rbuf, "db compare: src(%d) == replay(%d)", ndoc1, ndoc2);
#ifdef __DEBUG_E2E
    TEST_RESULT(rbuf);
#endif
}

/*
 * populate replay db up to specified seqnum
 */
void load_replay_kvs(storage_t *st, fdb_kvs_handle *replay_kvs, fdb_seqnum_t seqnum){

    TEST_INIT();

    fdb_iterator *it;
    fdb_doc *rdoc = NULL;
    fdb_status status;
    transaction_t *tx;


    // iterator end at seqnum
    status = fdb_iterator_sequence_init(st->rtx, &it, 0, seqnum,
                                        FDB_ITR_NONE);
    TEST_CHK(status == FDB_RESULT_SUCCESS);

    do {
        status = fdb_iterator_get(it, &rdoc);
        TEST_CHK(status == FDB_RESULT_SUCCESS);

        tx = (transaction_t *)rdoc->body;
        if (tx->type == SET_PERSON){
            status = fdb_set_kv(replay_kvs,
                                tx->refkey,
                                tx->refkey_len,
                                NULL,0);
            TEST_CHK(status == FDB_RESULT_SUCCESS);
        }
        if (tx->type == DEL_PERSON){
            status = fdb_del_kv(replay_kvs,
                                tx->refkey,
                                tx->refkey_len);
            TEST_CHK(status == FDB_RESULT_SUCCESS);
        }
        fdb_doc_free(rdoc);
        rdoc=NULL;

    } while (fdb_iterator_next(it) != FDB_RESULT_ITERATOR_FAIL);
    fdb_iterator_close(it);

}

/*
 * replay records db up to a checkpoint into another db
 * rollback all_docs to that checkpoint
 * compare all_docs at that state to new doc
 */
void replay(storage_t *st)
{
    TEST_INIT();

    int i;
    size_t v;
    char kvsbuf[10];
    fdb_file_handle *dbfile;
    fdb_kvs_handle *replay_kvs;
    fdb_config fconfig = fdb_get_default_config();
    fdb_kvs_config kvs_config = fdb_get_default_kvs_config();
    fconfig.wal_threshold = 1024;
    fconfig.flags = FDB_OPEN_FLAG_CREATE;
    fconfig.compaction_threshold = 10;
    fdb_iterator *it;
    fdb_status status;
    fdb_doc *rdoc = NULL;
    fdb_kvs_info info;
    transaction_t *tx;
    checkpoint_t *chk;
    fdb_seqnum_t rollback_seqnum;
    fdb_seqnum_t snap_seqnum;

    // create replay kvs
    status = fdb_open(&dbfile, E2EDB_RECORDS, &fconfig);
    TEST_CHK(status == FDB_RESULT_SUCCESS);

    e2e_fdb_commit(st->main, st->walflush);
    status = fdb_get_kvs_info(st->all_docs, &info);
    TEST_CHK(status == FDB_RESULT_SUCCESS);
    snap_seqnum = info.last_seqnum;


    // iterate over records kv and replay transactions
    status = fdb_iterator_sequence_init(st->rtx, &it, 0, 0, FDB_ITR_NONE);
    TEST_CHK(status == FDB_RESULT_SUCCESS);

    // seek to end so we can reverse iterate
    // seq iterators cannot seek
    do {
       ;
    } while (fdb_iterator_next(it) != FDB_RESULT_ITERATOR_FAIL);


    // reverse iterate from highest to lowest checkpoint
    i=0;
    while (fdb_iterator_prev(it) != FDB_RESULT_ITERATOR_FAIL){
        status = fdb_iterator_get(it, &rdoc);
        TEST_CHK(status == FDB_RESULT_SUCCESS);
        tx = (transaction_t *)rdoc->body;
        if(tx->type == END_CHECKPOINT){

            sprintf(kvsbuf, "rkvs%d", i);
            status = fdb_kvs_open(dbfile, &replay_kvs, kvsbuf,  &kvs_config);
            TEST_CHK(status == FDB_RESULT_SUCCESS);

            // load replay db up to this seqnum
            load_replay_kvs(st, replay_kvs, rdoc->seqnum);

            // get checkpoint doc for rollback
            status = fdb_get_kv(st->chk, tx->refkey, tx->refkey_len,
                                (void **)&chk, &v);

            TEST_CHK(status == FDB_RESULT_SUCCESS);
            rollback_seqnum = chk->seqnum_all;
;
            printf("rollback to %llu\n", chk->seqnum_all);
            status = fdb_rollback(&st->all_docs, rollback_seqnum);
            TEST_CHK(status == FDB_RESULT_SUCCESS);
            free(chk);
            chk=NULL;

            status = fdb_get_kvs_info(st->rtx, &info);
            TEST_CHK(status == FDB_RESULT_SUCCESS);

            // compare rollback and replay db
            e2e_fdb_commit(dbfile, st->walflush);
            db_compare(st->all_docs, replay_kvs);

            // drop replay kvs
            status = fdb_kvs_close(replay_kvs);
            TEST_CHK(status == FDB_RESULT_SUCCESS);
            status = fdb_kvs_remove(dbfile, kvsbuf);
            TEST_CHK(status == FDB_RESULT_SUCCESS);
            i++;
        }
        fdb_doc_free(rdoc);
        rdoc=NULL;
    }

    fdb_iterator_close(it);
    fdb_close(dbfile);

}

void e2e_kvs_index_pattern(int n_checkpoints, fdb_config fconfig, bool deletes, bool walflush)
{

    int n, i;
    storage_t *st;
    checkpoint_t verification_checkpoint;
    idx_prams_t index_params;
    fdb_kvs_config kvs_config = fdb_get_default_kvs_config();

    memleak_start();

    // init
    rm_storage_fs();
    gen_index_params(&index_params);
    memset(&verification_checkpoint, 0, sizeof(checkpoint_t));

    // setup
    st = init_storage(&fconfig, &fconfig, &kvs_config,
            &index_params, &verification_checkpoint, walflush);

    // test
    for (n=0;n<n_checkpoints;++n){

        // checkpoint
        start_checkpoint(st);

        for (i=0;i<100;++i){
#ifdef __DEBUG_E2E
            printf("\n\n----%d----\n", i);
#endif
            load_persons(st);
            if (deletes){
                delete_persons(st);
            }
            verify_db(st);

        }

        // end checkpoint
        end_checkpoint(st);

        // change index range
        update_index(st);
        verify_db(st);

    }

    if(fconfig.compaction_mode != FDB_COMPACTION_AUTO){
        /* replay involves rollbacks but
         * cannot rollback pre compact due to BUG: MB-13130
         */
        replay(st);
    }

    // teardown
    e2e_fdb_shutdown(st);

    memleak_end();
}

void e2e_index_basic_test()
{

    TEST_INIT();
    memleak_start();

    // configure
    fdb_config fconfig = fdb_get_default_config();
    fconfig.wal_threshold = 1024;
    fconfig.flags = FDB_OPEN_FLAG_CREATE;
    fconfig.durability_opt = FDB_DRB_ASYNC;

    // test
    e2e_kvs_index_pattern(1, fconfig, true, false); // normal commit
    e2e_kvs_index_pattern(1, fconfig, true, true);  // wal commit

    memleak_end();
    TEST_RESULT("TEST: e2e index basic test");
}

void e2e_index_walflush_test_no_deletes_auto_compact()
{

    TEST_INIT();
    memleak_start();

    // configure
    fdb_config fconfig = fdb_get_default_config();
    fconfig.wal_threshold = 1024;
    fconfig.flags = FDB_OPEN_FLAG_CREATE;
    fconfig.durability_opt = FDB_DRB_ASYNC;
    fconfig.compaction_mode=FDB_COMPACTION_AUTO;

    // test
    e2e_kvs_index_pattern(10, fconfig, false, true);

    memleak_end();
    TEST_RESULT("TEST: e2e index walflush test no deletes auto compact");
}

void e2e_index_walflush_autocompact_test()
{

    TEST_INIT();
    memleak_start();

    // opts
    fdb_config fconfig = fdb_get_default_config();
    fconfig.wal_threshold = 1024;
    fconfig.flags = FDB_OPEN_FLAG_CREATE;
    fconfig.durability_opt = FDB_DRB_ASYNC;
    fconfig.compaction_mode=FDB_COMPACTION_AUTO;

    // test
    e2e_kvs_index_pattern(2, fconfig, true, true);

    memleak_end();
    TEST_RESULT("TEST: e2e index walflush autocompact test");

}

void e2e_index_normal_commit_autocompact_test()
{

    TEST_INIT();
    memleak_start();

    // opts
    fdb_config fconfig = fdb_get_default_config();
    fconfig.wal_threshold = 1024;
    fconfig.flags = FDB_OPEN_FLAG_CREATE;
    fconfig.durability_opt = FDB_DRB_NONE;
    fconfig.compaction_mode=FDB_COMPACTION_AUTO;

    // test
    e2e_kvs_index_pattern(2, fconfig, true, false);

    memleak_end();
    TEST_RESULT("TEST: e2e index normal commit autocompact test");
}

int main()
{

    e2e_index_basic_test();
    e2e_index_walflush_test_no_deletes_auto_compact();
    e2e_index_walflush_autocompact_test();
    e2e_index_normal_commit_autocompact_test();

    return 0;
}
