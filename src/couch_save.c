/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <stdlib.h>

#include "internal.h"
#include "bitfield.h"
#include "util.h"
#include "reduces.h"

static size_t assemble_seq_index_value(DocInfo *docinfo, char *dst)
{
    memset(dst, 0, 16);
    set_bits(dst, 0, 12, docinfo->id.size);
    set_bits(dst + 1, 4, 28, docinfo->size);
    set_bits(dst + 5, 0, 1, docinfo->deleted);
    set_bits(dst + 5, 1, 47, docinfo->bp);
    dst[11] = docinfo->content_meta;
    set_bits(dst + 12, 0, 32, docinfo->rev_seq); //16 bytes in.
    memcpy(dst + 16, docinfo->id.buf, docinfo->id.size);
    memcpy(dst + 16 + docinfo->id.size, docinfo->rev_meta.buf,
           docinfo->rev_meta.size);
    return 16 + docinfo->id.size + docinfo->rev_meta.size;
}

static size_t assemble_id_index_value(DocInfo *docinfo, char *dst)
{
    memset(dst, 0, 21);
    set_bits(dst, 0, 48, docinfo->db_seq);
    set_bits(dst + 6, 0, 32, docinfo->size);
    set_bits(dst + 10, 0, 1, docinfo->deleted);
    set_bits(dst + 10, 1, 47, docinfo->bp);
    dst[16] = docinfo->content_meta;
    set_bits(dst + 17, 0, 32, docinfo->rev_seq); //21 bytes in
    memcpy(dst + 21, docinfo->rev_meta.buf, docinfo->rev_meta.size);
    return 21 + docinfo->rev_meta.size;
}

static couchstore_error_t write_doc(Db *db, const Doc *doc, uint64_t *bp,
                                    size_t* disk_size, couchstore_save_options writeopts)
{
    couchstore_error_t errcode;
    if (writeopts & COMPRESS_DOC_BODIES) {
        errcode = db_write_buf_compressed(db, &doc->data, (off_t *) bp, disk_size);
    } else {
        errcode = db_write_buf(db, &doc->data, (off_t *) bp, disk_size);
    }

    return errcode;
}

static int id_action_compare(const void *actv1, const void *actv2)
{
    const couchfile_modify_action *act1, *act2;
    act1 = (const couchfile_modify_action *) actv1;
    act2 = (const couchfile_modify_action *) actv2;

    int cmp = ebin_cmp(act1->key, act2->key);
    if (cmp == 0) {
        if (act1->type < act2->type) {
            return -1;
        }
        if (act1->type > act2->type) {
            return 1;
        }
    }
    return cmp;
}

static int seq_action_compare(const void *actv1, const void *actv2)
{
    const couchfile_modify_action *act1, *act2;
    act1 = (const couchfile_modify_action *) actv1;
    act2 = (const couchfile_modify_action *) actv2;

    uint64_t seq1, seq2;

    seq1 = get_48(act1->key->buf);
    seq2 = get_48(act2->key->buf);

    if (seq1 < seq2) {
        return -1;
    }
    if (seq1 == seq2) {
        if (act1->type < act2->type) {
            return -1;
        }
        if (act1->type > act2->type) {
            return 1;
        }
        return 0;
    }
    if (seq1 > seq2) {
        return 1;
    }
    return 0;
}

typedef struct _idxupdatectx {
    couchfile_modify_action *seqacts;
    int actpos;

    sized_buf **seqs;
    sized_buf **seqvals;
    int valpos;

    fatbuf *deltermbuf;
} index_update_ctx;

static void idfetch_update_cb(couchfile_modify_request *rq,
                              sized_buf *k, sized_buf *v, void *arg)
{
    (void)k;
    (void)rq;
    //v contains a seq we need to remove ( {Seq,_,_,_,_} )
    uint64_t oldseq;
    sized_buf *delbuf = NULL;
    index_update_ctx *ctx = (index_update_ctx *) arg;

    if (v == NULL) { //Doc not found
        return;
    }

    oldseq = get_48(v->buf);

    delbuf = (sized_buf *) fatbuf_get(ctx->deltermbuf, sizeof(sized_buf));
    delbuf->buf = (char *) fatbuf_get(ctx->deltermbuf, 6);
    delbuf->size = 6;
    memset(delbuf->buf, 0, 6);
    set_bits(delbuf->buf, 0, 48, oldseq);

    ctx->seqacts[ctx->actpos].type = ACTION_REMOVE;
    ctx->seqacts[ctx->actpos].value.data = NULL;
    ctx->seqacts[ctx->actpos].key = delbuf;

    ctx->actpos++;
}

static couchstore_error_t update_indexes(Db *db,
                                         sized_buf *seqs,
                                         sized_buf *seqvals,
                                         sized_buf *ids,
                                         sized_buf *idvals,
                                         int numdocs)
{
    couchfile_modify_action *idacts;
    couchfile_modify_action *seqacts;
    sized_buf *idcmps;
    size_t size;
    fatbuf *actbuf;
    node_pointer *new_id_root;
    node_pointer *new_seq_root;
    couchstore_error_t errcode;
    couchfile_modify_request seqrq, idrq;
    sized_buf tmpsb;
    int ii;

    /*
    ** Two action list up to numdocs * 2 in size + Compare keys for ids,
    ** and compare keys for removed seqs found from id index +
    ** Max size of a int64 erlang term (for deleted seqs)
    */
    size = 4 * sizeof(couchfile_modify_action) + 2 * sizeof(sized_buf) + 10;

    if ((actbuf = fatbuf_alloc(numdocs * size)) == NULL) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    idacts = fatbuf_get(actbuf, numdocs * sizeof(couchfile_modify_action) * 2);
    seqacts = fatbuf_get(actbuf, numdocs * sizeof(couchfile_modify_action) * 2);
    idcmps = fatbuf_get(actbuf, numdocs * sizeof(sized_buf));

    if (idacts == NULL || seqacts == NULL || idcmps == NULL) {
        fatbuf_free(actbuf);
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    index_update_ctx fetcharg = {
        seqacts, 0, &seqs, &seqvals, 0, actbuf
    };

    for (ii = 0; ii < numdocs; ii++) {
        idcmps[ii].buf = ids[ii].buf + 5;
        idcmps[ii].size = ids[ii].size - 5;

        idacts[ii * 2].type = ACTION_FETCH;
        idacts[ii * 2].value.arg = &fetcharg;
        idacts[ii * 2 + 1].type = ACTION_INSERT;
        idacts[ii * 2 + 1].value.data = &idvals[ii];

        idacts[ii * 2].key = &ids[ii];
        idacts[ii * 2 + 1].key = &ids[ii];
    }

    qsort(idacts, numdocs * 2, sizeof(couchfile_modify_action),
          id_action_compare);

    idrq.cmp.compare = ebin_cmp;
    idrq.cmp.arg = &tmpsb;
    idrq.db = db;
    idrq.actions = idacts;
    idrq.num_actions = numdocs * 2;
    idrq.reduce = by_id_reduce;
    idrq.rereduce = by_id_rereduce;
    idrq.fetch_callback = idfetch_update_cb;
    idrq.db = db;

    new_id_root = modify_btree(&idrq, db->header.by_id_root, &errcode);
    if (errcode != COUCHSTORE_SUCCESS) {
        fatbuf_free(actbuf);
        return errcode;
    }

    while (fetcharg.valpos < numdocs) {
        seqacts[fetcharg.actpos].type = ACTION_INSERT;
        seqacts[fetcharg.actpos].value.data = &seqvals[fetcharg.valpos];
        seqacts[fetcharg.actpos].key = &seqs[fetcharg.valpos];
        fetcharg.valpos++;
        fetcharg.actpos++;
    }

    //printf("Total seq actions: %d\n", fetcharg.actpos);
    qsort(seqacts, fetcharg.actpos, sizeof(couchfile_modify_action),
          seq_action_compare);

    seqrq.cmp.compare = seq_cmp;
    seqrq.cmp.arg = &tmpsb;
    seqrq.actions = seqacts;
    seqrq.num_actions = fetcharg.actpos;
    seqrq.reduce = by_seq_reduce;
    seqrq.rereduce = by_seq_rereduce;
    seqrq.db = db;

    new_seq_root = modify_btree(&seqrq, db->header.by_seq_root, &errcode);
    if (errcode != COUCHSTORE_SUCCESS) {
        fatbuf_free(actbuf);
        return errcode;
    }

    if (db->header.by_id_root != new_id_root) {
        free(db->header.by_id_root);
        db->header.by_id_root = new_id_root;
    }

    if (db->header.by_seq_root != new_seq_root) {
        free(db->header.by_seq_root);
        db->header.by_seq_root = new_seq_root;
    }

    fatbuf_free(actbuf);
    return errcode;
}

static couchstore_error_t add_doc_to_update_list(Db *db,
                                                 const Doc *doc,
                                                 const DocInfo *info,
                                                 fatbuf *fb,
                                                 sized_buf *seqterm,
                                                 sized_buf *idterm,
                                                 sized_buf *seqval,
                                                 sized_buf *idval,
                                                 uint64_t seq,
                                                 couchstore_save_options options)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    DocInfo updated = *info;
    updated.db_seq = seq;

    seqterm->buf = (char *) fatbuf_get(fb, 6);
    seqterm->size = 6;
    error_unless(seqterm->buf, COUCHSTORE_ERROR_ALLOC_FAIL);
    memset(seqterm->buf, 0, 6);
    set_bits(seqterm->buf, 0, 48, seq);

    if (doc) {
        size_t disk_size;

        // Don't compress a doc unless the meta flag is set
        if (!(info->content_meta & COUCH_DOC_IS_COMPRESSED)) {
            options &= ~COMPRESS_DOC_BODIES;
        }
        errcode = write_doc(db, doc, &updated.bp, &disk_size, options);

        if (errcode != COUCHSTORE_SUCCESS) {
            return errcode;
        }
        updated.size = disk_size;
    } else {
        updated.deleted = 1;
        updated.bp = 0;
        updated.size = 0;
    }

    *idterm = updated.id;

    seqval->buf = (char *) fatbuf_get(fb, (44 + updated.id.size + updated.rev_meta.size));
    error_unless(seqval->buf, COUCHSTORE_ERROR_ALLOC_FAIL);
    seqval->size = assemble_seq_index_value(&updated, seqval->buf);

    idval->buf = (char *) fatbuf_get(fb, (44 + 10 + updated.rev_meta.size));
    error_unless(idval->buf, COUCHSTORE_ERROR_ALLOC_FAIL);
    idval->size = assemble_id_index_value(&updated, idval->buf);

    //We use 37 + id.size + 2 * rev_meta.size bytes
cleanup:
    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_documents(Db *db,
                                             Doc* const docs[],
                                             DocInfo *infos[],
                                             unsigned numdocs,
                                             couchstore_save_options options)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    unsigned ii;
    sized_buf *seqklist, *idklist, *seqvlist, *idvlist;
    size_t term_meta_size = 0;
    const Doc *curdoc;
    uint64_t seq = db->header.update_seq;

    fatbuf *fb;

    for (ii = 0; ii < numdocs; ii++) {
        // Get additional size for terms to be inserted into indexes
        // IMPORTANT: This must match the sizes of the fatbuf_get calls in add_doc_to_update_list!
        term_meta_size += 6
                        + 44 + infos[ii]->id.size + infos[ii]->rev_meta.size
                        + 44 + 10 + infos[ii]->rev_meta.size;
    }

    fb = fatbuf_alloc(term_meta_size +
                      numdocs * (sizeof(sized_buf) * 4)); //seq/id key and value lists

    if (fb == NULL) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }


    seqklist = fatbuf_get(fb, numdocs * sizeof(sized_buf));
    idklist = fatbuf_get(fb, numdocs * sizeof(sized_buf));
    seqvlist = fatbuf_get(fb, numdocs * sizeof(sized_buf));
    idvlist = fatbuf_get(fb, numdocs * sizeof(sized_buf));

    for (ii = 0; ii < numdocs; ii++) {
        seq++;
        if (docs) {
            curdoc = docs[ii];
        } else {
            curdoc = NULL;
        }

        errcode = add_doc_to_update_list(db, curdoc, infos[ii], fb,
                                         &seqklist[ii], &idklist[ii],
                                         &seqvlist[ii], &idvlist[ii],
                                         seq, options);
        if (errcode != COUCHSTORE_SUCCESS) {
            break;
        }
    }

    if (errcode == COUCHSTORE_SUCCESS) {
        errcode = update_indexes(db, seqklist, seqvlist,
                                 idklist, idvlist, numdocs);
    }

    fatbuf_free(fb);
    if (errcode == COUCHSTORE_SUCCESS) {
        // Fill in the assigned sequence numbers for caller's later use:
        seq = db->header.update_seq;
        for (ii = 0; ii < numdocs; ii++) {
            infos[ii]->db_seq = ++seq;
        }
        db->header.update_seq = seq;
    }

    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_document(Db *db, const Doc *doc,
                                            DocInfo *info, couchstore_save_options options)
{
    return couchstore_save_documents(db, (Doc**)&doc, (DocInfo**)&info, 1, options);
}
