/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * @copyright 2013 Couchbase, Inc.
 *
 * @author Filipe Manana  <filipe@couchbase.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include <snappy-c.h>
#include "util.h"
#include "../util.h"
#include "../bitfield.h"
#include "collate_json.h"

/* Number of buffers that should be added if there aren't any free ones */
#define REALLOC_BUFFER_NUM 10
/* The maximum size of the buffer before it's written to disk */
#define COMPRESS_MAX_BUFFER_SIZE (64 * 1024 * 1024)


static bool is_incremental_update_record(view_file_merge_ctx_t *ctx) {
    return ctx->type == INCREMENTAL_UPDATE_VIEW_RECORD ||
            ctx->type == INCREMENTAL_UPDATE_SPATIAL_RECORD;
}


/* Adds (with re-allocation) new empty compression buffers to the
   end of the list of buffers */
static file_merger_error_t
extend_file_buffers(view_file_compression_buffer_t **buffers,
                    size_t *num_buffers) {
    view_file_compression_buffer_t *newbuffers;

    newbuffers = realloc(*buffers,
                         (*num_buffers + REALLOC_BUFFER_NUM) *
                         sizeof(view_file_compression_buffer_t));
    if (newbuffers == NULL) {
        return FILE_MERGER_ERROR_ALLOC;
    }

    /* Add empty items */
    for (int i = 0; i < REALLOC_BUFFER_NUM; i++) {
        sized_buf data_buffer;
        data_buffer.buf = NULL;
        data_buffer.size = 0;

        view_file_compression_buffer_t buffer;
        buffer.buffer = data_buffer;
        buffer.offset = 0;
        buffer.file = NULL;

        newbuffers[i + (*num_buffers)] = buffer;
    }

    *buffers = newbuffers;
    *num_buffers += REALLOC_BUFFER_NUM;
    return FILE_MERGER_SUCCESS;
}


/* Returns the buffer to the corresponding file */
static file_merger_error_t
get_file_buffer(view_file_merge_ctx_t *merge_ctx,
                FILE *file,
                view_file_compression_buffer_t **filebuf)
{
    size_t cur_buf;
    for(cur_buf = 0; cur_buf < merge_ctx->num_buffers; cur_buf++) {
        /* Buffer wasn't set  yet */
        if (merge_ctx->buffers[cur_buf].file == NULL) {
            merge_ctx->buffers[cur_buf].file = file;
            *filebuf = &merge_ctx->buffers[cur_buf];
            return FILE_MERGER_SUCCESS;
        }
        /* Buffer exists already and was Buffer found */
        else if (merge_ctx->buffers[cur_buf].file == file) {
            *filebuf = &merge_ctx->buffers[cur_buf];
            return FILE_MERGER_SUCCESS;
        }
    }
    /* No more free buffers, create addtional ones */
    file_merger_error_t ret = extend_file_buffers(&merge_ctx->buffers,
                                                  &merge_ctx->num_buffers);
    if (ret != FILE_MERGER_SUCCESS) {
        return ret;
    }
    merge_ctx->buffers[cur_buf].file = file;
    *filebuf = &merge_ctx->buffers[cur_buf];
    return FILE_MERGER_SUCCESS;
}


void read_from_file_buffer(void *dest,
                           size_t size,
                           view_file_compression_buffer_t **filebuf)
{
    memcpy(dest, &(*filebuf)->buffer.buf[(*filebuf)->offset], size);
    (*filebuf)->offset += size;
}


void free_file_buffer(view_file_compression_buffer_t **filebuf)
{
    free((*filebuf)->buffer.buf);
    (*filebuf)->buffer.buf = NULL;
    (*filebuf)->buffer.size = 0;
    (*filebuf)->offset = 0;
}


int fill_file_buffer(view_file_compression_buffer_t **filebuf)
{
    uint64_t comp_size, uncomp_size;
    snappy_status snappy_ret;
    int ret = FILE_MERGER_ERROR_FILE_READ;

    if (fread(&comp_size, sizeof(comp_size), 1, (*filebuf)->file) != 1) {
        if (feof((*filebuf)->file)) {
            return FILE_MERGER_SUCCESS;
        } else {
            return FILE_MERGER_ERROR_FILE_READ;
        }
    }
    char *comp = (char *)malloc(comp_size * sizeof(char));
    if (comp == NULL) {
        ret = FILE_MERGER_ERROR_ALLOC;
        goto cleanup;
    }
    if (fread(comp, comp_size, 1, (*filebuf)->file) != 1) {
        goto cleanup;
    }
    snappy_ret = snappy_uncompressed_length(comp, comp_size, &uncomp_size);
    if (snappy_ret != SNAPPY_OK) {
        goto cleanup;
    }
    (*filebuf)->buffer.buf = (char *)malloc(uncomp_size);
    if ((*filebuf)->buffer.buf == NULL) {
        ret = FILE_MERGER_ERROR_ALLOC;
        goto cleanup;
    }
    snappy_ret = snappy_uncompress(comp, comp_size,
                                   (*filebuf)->buffer.buf,
                                   &uncomp_size);
    if (snappy_ret != SNAPPY_OK) {
        free_file_buffer(filebuf);
        goto cleanup;
    }

    (*filebuf)->buffer.size = uncomp_size;
    ret = 1;
cleanup:
    free(comp);
    return ret;
}


/* Compress buffer and write it to disk */
static file_merger_error_t
flush_file_buffer(view_file_compression_buffer_t **filebuf)
{
    size_t comp_size = snappy_max_compressed_length((*filebuf)->offset);
    file_merger_error_t ret = FILE_MERGER_ERROR_FILE_WRITE;
    char *comp = (char *) malloc(comp_size);
    if (comp == NULL) {
        return FILE_MERGER_ERROR_ALLOC;
    }
    snappy_status snappy_ret = snappy_compress((*filebuf)->buffer.buf,
                                               (*filebuf)->offset,
                                               comp,
                                               &comp_size);
    if (snappy_ret != SNAPPY_OK) {
        goto cleanup;
    }

    if (fwrite(&comp_size, sizeof(comp_size), 1,
               (*filebuf)->file) != 1) {
        goto cleanup;
    }
    if (fwrite(comp, comp_size, 1, (*filebuf)->file) != 1) {
        goto cleanup;
    }

    ret = FILE_MERGER_SUCCESS;
cleanup:
    free(comp);
    return ret;
}


static void write_into_file_buffer(void *src,
                                   size_t size,
                                   view_file_compression_buffer_t **filebuf)
{
    memcpy(&(*filebuf)->buffer.buf[(*filebuf)->offset], src, size);
    (*filebuf)->offset += size;
}


/* Allocate a buffer if there isn't any yet, or flush the current one
   and create a new one if the data that would be inserted is too big */
static file_merger_error_t
maybe_alloc_buffer(view_file_compression_buffer_t **filebuf,
                   uint32_t record_size)
{
    /* Flush buffer if it would overflow */
    if ((*filebuf)->buffer.size > 0 &&
            (*filebuf)->offset + record_size > (*filebuf)->buffer.size) {
        file_merger_error_t ret = flush_file_buffer(filebuf);
        free_file_buffer(filebuf);
        if (ret != FILE_MERGER_SUCCESS) {
            return ret;
        }
    }

    /* Allocate a new buffer if there isn't one yet */
    if ((*filebuf)->buffer.size == 0) {
        (*filebuf)->buffer.buf = (char *)malloc(
            COMPRESS_MAX_BUFFER_SIZE * sizeof(char));
        if ((*filebuf)->buffer.buf == NULL) {
            return FILE_MERGER_ERROR_ALLOC;
        }
        (*filebuf)->buffer.size = COMPRESS_MAX_BUFFER_SIZE;
        (*filebuf)->offset = 0;
    }
    return FILE_MERGER_SUCCESS;
}


int view_key_cmp(const sized_buf *key1, const sized_buf *key2,
                 const void *user_ctx)
{
    uint16_t json_key1_len = decode_raw16(*((raw_16 *) key1->buf));
    uint16_t json_key2_len = decode_raw16(*((raw_16 *) key2->buf));
    sized_buf json_key1;
    sized_buf json_key2;
    sized_buf doc_id1;
    sized_buf doc_id2;
    int res;

    (void)user_ctx;

    json_key1.buf = key1->buf + sizeof(uint16_t);
    json_key1.size = json_key1_len;
    json_key2.buf = key2->buf + sizeof(uint16_t);
    json_key2.size = json_key2_len;

    res = CollateJSON(&json_key1, &json_key2, kCollateJSON_Unicode);

    if (res == 0) {
        doc_id1.buf = key1->buf + sizeof(uint16_t) + json_key1.size;
        doc_id1.size = key1->size - sizeof(uint16_t) - json_key1.size;
        doc_id2.buf = key2->buf + sizeof(uint16_t) + json_key2.size;
        doc_id2.size = key2->size - sizeof(uint16_t) - json_key2.size;

        res = ebin_cmp(&doc_id1, &doc_id2);
    }

    return res;
}


int view_id_cmp(const sized_buf *key1, const sized_buf *key2,
                const void *user_ctx)
{
    (void)user_ctx;
    return ebin_cmp(key1, key2);
}


int read_view_record(FILE *in, void **buf, void *ctx)
{
    uint32_t len, vlen;
    uint16_t klen;
    uint8_t op = 0;
    view_file_merge_record_t *rec;
    view_file_merge_ctx_t *merge_ctx = (view_file_merge_ctx_t *) ctx;
    view_file_compression_buffer_t *filebuf = NULL;

    file_merger_error_t ret = get_file_buffer(merge_ctx, in, &filebuf);
    if (ret != FILE_MERGER_SUCCESS) {
        return ret;
    }

    /* The records within the buffer is a bit weird, but it's compatible with
       what Erlang's file_sorter module requires. */

    /* As the buffer was fully read, free it */
    if (filebuf->buffer.size != 0 &&
            filebuf->offset == filebuf->buffer.size) {
        free_file_buffer(&filebuf);
    }

    /* As the buffer is empty, fill it up again */
    if (filebuf->buffer.size == 0) {
        int ret = fill_file_buffer(&filebuf);
        /* Error or file was fully read */
        if (ret < 1) {
            return ret;
        }
    }

    cb_assert(filebuf->buffer.size > 0);

    read_from_file_buffer(&len, sizeof(len), &filebuf);

    if (is_incremental_update_record(merge_ctx)) {
        read_from_file_buffer(&op, sizeof(rec->op), &filebuf);
    }
    read_from_file_buffer(&klen, sizeof(klen), &filebuf);
    klen = ntohs(klen);
    vlen = len - sizeof(klen) - klen;
    if (is_incremental_update_record(merge_ctx)) {
        vlen -= sizeof(op);
    }

    rec = (view_file_merge_record_t *) malloc(sizeof(*rec) + klen + vlen);
    if (rec == NULL) {
        return FILE_MERGER_ERROR_ALLOC;
    }

    rec->op = op;
    rec->ksize = klen;
    rec->vsize = vlen;

    read_from_file_buffer(VIEW_RECORD_KEY(rec), klen + vlen, &filebuf);

    *buf = (void *) rec;

    return klen + vlen;
}


file_merger_error_t write_view_record(FILE *out, void *buf, bool last_record, void *ctx)
{
    view_file_merge_record_t *rec = (view_file_merge_record_t *) buf;
    uint16_t klen = htons((uint16_t) rec->ksize);
    uint32_t len;
    view_file_merge_ctx_t *merge_ctx = (view_file_merge_ctx_t *) ctx;
    view_file_compression_buffer_t *filebuf = NULL;

    file_merger_error_t ret = get_file_buffer(merge_ctx, out, &filebuf);
    if (ret != FILE_MERGER_SUCCESS) {
        return ret;
    }

    len = (uint32_t)  sizeof(klen) + rec->ksize + rec->vsize;
    if (is_incremental_update_record(merge_ctx)) {
        len += (uint32_t) sizeof(rec->op);
    }

    /* The total size a record would take in buffer */
    uint32_t total_record_size = sizeof(len) + len;
    ret = maybe_alloc_buffer(&filebuf, total_record_size);
    if (ret != FILE_MERGER_SUCCESS) {
        return ret;
    }
    cb_assert(filebuf->buffer.size > 0);

    write_into_file_buffer(&len, sizeof(len), &filebuf);
    if (is_incremental_update_record(merge_ctx)) {
        write_into_file_buffer(&rec->op, sizeof(rec->op), &filebuf);
    }
    write_into_file_buffer(&klen, sizeof(klen), &filebuf);
    write_into_file_buffer(VIEW_RECORD_KEY(rec),
                           rec->ksize + rec->vsize,
                           &filebuf);

    /* All records were inserted into the buffer, write them to disk */
    if (last_record && filebuf->offset > 0) {
        file_merger_error_t ret = flush_file_buffer(&filebuf);
        free_file_buffer(&filebuf);
        if (ret != FILE_MERGER_SUCCESS) {
            return ret;
        }
    }

    return FILE_MERGER_SUCCESS;
}


int compare_view_records(const void *r1, const void *r2, void *ctx)
{
    view_file_merge_ctx_t *merge_ctx = (view_file_merge_ctx_t *) ctx;
    view_file_merge_record_t *rec1 = (view_file_merge_record_t *) r1;
    view_file_merge_record_t *rec2 = (view_file_merge_record_t *) r2;
    sized_buf k1, k2;

    k1.size = rec1->ksize;
    k1.buf = VIEW_RECORD_KEY(rec1);

    k2.size = rec2->ksize;
    k2.buf = VIEW_RECORD_KEY(rec2);

    return merge_ctx->key_cmp_fun(&k1, &k2, merge_ctx->user_ctx);
}


size_t dedup_view_records_merger(file_merger_record_t **records, size_t len, void *ctx)
{
    size_t i;
    size_t max = 0;
    (void) ctx;

    for (i = 1; i < len; i++) {
        if (records[max]->filenum < records[i]->filenum) {
            max = i;
        }
    }

    return max;
}


void free_view_record(void *record, void *ctx)
{
    (void) ctx;
    free(record);
}


LIBCOUCHSTORE_API
char *couchstore_read_line(FILE *in, char *buf, int size)
{
    size_t len;

    if (fgets(buf, size, in) != buf) {
        return NULL;
    }

    len = strlen(buf);
    if ((len >= 1) && (buf[len - 1] == '\n')) {
        buf[len - 1] = '\0';
    }

    return buf;
}


LIBCOUCHSTORE_API
uint64_t couchstore_read_int(FILE *in, char *buf, size_t size,
                                                  couchstore_error_t *ret)
{
    uint64_t val;
    *ret = COUCHSTORE_SUCCESS;

    if (couchstore_read_line(in, buf, size) != buf) {
        *ret = COUCHSTORE_ERROR_READ;
        return 0;
    }

    if (sscanf(buf, "%"SCNu64, &val) != 1) {
        *ret = COUCHSTORE_ERROR_READ;
        return 0;
    }

    return val;
}


void set_error_info(const view_btree_info_t *info,
                    const char *red_error,
                    couchstore_error_t ret,
                    view_error_t *error_info)
{
    char buf[4096];
    size_t len = 0;

    if (ret == COUCHSTORE_SUCCESS) {
        return;
    }

    if (red_error) {
        error_info->error_msg = (const char *) strdup(red_error);
        error_info->idx_type = "MAPREDUCE";
    } else {
        /* TODO: add more human friendly messages for other error types */
        switch (ret) {
            case COUCHSTORE_ERROR_REDUCTION_TOO_LARGE:
                len = snprintf(buf, 4096, "(view %d) reduction too large, ret = %d",
                        info->view_id,
                        ret);
                buf[len] = '\0';
                error_info->idx_type = "MAPREDUCE";
                break;

            default:
                len = snprintf(buf, 4096, "(view %d) failed, ret = %d", info->view_id, ret);
                buf[len] = '\0';
        }

        if (len) {
            error_info->error_msg = (const char *) strdup(buf);
        }
    }

    if (info->num_reducers) {
        error_info->view_name = (const char *) strdup(info->names[0]);
    }
}
