/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <libcouchstore/couch_db.h>
#include <snappy-c.h>
#include "couch_btree.h"
#include "views/view_group.h"
#include "views/util.h"
#include "util.h"
#include "bitfield.h"
#include "internal.h"

#define MAX_HEADER_SIZE         (64 * 1024)

typedef enum {
    DumpBySequence,
    DumpByID,
    DumpLocals
} DumpMode;

static DumpMode mode = DumpBySequence;
static bool dumpTree = false;
static bool dumpJson = false;
static bool dumpHex = false;
static bool oneKey = false;
static bool noBody = false;
static sized_buf dumpKey;

typedef struct {
    raw_64 cas;
    raw_32 expiry;
    raw_32 flags;
} CouchbaseRevMeta;

static void printsb(const sized_buf *sb)
{
    if (sb->buf == NULL) {
        printf("null\n");
        return;
    }
    printf("%.*s\n", (int) sb->size, sb->buf);
}

static void printsbhexraw(const sized_buf* sb) {
    size_t ii;
    for (ii = 0; ii < sb->size; ++ii) {
        printf("%.02x", (uint8_t)sb->buf[ii]);
    }
}

static void printsbhex(const sized_buf *sb, int with_ascii)
{
    size_t i;

    if (sb->buf == NULL) {
        printf("null\n");
        return;
    }
    printf("{");
    for (i = 0; i < sb->size; ++i) {
        printf("%.02x", (uint8_t)sb->buf[i]);
        if (i % 4 == 3) {
            printf(" ");
        }
    }
    printf("}");
    if (with_ascii) {
        printf("  (\"");
        for (i = 0; i < sb->size; ++i) {
            uint8_t ch = sb->buf[i];
            if (ch < 32 || ch >= 127) {
                ch = '?';
            }
            printf("%c", ch);
        }
        printf("\")");
    }
    printf("\n");
}

static void printjquote(const sized_buf *sb)
{
    const char* i = sb->buf;
    const char* end = sb->buf + sb->size;
    if (sb->buf == NULL) {
        return;
    }
    for (; i < end; i++) {
        if (*i > 31 && *i != '\"' && *i != '\\') {
            fputc(*i, stdout);
        } else {
            fputc('\\', stdout);
            switch(*i)
            {
                case '\\': fputc('\\', stdout);break;
                case '\"': fputc('\"', stdout);break;
                case '\b': fputc('b', stdout);break;
                case '\f': fputc('f', stdout);break;
                case '\n': fputc('n', stdout);break;
                case '\r': fputc('r', stdout);break;
                case '\t': fputc('t', stdout);break;
                default:
                           printf("u00%.02x", *i);
            }
        }
    }
}

static int foldprint(Db *db, DocInfo *docinfo, void *ctx)
{
    int *count = (int *) ctx;
    Doc *doc = NULL;
    uint64_t cas;
    uint32_t expiry, flags;
    uint8_t datatype = 0x00, flex_code = 0x01, conf_res_mode = 0x00;
    couchstore_error_t docerr;
    (*count)++;

    if (dumpJson) {
        printf("{\"seq\":%"PRIu64",\"id\":\"", docinfo->db_seq);
        printjquote(&docinfo->id);
        printf("\",");
    } else {
        if (mode == DumpBySequence) {
            printf("Doc seq: %"PRIu64"\n", docinfo->db_seq);
            printf("     id: ");
            printsb(&docinfo->id);
        } else {
            printf("  Doc ID: ");
            printsb(&docinfo->id);
            if (docinfo->db_seq > 0) {
                printf("     seq: %"PRIu64"\n", docinfo->db_seq);
            }
        }
    }
    if (docinfo->bp == 0 && docinfo->deleted == 0 && !dumpJson) {
        printf("         ** This b-tree node is corrupt; raw node value follows:*\n");
        printf("    raw: ");
        printsbhex(&docinfo->rev_meta, 1);
        return 0;
    }
    if (dumpJson) {
        printf("\"rev\":%"PRIu64",\"content_meta\":%d,", docinfo->rev_seq,
                                                         docinfo->content_meta);
        printf("\"physical_size\":%"PRIu64",", (uint64_t)docinfo->size);
    } else {
        printf("     rev: %"PRIu64"\n", docinfo->rev_seq);
        printf("     content_meta: %d\n", docinfo->content_meta);
        printf("     size (on disk): %"PRIu64"\n", (uint64_t)docinfo->size);
    }
    if (docinfo->rev_meta.size >= sizeof(CouchbaseRevMeta)) {
        const CouchbaseRevMeta* meta = (const CouchbaseRevMeta*)docinfo->rev_meta.buf;
        cas = decode_raw64(meta->cas);
        expiry = decode_raw32(meta->expiry);
        flags = decode_raw32(meta->flags);
        if (docinfo->rev_meta.size > sizeof(CouchbaseRevMeta)) {
            // 18 bytes of rev_meta indicates CouchbaseRevMeta along with
            // flex_meta_code (1B) and datatype (1B)
            if (docinfo->rev_meta.size < sizeof(CouchbaseRevMeta) + 2) {
                printf("     Error parsing the document: Possible corruption\n");
                return 1;
            }
            flex_code = *((uint8_t *)(docinfo->rev_meta.buf + sizeof(CouchbaseRevMeta)));
            if (flex_code < 0x01) {
                printf("     Error: Flex code mismatch (bad code: %d)\n",
                       flex_code);
                return 1;
            }
            datatype = *((uint8_t *)(docinfo->rev_meta.buf + sizeof(CouchbaseRevMeta) +
                        sizeof(uint8_t)));
            if (docinfo->rev_meta.size > sizeof(CouchbaseRevMeta) + 2) {
                // 19 bytes of rev_meta indicates CouchbaseRevMeta along with
                // flex_meta_code (1B) and datatype (1B), along with the conflict
                // resolution flag (1B).
                conf_res_mode = *((uint8_t *)(docinfo->rev_meta.buf +
                                  sizeof(CouchbaseRevMeta) + sizeof(uint8_t) +
                                  sizeof(uint8_t)));

                if (dumpJson) {
                    printf("\"cas\":\"%"PRIu64"\",\"expiry\":%"PRIu32",\"flags\":%"PRIu32","
                           "\"datatype\":%d,\"conflict_resolution_mode\":%d,",
                            cas, expiry, flags, datatype, conf_res_mode);
                } else {
                    printf("     cas: %"PRIu64", expiry: %"PRIu32", flags: %"PRIu32", "
                           "datatype: %d, conflict_resolution_mode: %d\n",
                           cas, expiry, flags, datatype, conf_res_mode);
                }
            } else {
                if (dumpJson) {
                    printf("\"cas\":\"%"PRIu64"\",\"expiry\":%"PRIu32",\"flags\":%"PRIu32","
                           "\"datatype\":%d,",
                            cas, expiry, flags, datatype);
                } else {
                    printf("     cas: %"PRIu64", expiry: %"PRIu32", flags: %"PRIu32", "
                           "datatype: %d\n",
                           cas, expiry, flags, datatype);
                }
            }
        } else {
            if (dumpJson) {
                printf("\"cas\":\"%"PRIu64"\",\"expiry\":%"PRIu32",\"flags\":%"PRIu32",",
                        cas, expiry, flags);
            } else {
                printf("     cas: %"PRIu64", expiry: %"PRIu32", flags: %"PRIu32"\n",
                        cas, expiry, flags);
            }
        }
    }
    if (docinfo->deleted) {
        if (dumpJson) {
            printf("\"deleted\":true,");
        } else {
            printf("     doc deleted\n");
        }
    }

    if (!noBody) {
        docerr = couchstore_open_doc_with_docinfo(db, docinfo, &doc, DECOMPRESS_DOC_BODIES);
        if (docerr != COUCHSTORE_SUCCESS) {
            if (dumpJson) {
                printf("\"body\":null}\n");
            } else {
                printf("     could not read document body: %s\n", couchstore_strerror(docerr));
            }
        } else if (doc) {
            sized_buf new_body;
            if (datatype >= 0x02) {
                size_t rlen;
                snappy_uncompressed_length(doc->data.buf, doc->data.size, &rlen);
                char *decbuf = (char *) malloc(rlen);
                size_t new_len;
                snappy_uncompress(doc->data.buf, doc->data.size, decbuf, &new_len);
                new_body.size = new_len;
                new_body.buf = decbuf;
            } else {
                new_body = doc->data;
            }
            if (dumpJson) {
                printf("\"size\":%"PRIu64",", (uint64_t)new_body.size);
                if (docinfo->content_meta & COUCH_DOC_IS_COMPRESSED) {
                    printf("\"snappy\":true,\"body\":\"");
                } else {
                    printf("\"body\":\"");
                }
                printjquote(&new_body);
                printf("\"}\n");
            } else {
                printf("     size: %"PRIu64"\n", (uint64_t)new_body.size);
                printf("     data:%s",
                       docinfo->content_meta & COUCH_DOC_IS_COMPRESSED ?
                       " (snappy) " : " ");
                if (dumpHex) {
                    printsbhexraw(&new_body);
                    printf("\n");
                } else {
                    printsb(&new_body);
                }
            }
            if (datatype >= 0x02) {
                free (new_body.buf);
            }
        }
    } else {
        if (dumpJson) {
            printf("\"body\":null}\n");
        } else {
            printf("\n");
        }
    }

    couchstore_free_document(doc);
    return 0;
}


static int visit_node(Db *db,
                      int depth,
                      const DocInfo* docinfo,
                      uint64_t subtreeSize,
                      const sized_buf* reduceValue,
                      void *ctx)
{
    int i;
    (void) db;

    for (i = 0; i < depth; ++i)
        printf("  ");
    if (reduceValue) {
        /* This is a tree node: */
        printf("+ (%"PRIu64") ", subtreeSize);
        printsbhex(reduceValue, 0);
    } else if (docinfo->bp > 0) {
        int *count;
        /* This is a document: */
        printf("%c (%"PRIu64") ", (docinfo->deleted ? 'x' : '*'),
               (uint64_t)docinfo->size);
        if (mode == DumpBySequence) {
            printf("#%"PRIu64" ", docinfo->db_seq);
        }
        printsb(&docinfo->id);

        count = (int *) ctx;
        (*count)++;
    } else {
        /* Document, but not in a known format: */
        printf("**corrupt?** ");
        printsbhex(&docinfo->rev_meta, 1);
    }
    return 0;
}

static couchstore_error_t local_doc_print(couchfile_lookup_request *rq,
                                          const sized_buf *k,
                                          const sized_buf *v)
{
    int* count = (int*) rq->callback_ctx;
    if (!v) {
        return COUCHSTORE_ERROR_DOC_NOT_FOUND;
    }
    (*count)++;
    sized_buf *id = (sized_buf *) k;
    if (dumpJson) {
        printf("{\"id\":\"");
        printjquote(id);
        printf("\",");
    } else {
        printf("Key: ");
        printsb(id);
    }

    if (dumpJson) {
        printf("\"value\":\"");
        printjquote(v);
        printf("\"}\n");
    } else {
        printf("Value: ");
        printsb(v);
        printf("\n");
    }

    return COUCHSTORE_SUCCESS;
}

static couchstore_error_t couchstore_print_local_docs(Db *db, int *count)
{
    sized_buf key;
    sized_buf *keylist = &key;
    couchfile_lookup_request rq;
    couchstore_error_t errcode;

    if (db->header.local_docs_root == NULL) {
        if (oneKey) {
            return COUCHSTORE_ERROR_DOC_NOT_FOUND;
        } else {
            return COUCHSTORE_SUCCESS;
        }
    }

    key.buf = (char *)"\0";
    key.size = 0;

    rq.cmp.compare = ebin_cmp;
    rq.file = &db->file;
    rq.num_keys = 1;
    rq.keys = &keylist;
    rq.callback_ctx = count;
    rq.fetch_callback = local_doc_print;
    rq.node_callback = NULL;
    rq.fold = 1;

    if (oneKey) {
        rq.fold = 0;
        key = dumpKey;
    }

    errcode = btree_lookup(&rq, db->header.local_docs_root->pointer);
    return errcode;
}

// Attempts to initialize the database from a header at the given file position
static couchstore_error_t find_view_header_at_pos(view_group_info_t *info,
                                                  cs_off_t pos)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    union {
        raw_file_header *raw;
        char *buf;
    } header_buf = { NULL };
    uint8_t buf;

    printf("vmx: trying to find header at %d\n", (int)pos);
    ssize_t readsize = info->file.ops->pread(&info->file.lastError,
                                             info->file.handle,
                                             &buf, 1, pos);

    //printf("vmx: readsize: %d\n", readsize);
    error_unless(readsize == 1, COUCHSTORE_ERROR_READ);
    if (buf == 0) {
        printf("vmx: no header found\n");
        return COUCHSTORE_ERROR_NO_HEADER;
    } else if (buf != 1) {
        printf("vmx: header corrupt\n");
        return COUCHSTORE_ERROR_CORRUPT;
    }

    info->header_pos = pos;

    return COUCHSTORE_SUCCESS;
    /*

    ssize_t readsize = db->file.ops->pread(&db->file.lastError, db->file.handle,
                                           buf, 2, pos);
    error_unless(readsize == 2, COUCHSTORE_ERROR_READ);
    if (buf[0] == 0) {
        return COUCHSTORE_ERROR_NO_HEADER;
    } else if (buf[0] != 1) {
        return COUCHSTORE_ERROR_CORRUPT;
    }

    header_len = pread_header(&db->file, pos, &header_buf.buf, MAX_DB_HEADER_SIZE);
    if (header_len < 0) {
        error_pass(static_cast<couchstore_error_t>(header_len));
    }

    db->header.position = pos;
    db->header.disk_version = decode_raw08(header_buf.raw->version);

    // Only 12 and 11 are valid
    error_unless(db->header.disk_version == COUCH_DISK_VERSION ||
                 db->header.disk_version == COUCH_DISK_VERSION_11,
                 COUCHSTORE_ERROR_HEADER_VERSION);
    db->header.update_seq = decode_raw48(header_buf.raw->update_seq);
    db->header.purge_seq = decode_raw48(header_buf.raw->purge_seq);
    db->header.purge_ptr = decode_raw48(header_buf.raw->purge_ptr);
    error_unless(db->header.purge_ptr <= db->header.position, COUCHSTORE_ERROR_CORRUPT);
    seqrootsize = decode_raw16(header_buf.raw->seqrootsize);
    idrootsize = decode_raw16(header_buf.raw->idrootsize);
    localrootsize = decode_raw16(header_buf.raw->localrootsize);
    error_unless(header_len == HEADER_BASE_SIZE + seqrootsize + idrootsize + localrootsize,
                 COUCHSTORE_ERROR_CORRUPT);

    root_data = (char*) (header_buf.raw + 1);  // i.e. just past *header_buf
    error_pass(read_db_root(db, &db->header.by_seq_root, root_data, seqrootsize));
    root_data += seqrootsize;
    error_pass(read_db_root(db, &db->header.by_id_root, root_data, idrootsize));
    root_data += idrootsize;
    error_pass(read_db_root(db, &db->header.local_docs_root, root_data, localrootsize));
*/
cleanup:
    free(header_buf.raw);
    return errcode;
}

// Finds the view header by scanning back from the end of the file at
// 4k boundaries
static couchstore_error_t find_view_header(view_group_info_t *info,
                                           int64_t start_pos)
{
    couchstore_error_t last_header_errcode = COUCHSTORE_ERROR_NO_HEADER;
    int64_t pos = start_pos;
    printf("vmx: find_view_header: pos1: %"PRIu64"\n", pos);
    pos -= pos % COUCH_BLOCK_SIZE;
    printf("vmx: find_view_header: pos2: %"PRIu64"\n", pos);
    for (; pos >= 0; pos -= COUCH_BLOCK_SIZE) {
        couchstore_error_t errcode = find_view_header_at_pos(info, pos);
        switch(errcode) {
            case COUCHSTORE_SUCCESS:
                // Found it!
                return COUCHSTORE_SUCCESS;
            case COUCHSTORE_ERROR_NO_HEADER:
                // No header here, so keep going
                break;
            case COUCHSTORE_ERROR_ALLOC_FAIL:
                // Fatal error
                return errcode;
            default:
                // Invalid header; continue, but remember the last error
                last_header_errcode = errcode;
                break;
        }
    }
    return last_header_errcode;
}


// From view_group.c
static int view_btree_cmp(const sized_buf *key1, const sized_buf *key2)
{
    return view_key_cmp(key1, key2, NULL);
}


// btree_lookup callback, called while iterating keys
static couchstore_error_t lookup_callback(couchfile_lookup_request *rq,
                                          const sized_buf *k,
                                          const sized_buf *v)
{
    uint16_t json_key_len = decode_raw16(*((raw_16 *) k->buf));
    sized_buf json_key;

    json_key.buf = k->buf + sizeof(uint16_t);
    json_key.size = json_key_len;

    printf("key: %.*s\n", (int)json_key.size, json_key.buf);
    return COUCHSTORE_SUCCESS;
}


static int process_file(const char *file, int *total)
{
    Db *db;
    view_group_info_t *info;
    couchstore_error_t errcode;
    int count = 0;
    index_header_t *header = NULL;
    char *header_buf = NULL;
    int header_len;

    info = (view_group_info_t *) calloc(1, sizeof(*info));
    if (info == NULL) {
        fprintf(stderr, "Memory allocation failure\n");
        return -1;
    }
    info->type = VIEW_INDEX_TYPE_MAPREDUCE;

    //errcode = couchstore_open_db(file, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
    errcode = open_view_group_file(file, COUCHSTORE_OPEN_FLAG_RDONLY, &info->file);
    if (errcode != COUCHSTORE_SUCCESS) {
        fprintf(stderr, "Failed to open \"%s\": %s\n",
                file, couchstore_strerror(errcode));
        return -1;
    } else {
        printf("Dumping \"%s\":\n", file);
    }

    // The header is at the end of the file, hence set the file pointer to
    // the end before searching
    info->file.pos = info->file.ops->goto_eof(&info->file.lastError,
                                              info->file.handle);
    printf("file pos %"PRIu64"\n", info->file.pos);
    find_view_header(info, info->file.pos - 2);
    //find_view_header(info, info->file.pos);

    printf("header pos %"PRIu64"\n", info->header_pos);

    header_len = pread_header(&info->file, (cs_off_t)info->header_pos, &header_buf,
                              MAX_HEADER_SIZE);
    if (header_len < 0) {
        error_pass((couchstore_error_t)header_len);
    }

    errcode = decode_index_header(header_buf, (size_t) header_len, &header);

    printf("num views: %d\n", header->num_views);

    for (int i = 0; i < header->num_views; ++i) {
        printf("view %d root node pos: %"PRIu64"\n",
               i, header->view_states[i]->pointer);

        sized_buf nullkey = {NULL, 0};
        sized_buf *lowkeys = &nullkey;
        couchfile_lookup_request rq;
        //lookup_context lookup_ctx = {info, 0, NULL, NULL, (tree == db->header.by_id_root), 0, NULL};

        rq.cmp.compare = view_btree_cmp;
        rq.file = &info->file;
        rq.num_keys = 1;
        rq.keys = &lowkeys;
        //rq.callback_ctx = &lookup_ctx;
        rq.callback_ctx = NULL;
        rq.fetch_callback = lookup_callback;
        rq.node_callback = NULL;
        rq.fold = 1;

        btree_lookup(&rq, header->view_states[i]->pointer);
    }

cleanup:
    exit(10);

    switch (mode) {
        case DumpBySequence:
            if (dumpTree) {
                errcode = couchstore_walk_seq_tree(db, 0, COUCHSTORE_INCLUDE_CORRUPT_DOCS,
                                                   visit_node, &count);
            } else {
                errcode = couchstore_changes_since(db, 0, COUCHSTORE_INCLUDE_CORRUPT_DOCS,
                                                   foldprint, &count);
            }
            break;
        case DumpByID:
            if (dumpTree) {
                errcode = couchstore_walk_id_tree(db, NULL, COUCHSTORE_INCLUDE_CORRUPT_DOCS,
                                                  visit_node, &count);
            } else if (oneKey) {
                DocInfo* info;
                errcode = couchstore_docinfo_by_id(db, dumpKey.buf, dumpKey.size, &info);
                if (errcode == COUCHSTORE_SUCCESS) {
                    foldprint(db, info, &count);
                    couchstore_free_docinfo(info);
                }
            } else {
                errcode = couchstore_all_docs(db, NULL, COUCHSTORE_INCLUDE_CORRUPT_DOCS,
                                              foldprint, &count);
            }
            break;
        case DumpLocals:
            errcode = couchstore_print_local_docs(db, &count);
            break;
    }
    (void)couchstore_close_db(db);

    if (errcode < 0) {
        fprintf(stderr, "Failed to dump database \"%s\": %s\n",
                file, couchstore_strerror(errcode));
        return -1;
    }

    *total += count;
    return 0;
}

static void usage(void) {
    printf("USAGE: couch_dbdump [options] file.couch [file2.couch ...]\n");
    printf("\nOptions:\n");
    printf("       --hex-body   convert document body data to hex (for binary data)\n");
    printf("       --no-body    don't retrieve document bodies (metadata only, faster)\n");
    printf("       --byid       sort output by document ID\n");
    printf("       --view       sort output by the view (default)\n");
    printf("       --json       dump data as JSON objects (one per line)\n");
    printf("\nAlternate modes:\n");
    printf("       --tree       show file b-tree structure instead of data\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char **argv)
{
    int error = 0;
    int count = 0;
    int ii = 1;

    if (argc < 2) {
        usage();
    }

    while (ii < argc && strncmp(argv[ii], "-", 1) == 0) {
        if (strcmp(argv[ii], "--byid") == 0) {
            mode = DumpByID;
        } else if (strcmp(argv[ii], "--view") == 0) {
            mode = DumpBySequence;
        } else if (strcmp(argv[ii], "--tree") == 0) {
            dumpTree = true;
        } else if (strcmp(argv[ii], "--json") == 0) {
            dumpJson = true;
        } else if (strcmp(argv[ii], "--hex-body") == 0) {
            dumpHex = true;
        } else if (strcmp(argv[ii], "--no-body") == 0) {
            noBody = true;
        } else {
            usage();
        }
        ++ii;
    }

    if (ii >= argc) {
        usage();
    }

    for (; ii < argc; ++ii) {
        error += process_file(argv[ii], &count);
    }

    printf("\nTotal docs: %d\n", count);
    if (error) {
        exit(EXIT_FAILURE);
    } else {
        exit(EXIT_SUCCESS);
    }
}
