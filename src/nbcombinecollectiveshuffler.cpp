/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <stdlib.h>
#include "log.h"
#include "stat.h"
#include "config.h"
#include "memory.h"
#include "hash.h"
#include "recordformat.h"
#include "kvcontainer.h"
#include "nbcombinecollectiveshuffler.h"

using namespace MIMIR_NS;

NBCombineCollectiveShuffler::NBCombineCollectiveShuffler(CombineCallback user_combine,
                                                         void *user_ptr,
                                                         Writable *out,
                                                         HashCallback user_hash)
    : NBCollectiveShuffler(out, user_hash)
{
    this->user_combine = user_combine;
    this->user_ptr = user_ptr;
    bucket = NULL;
}

NBCombineCollectiveShuffler::~NBCombineCollectiveShuffler()
{
}

bool NBCombineCollectiveShuffler::open() {
    NBCollectiveShuffler::open();
    bucket = new CombinerHashBucket();

    return true;
}

void NBCombineCollectiveShuffler::close() {
    garbage_collection();
    delete bucket;

    NBCollectiveShuffler::close();
}

void NBCombineCollectiveShuffler::write(BaseRecordFormat *record)
{
    int target = get_target_rank(((KVRecord*)record)->get_key(),
                                 ((KVRecord*)record)->get_key_size());

    int kvsize = record->get_record_size();
    if (kvsize > buf_size)
        LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n", 
                  kvsize, buf_size);

    CombinerUnique *u = bucket->findElem(((KVRecord*)record)->get_key(), 
                                         ((KVRecord*)record)->get_key_size());

    if (u == NULL) {
        CombinerUnique tmp;
        tmp.next = NULL;

        std::unordered_map < char *, int >::iterator iter;
        for (iter = slices.begin(); iter != slices.end(); iter++) {
            char *sbuf = iter->first;
            int ssize = iter->second;

            if (ssize >= kvsize) {
                tmp.kv = sbuf + (ssize - kvsize);
                kv.set_buffer(tmp.kv);
                kv.convert((KVRecord*)record);

                if (iter->second == kvsize)
                    slices.erase(iter);
                else
                    slices[iter->first] -= kvsize;

                bucket->insertElem(&tmp);

                break;
            }
        }

        if (iter == slices.end()) {
            if ((int64_t)send_offsets[cur_idx][target] + (int64_t) kvsize > buf_size) {
                garbage_collection();
                while (!done_kv_exchange()) {
                    push_kv_exchange();
                }
                start_kv_exchange();
            }
            tmp.kv = send_buffers[cur_idx] + target * (int64_t)buf_size 
                + send_offsets[cur_idx][target];
            kv.set_buffer(tmp.kv);
            kv.convert((KVRecord*)record);
            send_offsets[cur_idx][target] += kvsize;
        }

        bucket->insertElem(&tmp);
    }
    else {
        kv.set_buffer(u->kv);
        user_combine(this, &kv, (KVRecord*)record, user_ptr);
    }

    return;
}

void NBCombineCollectiveShuffler::update(BaseRecordFormat *record)
{
    int target = get_target_rank(((KVRecord*)record)->get_key(),
                                 ((KVRecord*)record)->get_key_size());

    int kvsize = record->get_record_size();
    int ukvsize = kv.get_record_size();
    int ksize = kv.get_key_size();

    if (kvsize > buf_size)
        LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n", 
                  kvsize, buf_size);

    if (((KVRecord*)record)->get_key_size() != kv.get_key_size()
        || memcmp(((KVRecord*)record)->get_key(), kv.get_key(), ksize) != 0)
        LOG_ERROR("Error: the result key of combiner is different!\n");

    if (kvsize <= ukvsize) {
        kv.convert((KVRecord*)record);
        if (kvsize < ukvsize)
            slices.insert(std::make_pair(kv.get_record() + ukvsize - kvsize, 
                                         ukvsize - kvsize));
    }
    else {
        if ((int64_t)send_offsets[cur_idx][target] + (int64_t) kvsize > buf_size) {
            garbage_collection();
            while (!done_kv_exchange()) {
                push_kv_exchange();
            }
            start_kv_exchange();
        }
        slices.insert(std::make_pair(kv.get_record(), ukvsize));
        char *gbuf = send_buffers[cur_idx] + target * (int64_t) buf_size + send_offsets[cur_idx][target];
        kv.set_buffer(gbuf);
        kv.convert((KVRecord*)record);
        send_offsets[cur_idx][target] += kvsize;
    }

    return;
}

void NBCombineCollectiveShuffler::garbage_collection()
{
    if (!slices.empty()) {

        int dst_off = 0, src_off = 0;
        char *dst_buf = NULL, *src_buf = NULL;

        for (int k = 0; k < mimir_world_size; k++) {
            src_buf = send_buffers[cur_idx] + k * (int64_t)buf_size;
            dst_buf = send_buffers[cur_idx] + k * (int64_t)buf_size;

            dst_off = src_off = 0;
            while (src_off < send_offsets[cur_idx][k]) {

                std::unordered_map < char *, int >::iterator iter = slices.find(src_buf);
                if (iter != slices.end()) {
                    src_off += iter->second;
                }
                else {
                    kv.set_buffer(src_buf);
                    int kvsize = kv.get_record_size();
                    if (src_off != dst_off) {
                        for (int kk = 0; kk < kvsize; kk++)
                            dst_buf[dst_off + kk] = src_buf[src_off + kk];
                    }
                    dst_off += kvsize;
                    src_off += kvsize;
                }
            }
            send_offsets[cur_idx][k] = dst_off;
        }
        slices.clear();
    }
    bucket->clear();
}
