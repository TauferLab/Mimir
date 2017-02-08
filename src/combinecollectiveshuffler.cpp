#include <stdio.h>
#include <stdlib.h>
#include "log.h"
#include "config.h"
#include "combinecollectiveshuffler.h"
#include "const.h"
#include "memory.h"
#include "kvcontainer.h"

#include "globals.h"
#include "hash.h"
#include "stat.h"
#include "log.h"

#include "recordformat.h"

using namespace MIMIR_NS;

CombineCollectiveShuffler::CombineCollectiveShuffler(CombineCallback user_combine,
                                                     void *user_ptr,
                                                     Writable *out,
                                                     HashCallback user_hash)
    : CollectiveShuffler(out, user_hash)
{
    this->user_combine = user_combine;
    this->user_ptr = user_ptr;
    bucket = NULL;
}

CombineCollectiveShuffler::~CombineCollectiveShuffler() {
}

bool CombineCollectiveShuffler::open() {
    CollectiveShuffler::open();
    bucket = new CombinerHashBucket();
    return true;
}

void CombineCollectiveShuffler::close() {
    garbage_collection();
    CollectiveShuffler::close();
    delete bucket;
}

void CombineCollectiveShuffler::write(KVRecord *record)
{
    int target = get_target_rank(record->get_key(),
                                 record->get_key_size());

    int kvsize = record->get_record_size();
    if (kvsize > buf_size)
        LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n", 
                  kvsize, buf_size);

    CombinerUnique *u = bucket->findElem(record->get_key(), 
                                         record->get_key_size());
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
                kv = *record;

                if (iter->second == kvsize)
                    slices.erase(iter);
                else
                    slices[iter->first] -= kvsize;

                bucket->insertElem(&tmp);

                break;
            }
        }

        if (iter == slices.end()) {
            if ((int64_t)send_offset[target] + (int64_t) kvsize > buf_size) {
                garbage_collection();
                exchange_kv();
            }
            tmp.kv = send_buffer + target * (int64_t)buf_size + send_offset[target];
            kv.set_buffer(tmp.kv);
            kv = *record;
            send_offset[target] += kvsize;

            bucket->insertElem(&tmp);
        }
    }
    else {
        kv.set_buffer(u->kv);
        user_combine(this, &kv, record, user_ptr);
    }

    return;
}

void CombineCollectiveShuffler::update(KVRecord *record)
{
    int target = get_target_rank(record->get_key(),
                                 record->get_key_size());

    int kvsize = record->get_key_size();
    int ukvsize = kv.get_key_size();

    if (kvsize > buf_size)
        LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n", 
                  kvsize, buf_size);

    if (kvsize != ukvsize 
        || memcmp(record->get_key(), kv.get_key(), kvsize) != 0)
        LOG_ERROR("Error: the result key of combiner is different!\n");

    if (kvsize <= ukvsize) {
        kv = *record;
        if (kvsize < ukvsize)
            slices.insert(std::make_pair(kv.get_record() + ukvsize - kvsize, 
                                         ukvsize - kvsize));
    }
    else {
        if ((int64_t)send_offset[target] + (int64_t) kvsize > buf_size) {
            garbage_collection();
            exchange_kv();
        }
        slices.insert(std::make_pair(kv.get_record(), ukvsize));
        int64_t global_buf_off = target * (int64_t) buf_size + send_offset[target];
        char *gbuf = send_buffer + global_buf_off;
        kv.set_buffer(gbuf);
        kv = *record;
        send_offset[target] += kvsize;
    }

    return;
}

void CombineCollectiveShuffler::garbage_collection()
{
    if (!slices.empty()) {
        LOG_PRINT(DBG_MEM, "Alltoall: garbege collection (size=%ld)\n", slices.size());

        int dst_off = 0, src_off = 0;
        char *dst_buf = NULL, *src_buf = NULL;

        for (int k = 0; k < mimir_world_size; k++) {
            dst_off = src_off = 0;
            int64_t global_buf_off = k * (int64_t) buf_size;
            while (src_off < send_offset[k]) {

                src_buf = send_buffer + global_buf_off + src_off;
                dst_buf = send_buffer + global_buf_off + dst_off;

                std::unordered_map < char *, int >::iterator iter = slices.find(src_buf);
                if (iter != slices.end()) {
                    src_off += iter->second;
                }
                else {
                    //char *key = NULL, *value = NULL;
                    //int keybytes = 0, valuebytes = 0, kvsize = 0;
                    //GET_KV_VARS(kv->ksize, kv->vsize, src_buf, key, keybytes,
                    //            value, valuebytes, kvsize);
                    kv.set_buffer(src_buf);
                    int kvsize = kv.get_record_size();
                    src_buf += kvsize;
                    if (src_off != dst_off)
                        memcpy(dst_buf, src_buf - kvsize, kvsize);
                    dst_off += kvsize;
                    src_off += kvsize;
                }
            }
            send_offset[k] = dst_off;
        }
        slices.clear();
    }
}
