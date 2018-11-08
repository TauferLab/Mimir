//
// (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego
//     Supercomputer Center, National University of Defense Technology,
//     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
//
//     See COPYRIGHT in top-level directory.
//

#ifndef MIMIR_COMBINE_NB_COLLECTIVE_SHUFFLER_H
#define MIMIR_COMBINE_NB_COLLECTIVE_SHUFFLER_H

#include <mpi.h>
#include <vector>

#include <unordered_map>

#include "container.h"
#include "nbcollectiveshuffler.h"

namespace MIMIR_NS {

template <typename KeyType, typename ValType>
class NBCombineCollectiveShuffler
    : public NBCollectiveShuffler<KeyType, ValType>,
      public Combinable<KeyType, ValType>
{
  public:
    NBCombineCollectiveShuffler(
        MPI_Comm comm,
        void (*user_combine)(Combinable<KeyType, ValType> *output, KeyType *key,
                             ValType *val1, ValType *val2, ValType *val3,
                             void *ptr),
        void *user_ptr, Writable<KeyType, ValType> *out,
        int (*user_hash)(KeyType *key, ValType *val, int npartition),
        int keycount, int valcount, bool split_hint, HashBucket<> *h)
        : NBCollectiveShuffler<KeyType, ValType>(comm, out, user_hash, keycount,
                                                 valcount, split_hint, h)
    {
        this->user_combine = user_combine;
        this->user_ptr = user_ptr;
        bucket = NULL;
    }

    virtual ~NBCombineCollectiveShuffler() {}

    virtual int open()
    {
        NBCollectiveShuffler<KeyType, ValType>::open();
        bucket = new HashBucket<CombinerVal>();

        return 0;
    }

    virtual void close()
    {
        garbage_collection();
        delete bucket;

        NBCollectiveShuffler<KeyType, ValType>::close();
    }

    virtual int write(KeyType *key, ValType *val)
    {
        int target = this->get_target_rank(key, val);

        if (target == this->shuffle_rank) {
            this->out->write(key, val);
            return 0;
        }

        int kvsize = this->ser->get_kv_bytes(key, val);
        if (kvsize > this->buf_size)
            LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n",
                      kvsize, this->buf_size);

        int keysize = this->ser->get_key_bytes(key);
        char *keyptr = this->ser->get_key_ptr(key);
        u = bucket->findEntry(keyptr, keysize);

        if (u == NULL) {
            CombinerVal tmp;

            std::unordered_map<char *, int>::iterator iter;
            char *range_start = this->msg_buffers[this->cur_idx].send_buffer
                                + target * (int64_t) this->buf_size;
            char *range_end
                = this->msg_buffers[this->cur_idx].send_buffer
                  + target * (int64_t) this->buf_size
                  + this->msg_buffers[this->cur_idx].send_offset[target];
            for (iter = slices.begin(); iter != slices.end(); iter++) {
                char *sbuf = iter->first;
                int ssize = iter->second;

                if (sbuf >= range_start && sbuf < range_end
                    && ssize >= kvsize) {
                    tmp.kv = sbuf + (ssize - kvsize);
                    //kv.set_buffer(tmp.kv);
                    //kv.convert((KVRecord*)record);
                    this->ser->kv_to_bytes(key, val, tmp.kv, kvsize);
                    if (iter->second == kvsize)
                        slices.erase(iter);
                    else
                        slices[iter->first] -= kvsize;

                    break;
                }
            }

            if (iter == slices.end()) {
                if ((int64_t) this->msg_buffers[this->cur_idx]
                            .send_offset[target]
                        + (int64_t) kvsize
                    > this->buf_size) {
                    garbage_collection();
                    this->start_kv_exchange();
                }
                tmp.kv = this->msg_buffers[this->cur_idx].send_buffer
                         + target * (int64_t) this->buf_size
                         + this->msg_buffers[this->cur_idx].send_offset[target];
                //kv.set_buffer(tmp.kv);
                //kv.convert((KVRecord*)record);
                this->ser->kv_to_bytes(key, val, tmp.kv, kvsize);
                this->msg_buffers[this->cur_idx].send_offset[target] += kvsize;
            }

            bucket->insertEntry(tmp.kv, keysize, &tmp);
            this->kvcount++;
        }
        else {
            typename SafeType<KeyType>::ptrtype u_key = NULL;
            typename SafeType<ValType>::ptrtype u_val = NULL;
            typename SafeType<ValType>::type r_val[this->valcount];

            int ukvsize = this->ser->kv_from_bytes(&u_key, &u_val, u->kv,
                                                   MAX_RECORD_SIZE);

            user_combine(this, u_key, u_val, val, r_val, user_ptr);

            int ukeysize = this->ser->get_key_bytes(u_key);
            int uvalsize = this->ser->get_val_bytes(u_val);
            int rvalsize = this->ser->get_val_bytes(r_val);

            if (rvalsize <= uvalsize) {
                this->ser->val_to_bytes(r_val, u->kv + ukeysize, uvalsize);
                if (rvalsize < uvalsize) {
                    char *ptr = u->kv + ukvsize - (uvalsize - rvalsize);
                    this->slices.insert(
                        std::make_pair(ptr, uvalsize - rvalsize));
                }
            }
            else {
                slices.insert(std::make_pair(u->kv, ukvsize));
                if ((int64_t) this->msg_buffers[this->cur_idx]
                            .send_offset[target]
                        + (int64_t)(ukeysize + rvalsize)
                    > this->buf_size) {
                    garbage_collection();
                    this->start_kv_exchange();
                    target = this->get_target_rank(key, val);
                    //u = NULL;
                }
                char *gbuf
                    = this->msg_buffers[this->cur_idx].send_buffer
                      + target * (int64_t) this->buf_size
                      + this->msg_buffers[this->cur_idx].send_offset[target];
                this->ser->kv_to_bytes(u_key, r_val, gbuf, ukeysize + rvalsize);
                this->msg_buffers[this->cur_idx].send_offset[target]
                    += ukeysize + rvalsize;
                if (u != NULL) u->kv = gbuf;
            }
        }

        return 0;
    }

    virtual void make_progress(bool issue_new = false)
    {
        if (issue_new && this->pending_msg == 0) {
            garbage_collection();
            this->start_kv_exchange();
        }
        this->push_kv_exchange();
    }

  protected:
    void garbage_collection()
    {
        if (!slices.empty()) {
            typename SafeType<KeyType>::ptrtype key = NULL;
            typename SafeType<ValType>::ptrtype val = NULL;

            LOG_PRINT(DBG_GEN,
                      "NBCollectiveShuffler garbage collection: slices=%ld\n",
                      slices.size());

            int dst_off = 0, src_off = 0;
            char *dst_buf = NULL, *src_buf = NULL;

            for (int k = 0; k < this->shuffle_size; k++) {
                src_buf = this->msg_buffers[this->cur_idx].send_buffer
                          + k * (int64_t) this->buf_size;
                dst_buf = this->msg_buffers[this->cur_idx].send_buffer
                          + k * (int64_t) this->buf_size;

                dst_off = src_off = 0;
                while (src_off
                       < this->msg_buffers[this->cur_idx].send_offset[k]) {
                    char *tmp_buf = src_buf + src_off;
                    std::unordered_map<char *, int>::iterator iter
                        = slices.find(tmp_buf);
                    if (iter != slices.end()) {
                        src_off += iter->second;
                    }
                    else {
                        //kv.set_buffer(tmp_buf);
                        //int kvsize = kv.get_record_size();
                        int kvsize = this->ser->kv_from_bytes(
                            &key, &val, tmp_buf,
                            this->msg_buffers[this->cur_idx].send_offset[k]
                                - src_off);
                        if (src_off != dst_off) {
                            for (int kk = 0; kk < kvsize; kk++)
                                dst_buf[dst_off + kk] = src_buf[src_off + kk];
                        }
                        dst_off += kvsize;
                        src_off += kvsize;
                    }
                }
                this->msg_buffers[this->cur_idx].send_offset[k] = dst_off;
            }
            slices.clear();
        }
        bucket->clear();
    }

    void (*user_combine)(Combinable<KeyType, ValType> *output, KeyType *key,
                         ValType *val1, ValType *val2, ValType *val3,
                         void *ptr);
    void *user_ptr;
    std::unordered_map<char *, int> slices;
    HashBucket<CombinerVal> *bucket;
    CombinerVal *u;
};

} // namespace MIMIR_NS
#endif
