/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
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
    : public NBCollectiveShuffler<KeyType,ValType>, 
    public Combinable<KeyType,ValType> {
public:
    NBCombineCollectiveShuffler(MPI_Comm comm,
                                void (*user_combine)(Combinable<KeyType,ValType> *output,
                                                     KeyType *key, ValType *val1, ValType *val2, void *ptr),
                                void *user_ptr,
                                Writable<KeyType,ValType> *out,
                                HashCallback user_hash,
                                int keycount, int valcount)
        : NBCollectiveShuffler<KeyType,ValType>(comm, out, user_hash, keycount, valcount)
    {
        this->user_combine = user_combine;
        this->user_ptr = user_ptr;
        bucket = NULL;
    }

    virtual ~NBCombineCollectiveShuffler()
    {
    }

    virtual int open() {
        NBCollectiveShuffler<KeyType,ValType>::open();
        bucket = new HashBucket<CombinerVal>();

        return 0;
    }

    void close() {
       garbage_collection();
       delete bucket;

       NBCollectiveShuffler<KeyType,ValType>::close();
   }

   virtual int write(KeyType *key, ValType *val)
   {
       int target = this->get_target_rank(key);

       if (target == this->shuffle_rank) {
           this->out->write(key, val);
           return 0;
       }

       int kvsize = this->ser->get_kv_bytes(key, val);
       if (kvsize > this->buf_size)
           LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n", 
                     kvsize, this->buf_size);

       keybytes = this->ser->key_to_bytes(key, keyarray, MAX_RECORD_SIZE);

       //u = bucket->findElem(((KVRecord*)record)->get_key(), 
       //                     ((KVRecord*)record)->get_key_size());
       u = bucket->findEntry(keyarray, keybytes);

       if (u == NULL) {
           CombinerVal tmp;

           std::unordered_map < char *, int >::iterator iter;
           char *range_start = this->msg_buffers[this->cur_idx].send_buffer + target * (int64_t)this->buf_size;
           char *range_end = this->msg_buffers[this->cur_idx].send_buffer + target * (int64_t)this->buf_size 
               + this->msg_buffers[this->cur_idx].send_offset[target];
           for (iter = slices.begin(); iter != slices.end(); iter++) {
               char *sbuf = iter->first;
               int ssize = iter->second;

               if (sbuf >= range_start && sbuf < range_end && ssize >= kvsize) {
                   tmp.kv = sbuf + (ssize - kvsize);
                   //kv.set_buffer(tmp.kv);
                   //kv.convert((KVRecord*)record);
                   this->ser->kv_to_bytes(key, val, tmp.kv, kvsize);
                   if (iter->second == kvsize)
                       slices.erase(iter);
                   else
                       slices[iter->first] -= kvsize;

                   bucket->insertEntry(tmp.kv, keybytes, &tmp);

                   break;
               }
           }

           if (iter == slices.end()) {
               if ((int64_t)this->msg_buffers[this->cur_idx].send_offset[target] + (int64_t) kvsize > this->buf_size) {
                   garbage_collection();
                   this->start_kv_exchange();
               }
               tmp.kv = this->msg_buffers[this->cur_idx].send_buffer + target * (int64_t)this->buf_size 
                   + this->msg_buffers[this->cur_idx].send_offset[target];
               //kv.set_buffer(tmp.kv);
               //kv.convert((KVRecord*)record);
               this->ser->kv_to_bytes(key, val, tmp.kv, kvsize);
               this->msg_buffers[this->cur_idx].send_offset[target] += kvsize;
           }

           bucket->insertEntry(tmp.kv, keybytes, &tmp);
           this->kvcount ++;
       }
       else {
           //kv.set_buffer(u->kv);
           //user_combine(this, &kv, (KVRecord*)record, user_ptr);
            typename SafeType<KeyType>::type u_key[this->keycount];
            typename SafeType<ValType>::type u_val[this->valcount];
            int ukvsize = this->ser->kv_from_bytes(u_key, u_val, u->kv, MAX_RECORD_SIZE);
            user_combine(this, u_key, u_val, val, user_ptr);
       }

       return 0;
   }

    //virtual void update(BaseRecordFormat *);
   virtual void update(KeyType *key, ValType *val)
   {
        int target = this->get_target_rank(key);

       //int target = get_target_rank(((KVRecord*)record)->get_key(),
       //                             ((KVRecord*)record)->get_key_size());

       //int kvsize = record->get_record_size();
       //int ukvsize = kv.get_record_size();
       //int ksize = kv.get_key_size();

        typename SafeType<KeyType>::type u_key[this->keycount];
        typename SafeType<ValType>::type u_val[this->valcount];
        int ukvsize = this->ser->kv_from_bytes(u_key, u_val, u->kv, MAX_RECORD_SIZE);
        int kvsize = this->ser->get_kv_bytes(key, val);

       if (kvsize > this->buf_size)
           LOG_ERROR("Error: KV size (%d) is larger than buf_size (%ld)\n", 
                     kvsize, this->buf_size);

       //if (((KVRecord*)record)->get_key_size() != kv.get_key_size()
       //    || memcmp(((KVRecord*)record)->get_key(), kv.get_key(), ksize) != 0)
       //    LOG_ERROR("Error: the result key of combiner is different!\n");

        if (this->ser->compare_key(key, u_key) != 0)
            LOG_ERROR("Error: the result key of combiner is different!\n");

       if (kvsize <= ukvsize) {
           //kv.convert((KVRecord*)record);
           this->ser->kv_to_bytes(key, val, u->kv, kvsize);
           if (kvsize < ukvsize)
               slices.insert(std::make_pair(u->kv + kvsize, 
                                            ukvsize - kvsize));
       }
       else {
           slices.insert(std::make_pair(u->kv, ukvsize));
           if ((int64_t)this->msg_buffers[this->cur_idx].send_offset[target] + (int64_t) kvsize > this->buf_size) {
               //while (!done_kv_exchange()) {
               //    push_kv_exchange();
               //}
               garbage_collection();
               this->start_kv_exchange();
               u = NULL;
           }
           char *gbuf = this->msg_buffers[this->cur_idx].send_buffer + target * (int64_t) this->buf_size 
               + this->msg_buffers[this->cur_idx].send_offset[target];
           //kv.set_buffer(gbuf);
           //kv.convert((KVRecord*)record);
           this->ser->kv_to_bytes(key, val, gbuf, kvsize);
           this->msg_buffers[this->cur_idx].send_offset[target] += kvsize;
           if (u != NULL) u->kv=gbuf;
       }
       return;
   }

   virtual void make_progress(bool issue_new = false) {
        if(issue_new && this->pending_msg == 0) {
            garbage_collection();
            this->start_kv_exchange();
        }
        this->push_kv_exchange();
    }

protected:
   void garbage_collection()
   {
       if (!slices.empty()) {
           typename SafeType<KeyType>::type key[this->keycount];
           typename SafeType<ValType>::type val[this->valcount];

           LOG_PRINT(DBG_GEN, "NBCollectiveShuffler garbage collection: slices=%ld\n",
                     slices.size());

           int dst_off = 0, src_off = 0;
           char *dst_buf = NULL, *src_buf = NULL;

           for (int k = 0; k < this->shuffle_size; k++) {
               src_buf = this->msg_buffers[this->cur_idx].send_buffer          \
                   + k * (int64_t)this->buf_size;
               dst_buf = this->msg_buffers[this->cur_idx].send_buffer          \
                   + k * (int64_t)this->buf_size;

               dst_off = src_off = 0;
               while (src_off < this->msg_buffers[this->cur_idx].send_offset[k]) {

                   char *tmp_buf = src_buf + src_off;
                   std::unordered_map < char *, int >::iterator iter = slices.find(tmp_buf);
                   if (iter != slices.end()) {
                       src_off += iter->second;
                   }
                   else {
                       //kv.set_buffer(tmp_buf);
                       //int kvsize = kv.get_record_size();
                       int kvsize = this->ser->kv_from_bytes(key, val,
                            tmp_buf, this->msg_buffers[this->cur_idx].send_offset[k] - src_off);
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

   void (*user_combine)(Combinable<KeyType,ValType> *output,
                         KeyType *key, ValType *val1, ValType *val2, void *ptr);
    void *user_ptr;
    std::unordered_map<char*, int> slices;
    HashBucket<CombinerVal> *bucket;
    CombinerVal *u;
    char* keyarray;
    int keybytes;
};

}
#endif
