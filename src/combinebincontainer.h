/*
 * (c) 2017 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */

#ifndef MIMIR_COMBINE_BIN_CONTAINER_H
#define MIMIR_COMBINE_BIN_CONTAINER_H

#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>
#include "kvcontainer.h"
#include "hashbucket.h"

namespace MIMIR_NS {

template <typename KeyType, typename ValType>
class CombineBinContainer : public BinContainer<KeyType, ValType>,
                            public Combinable<KeyType, ValType>
{
  public:
    CombineBinContainer(
        void (*user_combine)(Combinable<KeyType, ValType> *output, KeyType *key,
                             ValType *val1, ValType *val2, ValType *val3,
                             void *ptr),
        void *user_ptr, uint32_t bincount, int keycount = 1, int valcount = 1)
        : BaseDatabase<KeyType, ValType>(true), BinContainer<KeyType, ValType>(
                                                    bincount, keycount,
                                                    valcount)
    {
        this->user_combine = user_combine;
        this->user_ptr = user_ptr;
        bucket = NULL;
    }

    ~CombineBinContainer() {}

    virtual int open()
    {
        bucket = new HashBucket<CombinerVal>();

        BinContainer<KeyType, ValType>::open();

        LOG_PRINT(DBG_GEN, "CombineBinContainer open!\n");

        return 0;
    }

    virtual void close()
    {
        this->garbage_collection();
        bucket->clear();

        delete bucket;

        BinContainer<KeyType, ValType>::close();

        LOG_PRINT(DBG_GEN, "CombineBinContainer close.\n");
    }

    virtual int write(KeyType *key, ValType *val)
    {
        int ret = 0;

        // Get <key,value> length
        int kvsize = this->ser->get_kv_bytes(key, val);
        if (kvsize > this->bin_unit_size)
            LOG_ERROR("Error: KV size (%d) is larger than bin size (%d)\n",
                      kvsize, this->bin_unit_size);

        // Get bin index
        uint32_t bid = this->ser->get_hash_code(key) % (this->bincount);

        int keysize = this->ser->get_key_bytes(key);
        char *keyptr = this->ser->get_key_ptr(key);
        u = bucket->findEntry(keyptr, keysize);

        if (u == NULL) {
            // Find a slice to insert the KV
            CombinerVal tmp;

            auto iter = this->slices.begin();
            for (; iter != this->slices.end(); iter++) {
                char *sbuf = iter->first;
                int ssize = iter->second.first;
                uint32_t sbid = iter->second.second;

                if (sbid != bid) continue;

                if (ssize >= kvsize) {
                    tmp.kv = sbuf + (ssize - kvsize);
                    this->ser->kv_to_bytes(key, val, tmp.kv, kvsize);

                    if (ssize == kvsize)
                        this->slices.erase(iter);
                    else
                        this->slices[iter->first]
                            = std::make_pair(ssize - kvsize, sbid);

                    break;
                }
            }

            if (iter == this->slices.end()) {
                // Find a bin to insert the KV
                int bidx = 0;
                auto iter = this->bin_insert_idx.find(bid);
                if (iter == this->bin_insert_idx.end()) {
                    bidx = this->get_empty_bin();
                    this->bins[bidx].bintag = bid;
                    this->bin_insert_idx[bid] = bidx;
                }
                else {
                    bidx = iter->second;
                    if (this->bin_unit_size - this->bins[bidx].datasize
                        < kvsize) {
                        bidx = this->get_empty_bin();
                        this->bins[bidx].bintag = bid;
                        this->bin_insert_idx[bid] = bidx;
                    }
                }

                // Store the <key,value>
                tmp.kv = this->get_bin_ptr(bidx) + this->bins[bidx].datasize;
                this->ser->kv_to_bytes(
                    key, val, tmp.kv,
                    this->bin_unit_size - this->bins[bidx].datasize);
                this->bins[bidx].datasize += kvsize;
            }

            bucket->insertEntry(tmp.kv, keysize, &tmp);
            this->kvcount += 1;
            ret = 1;
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

            uint32_t bid = this->ser->get_hash_code(key) % (this->bincount);

            if (rvalsize <= uvalsize) {
                this->ser->val_to_bytes(r_val, u->kv + ukeysize, uvalsize);
                if (rvalsize < uvalsize) {
                    char *ptr = u->kv + ukvsize - (uvalsize - rvalsize);
                    this->slices.insert(std::make_pair(
                        ptr, std::make_pair(uvalsize - rvalsize, bid)));
                }
            }
            else {
                this->slices.insert(
                    std::make_pair(u->kv, std::make_pair(ukvsize, bid)));
                int bidx = 0;
                auto iter = this->bin_insert_idx.find(bid);
                if (iter == this->bin_insert_idx.end()) {
                    bidx = this->get_empty_bin();
                    this->bins[bidx].bintag = bid;
                    this->bin_insert_idx[bidx] = bid;
                }
                else {
                    bidx = iter->second;
                    if (this->bin_unit_size - this->bins[bidx].datasize
                        < ukeysize + rvalsize) {
                        bidx = this->get_empty_bin();
                        this->bins[bidx].bintag = bid;
                        this->bin_insert_idx[bidx] = bid;
                    }
                }
                // Store the <key,value>
                char *ptr = this->get_bin_ptr(bidx) + this->bins[bidx].datasize;
                this->ser->kv_to_bytes(
                    u_key, r_val, ptr,
                    this->bin_unit_size - this->bins[bidx].datasize);
                this->bins[bidx].datasize += ukeysize + rvalsize;
            }
            ret = 0;
        }

        return ret;
    }

#if 0
    void update(KeyType *key, ValType *val)
    {
        typename SafeType<KeyType>::ptrtype u_key = NULL;
        typename SafeType<ValType>::ptrtype u_val = NULL;

        int ukvsize = this->ser->kv_from_bytes(&u_key, &u_val, u->kv, MAX_RECORD_SIZE);
        int kvsize = this->ser->get_kv_bytes(key, val);

        if (this->ser->compare_key(key, u_key) != 0)
            LOG_ERROR("Error: the result key of combiner is different!\n");

        // Find a bin to insert the KV
        uint32_t bid = this->ser->get_hash_code(key) % (this->bincount);

        if (kvsize <= ukvsize) {
            this->ser->kv_to_bytes(key, val, u->kv, kvsize);
            if (kvsize < ukvsize) {
                this->slices.insert(std::make_pair(u->kv + ukvsize - kvsize,
                                                   std::make_pair(ukvsize - kvsize, bid)));
            }
        }
        else {
            this->slices.insert(std::make_pair(u->kv, std::make_pair(ukvsize,bid)));

            int bidx = 0;
            auto iter = this->bin_insert_idx.find(bid);
            if (iter == this->bin_insert_idx.end()) {
                bidx = this->get_empty_bin();
                this->bins[bidx].bintag = bid;
                this->bin_insert_idx[bidx] = bid;
            } else {
                bidx = iter->second;
                if (this->bin_unit_size - this->bins[bidx].datasize < kvsize) {
                    bidx = this->get_empty_bin();
                    this->bins[bidx].bintag = bid;
                    this->bin_insert_idx[bidx] = bid;
                }
            }
        }

        return;
    }
#endif

    //virtual int remove(KeyType *key, ValType *val, std::set<uint32_t>& remove_bins)
    //{

    //    int ret = BinContainer<KeyType, ValType>::remove(key, val, remove_bins);
    //    if (ret != -1) {
    //        int keysize = this->ser->get_key_bytes(key);
    //        char *keyptr = this->ser->get_key_ptr(key);
    //        bucket->removeEntry(keyptr, keysize);
    //    }

    //    return ret;
    //}

    virtual int get_next_bin(char *&buffer, int &datasize, uint32_t &bintag,
                             int &kvcount)
    {
        return BinContainer<KeyType, ValType>::get_next_bin(buffer, datasize,
                                                            bintag, kvcount);
    }

  private:
    void (*user_combine)(Combinable<KeyType, ValType> *output, KeyType *key,
                         ValType *val1, ValType *val2, ValType *val3,
                         void *ptr);
    void *user_ptr;
    HashBucket<CombinerVal> *bucket;
    CombinerVal *u;
};

} // namespace MIMIR_NS

#endif
