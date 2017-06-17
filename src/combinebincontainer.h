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
      public Combinable<KeyType, ValType> {
  public:
    CombineBinContainer(void (*user_combine)(Combinable<KeyType,ValType> *output,
                                             KeyType *key,
                                             ValType *val1, ValType *val2,
                                             void *ptr),
                        void *user_ptr, uint32_t bincount, 
                        int keycount = 1, int valcount = 1)
        : BinContainer<KeyType,ValType>(bincount, keycount, valcount) {

        this->user_combine = user_combine;
        this->user_ptr = user_ptr;
        bucket = NULL;
    }

    ~CombineBinContainer()
    {
    }

    virtual int open() {

        bucket = new HashBucket<CombinerVal>();

        BinContainer<KeyType,ValType>::open();

        keyarray = (char*) mem_aligned_malloc(MEMPAGE_SIZE, MAX_RECORD_SIZE);

        LOG_PRINT(DBG_GEN, "CombineBinContainer open!\n");

        return 0;
    }

    virtual void close() {

        this->garbage_collection();
        bucket->clear();

        mem_aligned_free(keyarray);

        delete bucket;

        BinContainer<KeyType,ValType>::close();

        LOG_PRINT(DBG_GEN, "CombineBinContainer close.\n");
    }

    virtual int write(KeyType* key, ValType* val) 
    {
        int ret = 0;

        // Get <key,value> length
        int kvsize = this->ser->get_kv_bytes(key, val);
        if (kvsize > this->bin_unit_size)
            LOG_ERROR("Error: KV size (%d) is larger than bin size (%ld)\n", 
                      kvsize, this->bin_unit_size);

        // Get bin index
        char tmpkey[MAX_RECORD_SIZE];
        int keysize = this->ser->get_key_bytes(key);
        if (keysize > MAX_RECORD_SIZE) LOG_ERROR("The key is too long!\n");
        this->ser->key_to_bytes(key, tmpkey, MAX_RECORD_SIZE);
        uint32_t bid = hashlittle(tmpkey, keysize, 0) % (this->bincount);

        keybytes = this->ser->key_to_bytes(key, keyarray, MAX_RECORD_SIZE);
        u = bucket->findEntry(keyarray, keysize);

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
                        this->slices[iter->first] = std::make_pair(ssize - kvsize, sbid);

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
                } else {
                    bidx = iter->second;
                    if (this->bin_unit_size - this->bins[bidx].datasize < kvsize) {
                        bidx = this->get_empty_bin();
                        this->bins[bidx].bintag = bid;
                        this->bin_insert_idx[bid] = bidx;
                    }
                }

                // Store the <key,value>
                tmp.kv = this->get_bin_ptr(bidx) + this->bins[bidx].datasize;
                this->ser->kv_to_bytes(key, val, tmp.kv, this->bin_unit_size - this->bins[bidx].datasize);
                this->bins[bidx].datasize += kvsize;
            }

            bucket->insertEntry(tmp.kv, keybytes, &tmp);
            this->kvcount += 1;
            ret = 1;
        }
        else {
            typename SafeType<KeyType>::type u_key[this->keycount];
            typename SafeType<ValType>::type u_val[this->valcount];
            this->ser->kv_from_bytes(u_key, u_val, u->kv, MAX_RECORD_SIZE);
            user_combine(this, u_key, u_val, val, user_ptr);
            ret = 0;
        }

        return ret;
    }

    void update(KeyType *key, ValType *val)
    {
        typename SafeType<KeyType>::type u_key[this->keycount];
        typename SafeType<ValType>::type u_val[this->valcount];

        int ukvsize = this->ser->kv_from_bytes(u_key, u_val, u->kv, MAX_RECORD_SIZE);
        int kvsize = this->ser->get_kv_bytes(key, val);

        if (this->ser->compare_key(key, u_key) != 0)
            LOG_ERROR("Error: the result key of combiner is different!\n");

        // Find a bin to insert the KV
        char tmpkey[MAX_RECORD_SIZE];
        int keysize = this->ser->get_key_bytes(key);
        if (keysize > MAX_RECORD_SIZE) LOG_ERROR("The key is too long!\n");
        this->ser->key_to_bytes(key, tmpkey, MAX_RECORD_SIZE);
        uint32_t bid = hashlittle(tmpkey, keysize, 0) % (this->bincount);

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

    virtual int remove(KeyType *key, ValType *val, std::set<uint32_t>& remove_bins)
    {

        int ret = BinContainer<KeyType, ValType>::remove(key, val, remove_bins);
        if (ret != -1) {
            keybytes = this->ser->key_to_bytes(key, keyarray, MAX_RECORD_SIZE);
            bucket->removeEntry(keyarray, keybytes);
        }

        return ret;
    }

  private:
    void (*user_combine)(Combinable<KeyType,ValType> *output,
                         KeyType *key, ValType *val1, ValType *val2, void *ptr);
    void *user_ptr;
    HashBucket<CombinerVal> *bucket;
    CombinerVal *u;
    char* keyarray;
    int keybytes;
};

}

#endif
