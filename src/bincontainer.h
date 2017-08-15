#ifndef MIMIR_BIN_CONTAINER_H
#define MIMIR_BIN_CONTAINER_H

#include <stdio.h>
#include <stdlib.h>
#include "container.h"
#include "containeriter.h"
#include "interface.h"
#include "serializer.h"
#include "config.h"
#include "stat.h"

namespace MIMIR_NS {

struct Bin {
    uint32_t  bintag;
    int       datasize;
    int       kvcount;
};

template <typename KeyType, typename ValType>
class BinContainer : virtual public BaseDatabase<KeyType, ValType>
{
  public:
    BinContainer(uint32_t bincount, int keycount, int valcount) 
        : BaseDatabase<KeyType, ValType>(true) 
    {
        this->bincount = bincount;
        this->keycount = keycount;
        this->valcount = valcount;

        ser = new Serializer<KeyType, ValType>(keycount, valcount);

        // Get bin size
        if (std::is_pointer<KeyType>::value || std::is_pointer<ValType>::value) {
            bin_unit_size = MAX_RECORD_SIZE;
            bin_per_page = (int)(DATA_PAGE_SIZE / bin_unit_size);
        } else {
            typename SafeType<KeyType>::type key[keycount];
            typename SafeType<ValType>::type val[valcount];
            int record_size = ser->get_kv_bytes(key, val);
            bin_unit_size = (MEMPAGE_SIZE + record_size - 1) / MEMPAGE_SIZE * MEMPAGE_SIZE;
            bin_per_page = (int)(DATA_PAGE_SIZE / bin_unit_size);
        }

        cur_bin_idx = 0;
        cur_bin_off = 0;

        isremove = false;
        pagesize = DATA_PAGE_SIZE;

        kvcount = 0;
    }

    virtual ~BinContainer() 
    {
        delete ser;

        for (size_t i = 0; i < pages.size(); i++) {
            BaseDatabase<KeyType, ValType>::mem_bytes -= pagesize;
            mem_aligned_free(pages[i].buffer);
        }
    }

    virtual int open() 
    {
        cur_bin_idx = 0;
        LOG_PRINT(DBG_DATA, "BinContainer open.\n");
        return true;
    }

    virtual void close()
    {
        LOG_PRINT(DBG_DATA, "BinContainer close (buffer size=%ld, data size=%ld).\n",
                  pages.size() * pagesize, get_data_size());
    }

    virtual int seek(DB_POS pos) {
        LOG_WARNING("FileReader doesnot support seek methods!\n");
        return false;
    }

    virtual int read(KeyType* key, ValType* val) 
    {
        // Find next bin
        while (cur_bin_idx < (int)bins.size() 
               && cur_bin_off >= bins[cur_bin_idx].datasize) {
            cur_bin_idx ++;
            cur_bin_off = 0;
        }

        // At the end
        if (cur_bin_idx >= (int)bins.size()) {
            return -1;
        }

        // Get the <key,value>
        char *ptr = get_bin_ptr(cur_bin_idx) + cur_bin_off;
        int kvsize = this->ser->kv_from_bytes(key, val, ptr, bin_unit_size - cur_bin_off);

        cur_bin_off += kvsize;

        return 0;
    }

    virtual int write(KeyType* key, ValType* val) 
    {
        // Get bin index
        uint32_t bid = ser->get_hash_code(key) % bincount;

        // Find a bin to insert the KV
        int bidx = 0;
        auto iter = bin_insert_idx.find(bid);
        if (iter == bin_insert_idx.end()) {
            bidx = get_empty_bin();
            bins[bidx].bintag = bid;
            bin_insert_idx[bid] = bidx;
        } else {
            bidx = iter->second;
            if (bid != bins[bidx].bintag) {
                LOG_ERROR("Bin error bid=%d, bidx=%d, bintag=%d\n",
                          bid, bidx, bins[bidx].bintag);
            }
        }

        // Store the <key,value>
        char *ptr = get_bin_ptr(bidx) + bins[bidx].datasize;
        int kvsize = this->ser->kv_to_bytes(key, val, ptr, bin_unit_size - bins[bidx].datasize);
        if (kvsize == -1) {
            bidx = get_empty_bin();
            bins[bidx].bintag = bid;
            bin_insert_idx[bid] = bidx;
            ptr = get_bin_ptr(bidx) + bins[bidx].datasize;
            kvsize = this->ser->kv_to_bytes(key, val, ptr, bin_unit_size - bins[bidx].datasize);
            if (kvsize == -1)
                LOG_ERROR("Error: KV size (%d) is larger \
                          than bin size (%d)\n", kvsize, bin_unit_size);
        }

        bins[bidx].datasize += kvsize;
        bins[bidx].kvcount += 1;

        kvcount += 1;

        return 1;
    }

#if 0
    virtual int remove(KeyType *key, ValType *val, std::set<uint32_t>& remove_bins)
    {
        // Find the first bin
        if (!isremove) {

            cur_bin_idx = 0;
            cur_bin_off = 0;

            // Find next bin
            while (cur_bin_idx < (int)bins.size() ) {
                uint32_t bintag = bins[cur_bin_idx].bintag;
                if (bins[cur_bin_idx].datasize == 0) {
                    cur_bin_idx ++;
                    continue;
                }
                if (remove_bins.find(bintag) != remove_bins.end()) {
                    break;
                }
                cur_bin_idx ++;
            }

            // At the end
            if (cur_bin_idx >= (int)bins.size()) {
                isremove = false;
                return -1;
            }

            isremove = true;
        }

        if (cur_bin_off >= bins[cur_bin_idx].datasize) {

            bins[cur_bin_idx].datasize = 0;
            cur_bin_off = 0;

            // Find next bin
            while (cur_bin_idx < (int)bins.size() ) {
                uint32_t bintag = bins[cur_bin_idx].bintag;
                if (bins[cur_bin_idx].datasize == 0) {
                    cur_bin_idx ++;
                    continue;
                }
                if (remove_bins.find(bintag) != remove_bins.end()) {
                    break;
                }
                cur_bin_idx ++;
            }

            // At the end
            if (cur_bin_idx >= (int)bins.size()) {
                isremove = false;
                return -1;
            }
        }

        char *ptr = get_bin_ptr(cur_bin_idx) + cur_bin_off;
        int kvsize = this->ser->kv_from_bytes(key, val, ptr, bin_unit_size - cur_bin_off);
        cur_bin_off += kvsize;

        kvcount -= 1;

        return 0;
    }
#endif

    virtual uint64_t get_record_count() { return kvcount; }

    virtual uint64_t get_data_size() {
        uint64_t total_size = 0;
        for (auto bin : bins) {
            total_size += bin.datasize;
        }
        return total_size;
    }

    char *get_bin_ptr(int bin_idx) {
        return pages[bin_idx / bin_per_page].buffer                            \
            + (bin_idx % bin_per_page) * bin_unit_size;
    }

    uint64_t add_page() {
        Page page;
        page.datasize = 0;
        page.buffer = (char*)mem_aligned_malloc(MEMPAGE_SIZE, pagesize);
        BaseDatabase<KeyType, ValType>::mem_bytes  += pagesize;
        PROFILER_RECORD_COUNT(COUNTER_MAX_KV_PAGES,
                              this->mem_bytes, OPMAX);
        pages.push_back(page);

        printf("%d[%d] add page=%ld\n", mimir_world_rank, mimir_world_size, pages.size());

        return pages.size() - 1;
    }

    int  get_empty_bin() {

        size_t idx = 0;
        for (idx = 0; idx < bins.size(); idx++) {
            if (bins[idx].datasize == 0) {
                return (int)idx;
            }
        }

        add_page();

        Bin bin;
        bin.bintag = 0;
        bin.datasize = 0;
        bin.kvcount = 0;
        for (int i = 0; i < bin_per_page; i++) {
            bins.push_back(bin);
        }

        return (int)idx;
    }

    int get_unit_size() { return bin_unit_size; }

    virtual int remove() {
        LOG_ERROR("This KV container doesnot support remove function!\n");
        return false;
    }

    void set_bin_info(int bidx, uint32_t bintag, int datasize, int kvcount) {
        bins[bidx].bintag = bintag;
        bins[bidx].datasize = datasize;
        if (bins[bidx].kvcount < kvcount) {
            this->kvcount += (kvcount - bins[bidx].kvcount);
        } else if (bins[bidx].kvcount > kvcount) {
            this->kvcount -= (bins[bidx].kvcount - kvcount);
        }
        bins[bidx].kvcount = kvcount;
    }

    virtual int get_next_bin(char *&buffer, int &datasize, uint32_t &bintag, int &kvcount)
    {
        //printf("cur_bin_idx=%d, size=%ld\n",
        //       cur_bin_idx, bins.size());

        //if (cur_bin_idx == 0) {
        //    printf("%d[%d] get next bin cur_bin_idx=%d, cur_bin_off=%d\n",
        //           mimir_world_rank, mimir_world_size, cur_bin_idx, cur_bin_off);
        //}

        if (cur_bin_idx == 0) garbage_collection();

        if (cur_bin_idx < (int)bins.size() ) {
            buffer = get_bin_ptr(cur_bin_idx);
            bintag = bins[cur_bin_idx].bintag;
            datasize = bins[cur_bin_idx].datasize;
            kvcount = bins[cur_bin_idx].kvcount;
            if (bintag == 2376) {
                printf("%d[%d] get next bintag=%d, count=%d\n",
                       mimir_world_rank, mimir_world_size, bintag, kvcount);
            }
            //kvcount -= bins[cur_bin_idx].kvcount;
            return cur_bin_idx++;
        }

        cur_bin_idx = 0;
        cur_bin_off = 0;
        return -1;
    }


protected:

    void garbage_collection()
    {
        if (!(this->slices.empty())) {

            typename SafeType<KeyType>::ptrtype key = NULL;
            typename SafeType<ValType>::ptrtype val = NULL;

            LOG_PRINT(DBG_GEN, "KVContainer garbage collection: slices=%ld\n",
                      this->slices.size());

            for (int i = 0; i < (int)bins.size(); i++) {

                int dst_off = 0, src_off = 0;
                char *dst_buf = NULL, *src_buf = NULL;

                src_buf = get_bin_ptr(i);
                dst_buf = get_bin_ptr(i);

                dst_off = src_off = 0;

                while (src_off < bins[i].datasize) {

                    char *tmp_buf = src_buf + src_off;
                    auto iter = slices.find(tmp_buf);
                    if (iter != slices.end()) {
                        src_off += iter->second.first;
                    }
                    else {
                        int kvsize = this->ser->kv_from_bytes(&key, &val, tmp_buf, bins[i].datasize - src_off);
                        if (src_off != dst_off) {
                            for (int kk = 0; kk < kvsize; kk++)
                                dst_buf[dst_off + kk] = src_buf[src_off + kk];
                        }
                        dst_off += kvsize;
                        src_off += kvsize;
                    }
                }
                bins[i].datasize = dst_off;
            }

            this->slices.clear();
        }
        bin_insert_idx.clear();
    }

    int keycount, valcount;
    uint32_t    bincount;

    std::vector<Page> pages;
    std::vector<Bin>  bins;
    int bin_unit_size, bin_per_page;
    int cur_bin_idx, cur_bin_off;

    std::unordered_map<int, uint32_t> bin_insert_idx;

    int64_t  pagesize;

    uint64_t kvcount;
    bool     isremove;
    Serializer<KeyType, ValType> *ser;
    std::unordered_map<char*, std::pair<int, uint32_t>> slices;
};

}

#endif
