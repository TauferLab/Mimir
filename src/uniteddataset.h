/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_UNITED_DATASET_H
#define MIMIR_UNITED_DATASET_H

#include <vector>
#include "interface.h"

namespace MIMIR_NS {

template <typename KeyType, typename ValType>
class UnitedDataset : public Readable<KeyType, ValType>
{
  public:
    UnitedDataset(std::vector<Readable<KeyType, ValType> *> &datasets)
    {
        for (auto iter : datasets) {
            this->datasets.push_back(iter);
        }
        dataset_idx = 0;
    }

    virtual ~UnitedDataset() {}

    virtual int open()
    {
        for (auto iter : datasets) {
            iter->open();
        }
        dataset_idx = 0;
        return true;
    }

    virtual void close()
    {
        for (auto iter : datasets) {
            iter->close();
        }
    }

    virtual int seek(DB_POS pos)
    {
        int ret = 0;
        if (pos == DB_START) {
            dataset_idx = 0;
            for (auto iter : datasets) {
                ret = iter->seek(DB_START);
                if (ret != true) return false;
            }
        }
        else if (pos == DB_END) {
            dataset_idx = (int) datasets.size() - 1;
            ret = datasets[dataset_idx]->seek(DB_END);
            if (ret != true) return false;
        }

        return true;
    }

    virtual uint64_t get_record_count()
    {
        uint64_t total_count = 0;
        for (auto iter : datasets) {
            total_count += iter->get_record_count();
        }
        return total_count;
    }

    virtual int read(KeyType *key, ValType *val)
    {
        int ret = 0;
        while (dataset_idx < datasets.size()) {
            ret = datasets[dataset_idx]->read(key, val);
            if (ret == true) return true;
            dataset_idx++;
        }
        return false;
    }

  private:
    std::vector<Readable<KeyType, ValType> *> datasets;
    size_t dataset_idx;
};

} // namespace MIMIR_NS

#endif
