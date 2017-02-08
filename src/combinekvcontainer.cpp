#include <string.h>
#include <string>
#include "combinekvcontainer.h"
#include "log.h"
#include "const.h"
#include "recordformat.h"

using namespace MIMIR_NS;

CombineKVContainer::CombineKVContainer(CombineCallback user_combine,
                                       void *user_ptr)
    : KVContainer()
{
    this->user_combine = user_combine;
    this->user_ptr = user_ptr;
    bucket = NULL;
}

CombineKVContainer::~CombineKVContainer()
{
    LOG_PRINT(DBG_DATA, "DATA: KV Destroy (id=%d).\n", id);
}

bool CombineKVContainer::open() {
    CombineKVContainer::open();
    bucket = new CombinerHashBucket();
    return true;
}

void CombineKVContainer::close() {
    garbage_collection();
    CombineKVContainer::close();
    delete bucket;
}

void CombineKVContainer::write(KVRecord *record)
{
    if (page == NULL)
        page = add_page();

    int kvsize = record->get_record_size();
    if (kvsize > pagesize)
        LOG_ERROR("Error: KV size (%d) is larger \
                  than one page (%ld)\n", kvsize, pagesize);

    CombinerUnique *u = bucket->findElem(record->get_key(), 
                                         record->get_key_size());
    if (u == NULL) {
        CombinerUnique tmp;
        tmp.next = NULL;

        std::unordered_map < char *, int >::iterator iter;
        for (iter = slices.begin(); iter != slices.end(); iter++) {\
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

                break;
            }
        }
        if (iter == slices.end()) {
            if (kvsize > (pagesize - page->datasize))
                page = add_page();
            tmp.kv = page->buffer + page->datasize;
            kv.set_buffer(tmp.kv);
            kv = *record;
            kvsize = record->get_record_size();
            page->datasize += kvsize;
        }
        bucket->insertElem(&tmp);
    }
    else {
        kv.set_buffer(u->kv);
        user_combine(this, &kv, record, user_ptr);
    }

    kvcount += 1;

    return;
}


void CombineKVContainer::update(KVRecord *record)
{
    int kvsize = record->get_key_size();
    int ukvsize = kv.get_key_size();
    if (kvsize != ukvsize 
        || memcmp(record->get_key(), kv.get_key(), kvsize) != 0)
        LOG_ERROR("Error: the result key of combiner is different!\n");

    if (kvsize <= ukvsize) {
        //record.set_buffer(this->record.get_record());
        kv = *record;
         if (kvsize < ukvsize) {
            slices.insert(std::make_pair(kv.get_record() + ukvsize - kvsize, ukvsize - kvsize));
        }
    }
    else {
        slices.insert(std::make_pair(kv.get_record(), ukvsize));
        if (kvsize > (pagesize - page->datasize))
            page = add_page();

        kv.set_buffer(page->buffer + page->datasize);
        kv = *record;
        kvsize = record->get_record_size();
        page->datasize += kvsize;
    }

    return;
}


void CombineKVContainer::garbage_collection()
{
    char *dst_buf = NULL, *src_buf = NULL;
    int64_t dst_off = 0, src_off = 0;
    int kvsize;

    if (!slices.empty()) {
        page = get_next_page();
        while (page != NULL) {
            src_off = 0;
            while (src_off < page->datasize) {
                src_buf = page->buffer + src_off;
                std::unordered_map < char *, int >::iterator iter = slices.find(src_buf);
                if (iter != slices.end()) {
                    if (dst_buf == NULL) {
                        dst_off = src_off;
                        dst_buf = src_buf;
                    }
                    src_off += iter->second;
                }
                else {
                    kv.set_buffer(src_buf);
                    kvsize = kv.get_record_size();
                    if (dst_buf != NULL && src_buf != dst_buf) {
                        if (pagesize - dst_off < kvsize) {
                            page->datasize = dst_off;
                            //dst_pid += 1;
                            dst_off = 0;
                            dst_buf = page->buffer;
                        }
                        // copy the KV
                        memcpy(dst_buf, src_buf, kvsize);
                        dst_off += kvsize;
                        dst_buf += kvsize;
                    }
                    src_off += kvsize;
                }
            }
            //src_pid += 1;
        }
        // free extra space
        //for (int i = dst_pid + 1; i < (int)pages.size(); i++) {
        //    mem_aligned_free(pages[i].buffer);
        //    page->buffer = NULL;
        //    page->datasize = 0;
        //}
        if (dst_buf != NULL) {
            page->datasize = dst_off;
        }
        slices.clear();
    }
}


