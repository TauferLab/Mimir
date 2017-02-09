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
    //LOG_PRINT(DBG_DATA, "DATA: KV Destroy (id=%d).\n", id);
}

bool CombineKVContainer::open() {
    bucket = new CombinerHashBucket();
    KVContainer::open();

    LOG_PRINT(DBG_GEN, "CombineKVContainer open!\n");

    return true;
}

void CombineKVContainer::close() {
    garbage_collection();
    delete bucket;
    KVContainer::close();

    LOG_PRINT(DBG_GEN, "CombineKVContainer close.\n");
}

void CombineKVContainer::write(BaseRecordFormat *record)
{
    if (page == NULL) page = add_page();

    int kvsize = record->get_record_size();
    if (kvsize > pagesize)
        LOG_ERROR("Error: KV size (%d) is larger \
                  than one page (%ld)\n", kvsize, pagesize);

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

                break;
            }
        }
        if (iter == slices.end()) {
            if (kvsize + page->datasize > pagesize ) page = add_page();
            tmp.kv = page->buffer + page->datasize;
            kv.set_buffer(tmp.kv);
            kv.convert((KVRecord*)record);
            page->datasize += kvsize;
        }
        bucket->insertElem(&tmp);
        kvcount += 1;
    }
    else {
        kv.set_buffer(u->kv);
        user_combine(this, &kv, (KVRecord*)record, user_ptr);
    }

    return;
}


void CombineKVContainer::update(BaseRecordFormat *record)
{
    int ksize = kv.get_key_size();
    int kvsize = record->get_record_size();
    int ukvsize = kv.get_record_size();

    if (((KVRecord*)record)->get_key_size() != kv.get_key_size()
        || memcmp(((KVRecord*)record)->get_key(), kv.get_key(), ksize) != 0)
        LOG_ERROR("Error: the result key of combiner is different!\n");

    if (kvsize <= ukvsize) {
        kv.convert((KVRecord*)record);
         if (kvsize < ukvsize) {
            slices.insert(std::make_pair(kv.get_record() + ukvsize - kvsize, ukvsize - kvsize));
        }
    }
    else {
        slices.insert(std::make_pair(kv.get_record(), ukvsize));
        if (kvsize + page->datasize > pagesize) page = add_page();

        kv.set_buffer(page->buffer + page->datasize);
        kv.convert((KVRecord*)record);
        page->datasize += kvsize;
    }

    return;
}

void CombineKVContainer::garbage_collection()
{
    ContainerIter dst_iter(this), src_iter(this);
    Page *dst_page = NULL, *src_page = NULL;
    int64_t dst_off = 0, src_off = 0;
    int kvsize;

    if (!slices.empty()) {
        dst_page = dst_iter.next();
        while ((src_page = src_iter.next()) != NULL) {
            src_off = 0;
            while (src_off < src_page->datasize) {
                char *src_buf = src_page->buffer + src_off;
                std::unordered_map < char *, int >::iterator iter = slices.find(src_buf);
                if (iter != slices.end()) {
                    src_off += iter->second;
                }
                else {
                    kv.set_buffer(src_buf);
                    kvsize = kv.get_record_size();
                    if (dst_page != src_page || dst_off != src_off) {
                        if (dst_off + kvsize > pagesize) {
                            dst_page->datasize = dst_off;
                            dst_page = dst_iter.next();
                            dst_off = 0;
                        }
                        for (int kk = 0; kk < kvsize; kk++) {
                            dst_page->buffer[dst_off + kk] = src_page->buffer[src_off + kk];
                        }
                    }
                    src_off += kvsize;
                    dst_off += kvsize;
                }
            }
            if (src_page == dst_page && src_off == dst_off) {
                dst_page = dst_iter.next();
                dst_off = 0;
            }
        }
        if (dst_page != NULL) dst_page->datasize = dst_off;
        while ((dst_page = dst_iter.next()) != NULL) {
            dst_page->datasize = 0;
        }
        slices.clear();
    }
    bucket->clear();
}


