#include <string.h>
#include <string>
#include "combinekvcontainer.h"
#include "log.h"
#include "const.h"
#include "recordformat.h"

using namespace MIMIR_NS;

CombineKVContainer::CombineKVContainer()
    : KVContainer()
{
    ksize = vsize = KVGeneral;
    kvcount = 0;
    //mr = NULL;
    //mycombiner = NULL;
    bucket = NULL;
    page = NULL;
    LOG_PRINT(DBG_DATA, "DATA: KV Create (id=%d).\n", id);
}

CombineKVContainer::~CombineKVContainer()
{
    if (bucket != NULL) {
        delete bucket;
    }

    LOG_PRINT(DBG_DATA, "DATA: KV Destroy (id=%d).\n", id);
}

int CombineKVContainer::getNextKV(char**pkey, int &keybytes, char **pvalue, int &valuebytes)
{
    if (page == NULL || pageoff >= page->datasize) {
        page = get_next_page();
        pageoff = 0;
        if (page == NULL)
            return -1;
    }

    char *ptr = page->buffer + pageoff;
    int kvsize;
    KVRecord record(ksize, vsize);
    record.set_buffer(ptr);
    *pkey = record.get_key();
    keybytes = record.get_key_size();
    *pvalue = record.get_val();
    valuebytes = record.get_val_size();
    kvsize = record.get_record_size();
    //GET_KV_VARS(ksize, vsize, ptr, 
    //            *pkey, keybytes, *pvalue, valuebytes, kvsize);
    pageoff += kvsize;

    return kvsize;
}

#if 0
void CombineKVContainer::set_combiner(MapReduce *_mr, UserCombiner _combiner)
{
    mr = _mr;
    mycombiner = _combiner;

    if (mycombiner != NULL) {
        bucket = new CombinerHashBucket(this);
    }
}
#endif

int CombineKVContainer::addKV(const char *key, int keybytes,
                    const char *value, int valuebytes)
{
    KVRecord record(ksize, vsize);

    if (page == NULL)
        page = add_page();

    // get the size of the KV
    int kvsize = record.get_head_size() + keybytes + valuebytes;
    //GET_KV_SIZE(ksize, vsize, keybytes, valuebytes, kvsize);

    // KV size should be smaller than page size.
    if (kvsize > pagesize)
        LOG_ERROR("Error: KV size (%d) is larger \
                  than one page (%ld)\n", kvsize, pagesize);

    // without combiner
    //if (mycombiner == NULL) {

        // add another page
        if (kvsize > (pagesize - page->datasize))
            page = add_page();

        // put KV data in
        char *ptr = page->buffer + page->datasize;
        record.set_buffer(ptr);
        record.set_key_value(key, keybytes, value, valuebytes);
        //PUT_KV_VARS(ksize, vsize, ptr, key, keybytes, value, valuebytes, kvsize);
        page->datasize += kvsize;

        // with combiner
    //}
    //else {
        // check the bucket
        u = bucket->findElem(key, keybytes);
        // the key is not in the bucket
        if (u == NULL) {
            CombinerUnique tmp;
            tmp.next = NULL;

            // find a hole to store the KV
            std::unordered_map < char *, int >::iterator iter;
            for (iter = slices.begin(); iter != slices.end(); iter++) {

                char *sbuf = iter->first;
                int ssize = iter->second;

                // the hole is big enough to store the KV
                if (ssize >= kvsize) {

                    tmp.kv = sbuf + (ssize - kvsize);
                    record.set_buffer(tmp.kv);
                    record.set_key_value(key, keybytes, value, valuebytes);
                    //PUT_KV_VARS(ksize, vsize, tmp.kv, key, keybytes,
                    //            value, valuebytes, kvsize);

                    if (iter->second == kvsize)
                        slices.erase(iter);
                    else
                        slices[iter->first] -= kvsize;

                    break;
                }
            }
            // Add the KV at the tail of KV Container
            if (iter == slices.end()) {

                if (kvsize > (pagesize - page->datasize))
                    page = add_page();

                tmp.kv = page->buffer + page->datasize;
                record.set_buffer(tmp.kv);
                record.set_key_value(key, keybytes, value, valuebytes);
                kvsize = record.get_record_size();
                //PUT_KV_VARS(ksize, vsize, tmp.kv, key, keybytes,
                //            value, valuebytes, kvsize);
                page->datasize += kvsize;

                //slices.insert(std::make_pair(tmp.kv, kvsize));

            }

            bucket->insertElem(&tmp);

            // the key is in the bucket
        }
        else {

            record.set_buffer(u->kv);
            ukey = record.get_key();
            ukeybytes = record.get_key_size();
            uvalue = record.get_val();
            uvaluebytes = record.get_val_size();
            ukvsize = record.get_record_size();
            //GET_KV_VARS(ksize, vsize, u->kv, ukey, ukeybytes,
            //            uvalue, uvaluebytes, ukvsize);

            // invoke user-defined combine function
            //mycombiner(mr, key, keybytes, uvalue, uvaluebytes,
            //           value, valuebytes, mr->myptr);

        }
    //}

    kvcount += 1;

    return 0;
}


int CombineKVContainer::updateKV(const char *newkey, int newkeysize,
                       const char *newval, int newvalsize)
{
    KVRecord record(ksize, vsize);
    // check if the key is same
    if (newkeysize != ukeybytes || memcmp(newkey, ukey, ukeybytes) != 0)
        LOG_ERROR("Error: the result key of combiner is different!\n");

    int kvsize;
    // get combined KV size
    kvsize = record.get_head_size() + newkeysize + newvalsize;
    //GET_KV_SIZE(ksize, vsize, newkeysize, newvalsize, kvsize);

    // replace the exsiting KV
    if (kvsize <= ukvsize) {
        record.set_buffer(u->kv);
        record.set_key_value(ukey, ukeybytes, newval, newvalsize);
        kvsize = record.get_record_size();
        //PUT_KV_VARS(ksize, vsize, u->kv, ukey, ukeybytes, newval, newvalsize, kvsize);
        if (kvsize < ukvsize) {
            slices.insert(std::make_pair((u->kv + ukvsize - kvsize), ukvsize - kvsize));
        }

        // add at the tail
    }
    else {
        slices.insert(std::make_pair(u->kv, ukvsize));
        // add at the end of buffers
        if (kvsize > (pagesize - page->datasize))
            page = add_page();

        u->kv = page->buffer + page->datasize;

        record.set_buffer(u->kv);
        record.set_key_value(ukey, ukeybytes, newval, newvalsize);
        kvsize = record.get_record_size();
        //PUT_KV_VARS(ksize, vsize, u->kv, ukey, ukeybytes, newval, newvalsize, kvsize);
        page->datasize += kvsize;
    }
    return 0;
}


void CombineKVContainer::gc()
{
    //if (mycombiner != NULL && get_group_count() > 0 && slices.empty() == false) {
    //    LOG_PRINT(DBG_MEM, "Key Value: garbege collection (size=%ld)\n", slices.size());

#if 0
        int dst_pid = 0, src_pid = 0;
        int64_t dst_off = 0, src_off = 0;

        char *dst_buf = NULL;
        char *src_buf = pages[0].buffer;

        // scan all pages
        while (src_pid < (int)pages.size()) {
            src_off = 0;
            // scan page src_pid
            while (src_off < pages[src_pid].datasize) {
                // get current input buffer
                src_buf = pages[src_pid].buffer + src_off;

                // find the buffer
                std::unordered_map < char *, int >::iterator iter = slices.find(src_buf);

                // find a slice
                if (iter != slices.end()) {
                    if (dst_buf == NULL) {
                        dst_pid = src_pid;
                        dst_off = src_off;
                        dst_buf = src_buf;
                    }
                    src_off += iter->second;
                }
                else {
                    // get the KV
                    char *key = NULL, *value = NULL;
                    int keybytes = 0, valuebytes = 0, kvsize = 0;
                    GET_KV_VARS(ksize, vsize, src_buf, key, keybytes, value, valuebytes, kvsize);
                    // copy the KV
                    if (dst_buf != NULL && src_buf != dst_buf) {
                        // jump to the next page
                        if (pagesize - dst_off < kvsize) {
                            pages[dst_pid].datasize = dst_off;
                            dst_pid += 1;
                            dst_off = 0;
                            dst_buf = pages[dst_pid].buffer;
                        }
                        // copy the KV
                        memcpy(dst_buf, src_buf, kvsize);
                        dst_off += kvsize;
                        dst_buf += kvsize;
                    }
                    src_off += kvsize;
                }
            }
            src_pid += 1;
        }
        // free extra space
        for (int i = dst_pid + 1; i < (int)pages.size(); i++) {
            mem_aligned_free(pages[i].buffer);
            pages[i].buffer = NULL;
            pages[i].datasize = 0;
        }
        if (dst_buf != NULL) {
            pages[dst_pid].datasize = dst_off;
            //npages = dst_pid + 1;
        }
        slices.clear();
#endif
    //}
}

void CombineKVContainer::print(FILE *fp, ElemType ktype, ElemType vtype)
{
    char *key, *value;
    int keybytes, valuebytes;

    printf("key\tvalue\n");

#if 0
    for (int i = 0; i < (int)pages.size(); i++) {
        acquire_page(i);
        int offset = getNextKV(&key, keybytes, &value, valuebytes);
        while (offset != -1) {
            if (ktype == StringType)
                fprintf(fp, "%s", key);
            else if (ktype == Int32Type)
                fprintf(fp, "%d", *(int*) key);
            else if (ktype == Int64Type)
                fprintf(fp, "%ld", *(int64_t*) key);

            if (vtype == StringType)
                fprintf(fp, "\t%s", value);
            else if (vtype == Int32Type)
                fprintf(fp, "\t%d", *(int*) value);
            else if (vtype == Int64Type)
                fprintf(fp, "\t%ld", *(int64_t*) value);

            fprintf(fp, "\n");
            offset = getNextKV(&key, keybytes, &value, valuebytes);
        }

        release_page(i);
    }
#endif
}
