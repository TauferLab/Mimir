#include <string.h>
#include <string>
#include "log.h"
#include "const.h"
#include "kvcontainer.h"
#include "recordformat.h"

#if 0
using namespace MIMIR_NS;
KVContainer::KVContainer()
    : Container(KVType)
{
    ksize = vsize = KVGeneral;
    kvcount = 0;
    page = NULL;
    pageoff = 0;
    LOG_PRINT(DBG_DATA, "DATA: KV Create (id=%d).\n", id);
}

KVContainer::~KVContainer()
{

    LOG_PRINT(DBG_DATA, "DATA: KV Destroy (id=%d).\n", id);
}

int KVContainer::getNextKV(char**pkey, int &keybytes, char **pvalue, int &valuebytes)
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

int KVContainer::addKV(const char *key, int keybytes,
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

    // add another page
    if (kvsize > (pagesize - page->datasize))
        page = add_page();

    // put KV data in
    char *ptr = page->buffer + page->datasize;
    record.set_buffer(ptr);
    record.set_key_value(key, keybytes, value, valuebytes);
    //PUT_KV_VARS(ksize, vsize, ptr, key, keybytes, value, valuebytes, kvsize);
    page->datasize += kvsize;

    kvcount += 1;

    return 0;
}

void KVContainer::print(FILE *fp, ElemType ktype, ElemType vtype)
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
#endif
