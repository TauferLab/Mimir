/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include <string.h>
#include <string>
#include "log.h"
#include "kvcontainer.h"
#include "recordformat.h"

#if 0
using namespace MIMIR_NS;

KVContainer::KVContainer() : BaseDatabase(true) {
    this->ksize = KTYPE;
    this->vsize = VTYPE;
    kv.set_kv_size(ksize, vsize);
    kvcount = 0;
    page = NULL;
    pageoff = 0;
}

KVContainer::~KVContainer() {
}

bool KVContainer::open() {
    page = NULL;
    pageoff = 0;
    iter = new ContainerIter(this);
    return true;
}

void KVContainer::close() {
    delete iter;
    page = NULL;
    pageoff = 0;
    return;
}

BaseRecordFormat* KVContainer::read() {
    char *ptr;
    int kvsize;

    if (page == NULL || pageoff >= page->datasize) {
        page = iter->next();
        pageoff = 0;
        if (page == NULL)
            return NULL;
    }

    ptr = page->buffer + pageoff;
    kv.set_buffer(ptr);
    kvsize = kv.get_record_size();

    pageoff += kvsize;
    return &kv;
}

void KVContainer::write(BaseRecordFormat *record) {
    if (page == NULL)
        page = add_page();
    int kvsize = record->get_record_size();
    if (kvsize > pagesize)
        LOG_ERROR("Error: KV size (%d) is larger \
                  than one page (%ld)\n", kvsize, pagesize);

    if (kvsize > (pagesize - page->datasize))
        page = add_page();

    char *ptr = page->buffer + page->datasize;
    kv.set_buffer(ptr);
    kv.convert((KVRecord*)record);
    page->datasize += kvsize;

    kvcount += 1;
}
#endif
