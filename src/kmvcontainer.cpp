#include "kmvcontainer.h"

using namespace MIMIR_NS;

void KMVContainer::convert(KVContainer *kv) {
    const char *key, *value;
    int keybytes, valuebytes;

    kv->open();
    ReducerUnique u;
    KVRecord *record = kv->read();
    while (record != NULL) {
        key = record->get_key();
        keybytes = record->get_key_size();
        value = record->get_val();
        valuebytes = record->get_val_size();

        u.key = (char*)key;
        u.keybytes = keybytes;
        u.mvbytes = valuebytes;

        h.insertElem(&u);

        record = kv->read();
    }
    kv->close();

    // Set pointers to hold MVs
    char *page_buf = NULL;
    int64_t page_off = 0;
    Page *page = NULL;
    int page_id = 0;
    ReducerSet *pset = h.BeginSet();
    while (pset != NULL) {
        if (page_buf == NULL || page_id != pset->pid) {
            page = add_page();
            page_buf = page->buffer;
            page_off = 0;
         }

        if (kv->vsize == KVGeneral) {
            pset->soffset = (int*) (page_buf + page_off);
            page_off += sizeof(int) * (pset->nvalue);
        }
        else {
            pset->soffset = NULL;
        }

        pset->voffset = page_buf + page_off;
        pset->curoff = pset->voffset;
        page_off += pset->mvbytes;

        if (page_off > get_page_size())
            LOG_ERROR
                ("Error: the pointer of page %d exceeds the range (page_off=%ld, iset=%ld),pset=%p!\n",
                 page_id, page_off, h.iset, pset);

        pset = h.NextSet();
    }

    // Modify the pointers
    ReducerUnique *uq = h.BeginUnique();
    while (uq != NULL) {

        uq->lastset = uq->firstset;

        uq = h.NextUnique();
    }

    kv->open();
    record = kv->read();
    while (record != NULL) {
        key = record->get_key();
        keybytes = record->get_key_size();
        value = record->get_val();
        valuebytes = record->get_val_size();

        ReducerUnique *punique = h.findElem(key, keybytes);
        ReducerSet *pset = punique->lastset;

        if (kv->vsize == KVGeneral) {
            pset->soffset[pset->ivalue] = valuebytes;
        }

        memcpy(pset->curoff, value, valuebytes);
        pset->curoff += valuebytes;
        pset->ivalue += 1;

        if (pset->ivalue == pset->nvalue) {
            punique->lastset = punique->lastset->next;
        }

        record = kv->read();
    }
    kv->close();

    LOG_PRINT(DBG_GEN, "MapReduce: convert end.\n");
}

