#ifndef MIMIR_CONTAINER_H
#define MIMIR_CONTAINER_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "log.h"
#include "const.h"
#include "memory.h"

namespace MIMIR_NS {

enum DataType { ByteType, KVType };

struct Page {
    int64_t  datasize;
    char*    buffer;
};

class Container {
public:
    Container(DataType datatype = ByteType) {
        this->datatype = datatype;
        pagesize = DATA_PAGE_SIZE;
        id = ref = 0;
        groupid = 0;
        pageid = 0;
        id = Container::object_id++;
    }

    virtual ~Container() {
    }

    Page* get_page(int pageid, int groupid=0) {
        if (groupid >= (int)groups.size())
            return NULL;
        if(pageid >= (int)groups[groupid].pages.size())
            return NULL;
        return &groups[groupid].pages[pageid];
    }

    Page* add_page(int groupid=0) {
        int pageid = 0;
        if (groupid >= (int)groups.size()) {
            Group group;
            groups.push_back(group);
            groupid = (int)groups.size() - 1;
        }
        Page page;
        page.datasize = 0;
        page.buffer = (char*) mem_aligned_malloc(MEMPAGE_SIZE, pagesize);
        groups[groupid].pages.push_back(page);
        pageid = (int)groups[groupid].pages.size() - 1;
        return &groups[groupid].pages[pageid];
    }

    Page* get_next_page() {
        while (groupid < get_group_count()) {
            if (pageid >= get_page_count(groupid)) {
                groupid ++;
                pageid = 0;
                continue;
            }
            Page *page = get_page(groupid, pageid);
            pageid ++;
            return page;
        }
        groupid = 0;
        pageid = 0;
        return NULL;
    }

    int get_group_count() {
        return (int)groups.size();
    }

    int get_page_count(int groupid = 0) {
        if (groupid >= (int)groups.size())
            return 0;
        return (int)groups[groupid].pages.size();
    }

    int64_t get_page_size() {
        return pagesize;
    }

protected:
    int64_t  pagesize;
    DataType datatype;
    int id, ref;

private:
    struct Group {
        std::vector<Page> pages;
    };
    std::vector<Group> groups;
    int groupid, pageid;

public:
    static int object_id;
    static int64_t mem_bytes;
    static void addRef(Container*);
    static void subRef(Container*);
};

}

#endif
