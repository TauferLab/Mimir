/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_CONTAINER_H
#define MIMIR_CONTAINER_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "log.h"
#include "globals.h"
#include "memory.h"
#include "stat.h"

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
    }

    virtual ~Container() {
        for (size_t i = 0; i < groups.size(); i++) {
            for (size_t j = 0; j < groups[i].pages.size(); j++) {
                mem_aligned_free(groups[i].pages[j].buffer);
                Container::mem_bytes -= pagesize;		
	    }
	}
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
	Container::mem_bytes += pagesize;
	PROFILER_RECORD_COUNT(COUNTER_MAX_PAGES, Container::mem_bytes, OPMAX);
        groups[groupid].pages.push_back(page);
        pageid = (int)groups[groupid].pages.size() - 1;
        return &groups[groupid].pages[pageid];
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

private:
    struct Group {
        std::vector<Page> pages;
    };
    std::vector<Group> groups;

public:
    static uint64_t mem_bytes;
};

}

#endif
