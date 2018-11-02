/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_CONTAINER_ITER_H
#define MIMIR_CONTAINER_ITER_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "log.h"
#include "memory.h"
#include "container.h"

namespace MIMIR_NS {

class ContainerIter
{
  public:
    ContainerIter(Container *container)
    {
        this->container = container;
        groupid = 0;
        pageid = 0;
    }

    ~ContainerIter() {}

    Page *next()
    {
        while (groupid < container->get_group_count()) {
            if (pageid >= container->get_page_count(groupid)) {
                groupid++;
                pageid = 0;
                continue;
            }
            Page *page = container->get_page(pageid, groupid);
            pageid++;
            return page;
        }

        groupid = 0;
        pageid = 0;
        return NULL;
    }

  private:
    Container *container;
    int groupid, pageid;
};

} // namespace MIMIR_NS

#endif
