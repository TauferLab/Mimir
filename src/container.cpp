#include <string.h>
#include <sys/stat.h>
#include "container.h"
#include <mpi.h>
#include "log.h"
#include "config.h"
#include "const.h"
#include "memory.h"
#include "stat.h"

using namespace MIMIR_NS;

int Container::object_id = 0;
int64_t Container::mem_bytes = 0;

void Container::addRef(Container *data)
{
    if (data)
        data->ref++;
}

void Container::subRef(Container *data)
{
    if (data) {
        data->ref--;
        if (data->ref == 0) {
            delete data;
            data = NULL;
        }
    }
}
