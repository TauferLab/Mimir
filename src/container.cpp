/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
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

uint64_t Container::mem_bytes = 0;
