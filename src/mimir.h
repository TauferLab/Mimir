/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#ifndef MIMIR_H
#define MIMIR_H

#include "interface.h"
#include "memory.h"
#include "recordformat.h"
#include "inputsplit.h"
#include "filesplitter.h"
#include "filereader.h"
#include "filewriter.h"
#include "kvcontainer.h"
#include "mimircontext.h"
#include "var128int.h"

void Mimir_Init(int*, char ***, MPI_Comm);
void Mimir_Finalize();

void Mimir_stat(const char*);

#endif
