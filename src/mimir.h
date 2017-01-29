#ifndef MIMIR_H
#define MIMIR_H

#include "inputsplit.h"
#include "filesplitter.h"
#include "filereader.h"
#include "recordformat.h"
#include "mapreduce.h"
#include "memory.h"

void Mimir_Init(int*, char ***, MPI_Comm);
void Mimir_Finalize();


#endif
