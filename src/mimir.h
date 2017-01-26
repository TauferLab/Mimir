#ifndef MIMIR_H
#define MIMIR_H

#include "inputsplit.h"
#include "filesplitter.h"
#include "basefilereader.h"
#include "filereader.h"
#include "baserecordformat.h"
#include "mapreduce.h"

void Mimir_Init(int*, char ***, MPI_Comm);
void Mimir_Finalize();


#endif
