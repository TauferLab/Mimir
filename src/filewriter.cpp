/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */

#include "filewriter.h"

using namespace MIMIR_NS;

FileWriter* FileWriter::writer = NULL;
FileWriter* FileWriter::getWriter(MPI_Comm comm, const char *filename) {
    //if (writer != NULL) delete writer;
    if (WRITER_TYPE == 0) {
        writer = new FileWriter(comm, filename);
    } else if (WRITER_TYPE == 1) {
        writer = new DirectFileWriter(comm, filename);
    } else if (WRITER_TYPE == 2) {
        writer = new MPIFileWriter(comm, filename); 
    } else {
        LOG_ERROR("Error writer type %d\n", WRITER_TYPE);
    }
    return writer;
}
