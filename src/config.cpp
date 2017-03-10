/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include "config.h"


/// Number of hash bucket (2^x)
int BUCKET_COUNT = 17;

/// Communication buffer size
int64_t COMM_BUF_SIZE = 64 * 1024 * 1024;

/// Page size
int64_t DATA_PAGE_SIZE = 64 * 1024 * 1024;

/// Input buffer size
int64_t INPUT_BUF_SIZE = 64 * 1024 * 1024;

/// IO split unit
int64_t FILE_SPLIT_UNIT = 1;

/// Disk IO type
/// (0: c library io; 1: mpi io; 2; mpi collective io)
int DISK_IO_TYPE = 1;

/// Tail buffer size
int TAIL_BUF_SIZE = 1024 * 1024;

/// KV exchange communication mode
/// (0: collective; 1: point to point)
int KV_EXCH_COMM = 0;

/// Maximum number of pages
int MAX_PAGE_COUNT = 1024;

/// Number of set when converting KVs to KMVs (2^x)
int SET_COUNT = 1024 * 1024;

/// Maximum string size pass to map_text_file
int MAX_RECORD_SIZE = 1024 * 1024;

/// Unit size for comm buffer
int COMM_UNIT_SIZE = 4096;

/// Debug Level
int DBG_LEVEL = 0;

/// Record peakmem
int RECORD_PEAKMEM = 1;

int KTYPE = -2, VTYPE = -2;

// 0: POSIX interfaces
// 1: MPIIO interfaces
int READER_TYPE = 0;
int WRITER_TYPE = 0;
// 0: blocking collective communication
// 1: nonblocking collective communication
int SHUFFLE_TYPE = 0;

/// output profile and trace data
int OUTPUT_STAT = 0;
