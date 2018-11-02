//
// (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego
//     Supercomputer Center, National University of Defense Technology,
//     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
//
//     See COPYRIGHT in top-level directory.
//

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <dirent.h>

#include "log.h"
#include "globals.h"
#include "inputsplit.h"

using namespace MIMIR_NS;

void InputSplit::print()
{
    FileSeg *fileseg = NULL;
    while ((fileseg = get_next_file()) != NULL) {
        printf(
            "%d[%d]File=%s,filesize=%ld,segment=%ld+%ld(max:%ld),ranks=%d->%d,"
            "order=%d\n",
            mimir_world_rank, mimir_world_size, fileseg->filename.c_str(),
            fileseg->filesize, fileseg->startpos, fileseg->segsize,
            fileseg->maxsegsize, fileseg->startrank, fileseg->endrank,
            fileseg->readorder);
    }
}

void InputSplit::get_file_list(const char *filepath, int recurse)
{
    struct stat inpath_stat;
    int err = stat(filepath, &inpath_stat);
    if (err)
        LOG_ERROR("Error in get input files filepath=%s, err=%d\n", filepath,
                  err);

    if (S_ISREG(inpath_stat.st_mode)) {
        int64_t fsize = inpath_stat.st_size;
        FileSeg seg;
        seg.filename = filepath;
        seg.filesize = fsize;
        seg.startpos = 0;
        seg.segsize = fsize;
        seg.maxsegsize = fsize;
        seg.startrank = mimir_world_rank;
        seg.endrank = mimir_world_rank;
        seg.readorder = -1;
        filesegs.push_back(seg);
    }
    else if (S_ISDIR(inpath_stat.st_mode)) {
        struct dirent *ep;
        DIR *dp = opendir(filepath);
        if (!dp) LOG_ERROR("Error in get input files\n");

        while ((ep = readdir(dp)) != NULL) {
#ifdef BGQ
            if (ep->d_name[1] == '.') continue;
#else
            if (ep->d_name[0] == '.') continue;
#endif
            char newstr[MAXLINE];
#ifdef BGQ
            sprintf(newstr, "%s/%s", filepath, &(ep->d_name[1]));
#else
            sprintf(newstr, "%s/%s", filepath, ep->d_name);
#endif
            err = stat(newstr, &inpath_stat);
            if (err) LOG_ERROR("Error in get input files, err=%d\n", err);

            if (S_ISREG(inpath_stat.st_mode)) {
                int64_t fsize = inpath_stat.st_size;
                FileSeg seg;
                seg.filename = newstr;
                seg.filesize = fsize;
                seg.startpos = 0;
                seg.segsize = fsize;
                seg.maxsegsize = fsize;
                seg.startrank = mimir_world_rank;
                seg.endrank = mimir_world_rank;
                seg.readorder = -1;
                filesegs.push_back(seg);
            }
            else if (S_ISDIR(inpath_stat.st_mode) && recurse) {
                get_file_list(newstr, recurse);
            }
        }
    }
}
