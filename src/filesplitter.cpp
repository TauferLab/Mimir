#include <sys/stat.h>
#include <dirent.h>
#include "log.h"
#include "const.h"
#include "filesplitter.h"


using namespace MIMIR_NS;

int64_t FileSplitter::get_file_count() {

    if(shared)
        return (int64_t)filesegs.size();

    return (int64_t)infiles.size();
}

const char* FileSplitter::get_file_name(int64_t i) {

    if(shared)
        return filesegs[i].filename.c_str();

    return infiles[i].filename.c_str();
}

int64_t FileSplitter::get_start_offset(int64_t i) {

    if(shared)
        return filesegs[i].startoff;

    return 0;
}

int64_t FileSplitter::get_end_offset(int64_t i) {

    if(shared)
        return filesegs[i].endoff;

    return infiles[i].filesize;
}

int64_t FileSplitter::get_file_size(int64_t i) {

    if(shared)
        return filesegs[i].filesize;

    return infiles[i].filesize;
}

int64_t FileSplitter::get_total_size(){
    return totalsize;
}

int64_t FileSplitter::get_block_size(){
    return blocksize;
}

const char* FileSplitter::get_group_filename(int group_id){
    int i = group_to_idx[group_id];

    if(i != -1)
        return filesegs[i].filename.c_str();

    return NULL;
}

void FileSplitter::get_group_ranks(
                                    int group_id,
                                    int &low, 
                                    int &high){

    int i = group_to_idx[group_id];
    if(i != -1){
        low = filesegs[i].start_rank;
        high = filesegs[i].end_rank;
        printf("%d[%d] i=%d, group_id=%d, filesegs.group_id=%d, ranks%d->%d\n", 
               me, nprocs, i, group_id, filesegs[i].group_id, low, high);
    }else{
        low = high = me;
    }
}

int64_t FileSplitter::get_group_maxblocks(int64_t group_id){
    int64_t ret = 0;

    int i = group_to_idx[group_id];
    if(i != -1)
        ret = filesegs[i].max_blocks;

    return ret;
}

int FileSplitter::get_file_groupid(int64_t i){
    return filesegs[i].group_id;
}

int64_t FileSplitter::get_group_fileid(int group_id){
    return group_to_idx[group_id];
}

void FileSplitter::print(){
    if(shared){
        std::vector<FileSegment>::iterator iter = filesegs.begin();
        for(; iter != filesegs.end(); iter++){
            printf("%d[%d] %s:%ld[%ld->%ld],group_id=%d,max_blocks=%ld\n", 
                   me, nprocs, 
                   iter->filename.c_str(),
                   iter->filesize, 
                   iter->startoff, 
                   iter->endoff,
                   iter->group_id,
                   iter->max_blocks);
        }
    }
}

void FileSplitter::split(){

    _get_input_files(filepath.c_str(), shared, recurse);

    if (shared) {
        int *send_count = new int[nprocs];
        int *send_displs = new int[nprocs];
        char *send_buf = NULL;
        char *recv_buf = NULL;
        int total_count = 0;

        if(me == 0) {
            _partition_files();
            for(int i = 0; i < nprocs; i++) send_count[i] = 0;
            total_count=_get_send_counts(send_count);
        }

        int recv_count;
        MPI_Scatter(send_count, 1, MPI_INT, &recv_count, 1, MPI_INT, 0, comm);

        if (me == 0) {
            send_displs[0] = 0;
            for (int i = 1; i < nprocs; i++) {
                send_displs[i] = send_displs[i - 1] + send_count[i - 1];
            }
        }

        send_buf = new char[total_count];
        recv_buf = new char[recv_count];

        if (me == 0) _get_send_data(send_buf);

        MPI_Scatterv(send_buf, send_count, send_displs, MPI_BYTE,
                     recv_buf, recv_count, MPI_BYTE, 0, comm);

        _get_recv_data(recv_buf, recv_count);

        delete [] send_count;
        delete [] send_displs;
        delete [] send_buf;
        delete [] recv_buf;
    }
}

void FileSplitter::_get_recv_data(char *recv_buf, int recv_count){
    char *ptr = recv_buf;
    char *ptr_end = recv_buf + recv_count;
    totalsize = 0;
    //int i=0;
    while(ptr < ptr_end){
        FileSegment seg;
        seg.filename = ptr;
        ptr += strlen(ptr) + 1;
        seg.filesize = *(int64_t*)ptr;
        ptr += sizeof(int64_t);
        seg.startoff = *(int64_t*)ptr;
        ptr += sizeof(int64_t);
        seg.endoff = *(int64_t*)ptr;
        ptr += sizeof(int64_t);
        seg.max_blocks = *(int64_t*)ptr;
        ptr += sizeof(int64_t);
        seg.start_rank = *(int*)ptr;
        ptr += sizeof(int);
        seg.end_rank = *(int*)ptr;
        ptr += sizeof(int);
        seg.group_id = *(int*)ptr;
        ptr += sizeof(int);

        std::vector<FileSegment>::iterator iter = filesegs.begin();
        for(; iter != filesegs.end(); iter++){
            if( seg.group_id < iter->group_id ){
                filesegs.insert(iter, seg);
                break;
            }
        }
        if(iter == filesegs.end())
            filesegs.push_back(seg);

        LOG_PRINT(DBG_IO, "File=%s, size=%ld, offset=[%ld, %ld], ranks=[%d, %d], group_id=%d\n", 
                  seg.filename.c_str(), seg.filesize, 
                  seg.startoff, seg.endoff, 
                  seg.start_rank, seg.end_rank, 
                  seg.group_id);

        totalsize += seg.endoff - seg.startoff;
    }

    for(int i = 0; i < (int)filesegs.size(); i++){
        if(filesegs[i].group_id == GROUP1){
            group_to_idx[GROUP1] = i;
            printf("%d[%d] GROUP1=%d\n", me, nprocs, i);
        }
        else if(filesegs[i].group_id == GROUP2){
            group_to_idx[GROUP2] = i;
            printf("%d[%d] GROUP2=%d\n", me, nprocs, i);
        }
    }
}

void FileSplitter::_get_send_data(char *send_buf){
    int offset = 0;
    std::vector<FileInfo>::iterator iter = infiles.begin();
    for( ; iter != infiles.end(); iter++ ) {
        if(iter->filesize == 0) continue;
        for(int i = iter->start_rank; i < iter->end_rank + 1; i++){
            memcpy(send_buf + offset, iter->filename.c_str(),
                   iter->filename.size() + 1);
            offset += (int) (iter->filename.size()) + 1;
            *(int64_t*)(send_buf + offset) = iter->filesize;
            offset += (int)sizeof(int64_t);
            *(int64_t*)(send_buf + offset) = 
                iter->offstarts[i - iter->start_rank];
            offset += (int)sizeof(int64_t);
            *(int64_t*)(send_buf + offset) = 
                iter->offstarts[i - iter->start_rank + 1];
            offset += (int)sizeof(int64_t);
            *(int64_t*)(send_buf + offset) = iter->max_blocks;
            offset += (int)sizeof(int64_t); 
            *(int*)(send_buf + offset) = iter->start_rank;
            offset += (int)sizeof(int);
            *(int*)(send_buf + offset) = iter->end_rank;
            offset += (int)sizeof(int);
            *(int*)(send_buf + offset) = iter->group_id;
            offset += (int)sizeof(int);
        }
    }
}

int FileSplitter::_get_send_counts(int *send_count){

    std::vector<FileInfo>::iterator iter = infiles.begin();
    for( ; iter != infiles.end(); iter++ ) {

        if(iter->filesize == 0) continue;

        for(int i = iter->start_rank; i < iter->end_rank+1; i++){
            send_count[i] += (int)(iter->filename.size()+1);
            send_count[i] += (int)sizeof(int64_t)*4;
            send_count[i] += (int)sizeof(int)*3;
        }
    }

    int total_count=0;
    for(int i = 0; i < nprocs; i++) total_count += send_count[i];

    return total_count;
}



void FileSplitter::_partition_files(){

    int64_t *block_count = new int64_t[nprocs];

    // partition blocks
    for(int i = 0; i < nprocs; i++){
        block_count[i] = totalblocks / nprocs;
        if(i < totalblocks % nprocs) block_count[i] += 1;
    }

    // partition files
    int     pidx = 0;
    int64_t boff = 0;
    std::vector<FileInfo>::iterator iter = infiles.begin();
    for( ; iter != infiles.end(); iter++ ) {
        // skip empty file
        if(iter->filesize == 0) continue;
        // first offset is always zero
        iter->offstarts.push_back(0);
        int64_t foff = 0;
        int64_t fblocks = ROUNDUP(iter->filesize, blocksize);
        iter->start_rank = pidx;
        iter->max_blocks = 0;
        while(fblocks > 0){
            // partition whole file to pidx
            if(boff + fblocks <= block_count[pidx]){
                iter->offstarts.push_back(iter->filesize);
                iter->end_rank = pidx;
                boff += fblocks;
                if(fblocks > iter->max_blocks)
                    iter->max_blocks = fblocks;
                fblocks = 0;
            }else{
                foff += (block_count[pidx] - boff) * blocksize;
                iter->offstarts.push_back(foff);
                if((block_count[pidx] - boff) > iter->max_blocks)
                    iter->max_blocks = block_count[pidx] - boff;
                fblocks -= (block_count[pidx] - boff);
                boff += (block_count[pidx] - boff);
            }
            if( boff == block_count[pidx]){
                pidx+=1;
                boff=0;
            }
        }
    }

    delete [] block_count;

    // schedule communication group
    int max_rank = -1;
    iter = infiles.begin();
    for( ; iter != infiles.end(); iter++ ) {
        if(iter->end_rank == iter->start_rank){
            iter->group_id = GROUPNON;
        }else{
            if(iter->start_rank == max_rank){
                iter->group_id = GROUP2;
            }else{
                iter->group_id = GROUP1;
                max_rank = iter->end_rank;
            }
        }
    }

}

void FileSplitter::_get_input_files(const char* filepath, 
                                    int sharedflag, 
                                    int recurse){

    if (!sharedflag || (sharedflag && me == 0)) {
        struct stat inpath_stat;
        int err = stat(filepath, &inpath_stat);
        if (err) LOG_ERROR("Error in get input files, err=%d\n", err);

        if (S_ISREG(inpath_stat.st_mode)) {
            int64_t fsize = inpath_stat.st_size;
            totalsize += fsize;
            totalblocks += (fsize + blocksize - 1) / blocksize;
            FileInfo finfo(std::string(filepath), fsize);
            infiles.push_back(finfo);
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
                    totalsize+=fsize;
                    totalblocks += (fsize + blocksize - 1) / blocksize;
                    FileInfo finfo(std::string(newstr), fsize);
                    infiles.push_back(finfo);
                }
                else if (S_ISDIR(inpath_stat.st_mode) && recurse) {
                    _get_input_files(newstr, sharedflag, recurse);
                }
            }
        }
    }
}
