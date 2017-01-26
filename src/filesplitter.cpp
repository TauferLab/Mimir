#include "globals.h"
#include "config.h"
#include "const.h"
#include "log.h"
#include "filesplitter.h"

using namespace MIMIR_NS;

FileSplitter* FileSplitter::splitter = NULL;

void FileSplitter::_bcast_file_list(InputSplit *input){
    int   total_count = 0;
    char *tmp_buf = NULL;
    FileSeg *fileseg = NULL;

    MPI_Barrier(mimir_world_comm);

    if(mimir_world_rank == 0){
        while((fileseg = input->get_next_file()) != NULL){
            total_count += (int)(fileseg->filename.size() + 1);
            total_count += (int)sizeof(uint64_t);
        }
    }else{
        input->clear();
    }

    MPI_Bcast( &total_count, 1, MPI_INT, 0, mimir_world_comm);

    tmp_buf = new char[total_count];

    int off = 0;
    if(mimir_world_rank == 0){
        while((fileseg = input->get_next_file()) != NULL){
            memcpy(tmp_buf + off, fileseg->filename.c_str(),
                   (int)(fileseg->filename.size() + 1));
            off += (int)(fileseg->filename.size() + 1);
            memcpy(tmp_buf + off, &(fileseg->filesize), sizeof(uint64_t));
            off += (int)sizeof(uint64_t);
        }
        if(off != total_count) LOG_ERROR("Error: broadcast file list!\n");
        MPI_Bcast( tmp_buf, total_count, MPI_BYTE, 0, mimir_world_comm );
    }else{
        MPI_Bcast( tmp_buf, total_count, MPI_BYTE, 0, mimir_world_comm );
        while (off < total_count){
            FileSeg tmp;

            tmp.filename = tmp_buf + off;
            off += (int)strlen(tmp_buf + off) + 1;
            tmp.filesize = *(uint64_t*)(tmp_buf + off);
            off += (int)sizeof(uint64_t);

            tmp.startpos = 0;
            tmp.segsize = tmp.filesize;

            input->add_seg_file(&tmp);
        }
        if(off != total_count) LOG_ERROR("Error: broadcast file list!\n");
    }

    delete [] tmp_buf;

    LOG_PRINT(DBG_IO, "Broadcast file list (count=%d)\n", total_count);
}

void FileSplitter::_split(InputSplit *input, SplitPolicy policy){
    switch(policy){
    case BYSIZE: _split_by_size(input); break;
    case BYNAME: _split_by_name(input); break;
    }
}

void FileSplitter::_split_by_size(InputSplit *input){
    FileSeg *fileseg = NULL;
    uint64_t totalblocks = 0;

    while((fileseg = input->get_next_file()) != NULL)
        totalblocks += ROUNDUP(fileseg->filesize, FILE_SPLIT_UNIT);

    InputSplit tmpsplit;
    FileSeg tmpseg;

    int proc_rank = 0;
    uint64_t block_off = 0;
     while((fileseg = input->get_next_file()) != NULL){

        uint64_t proc_blocks = _get_proc_count(proc_rank, totalblocks);
        uint64_t file_off = 0;
        uint64_t file_blocks = ROUNDUP(fileseg->filesize, FILE_SPLIT_UNIT);

        while(file_blocks > 0){
            // partition whole file to pidx
            if(block_off + file_blocks <= proc_blocks){
                tmpseg.filename = fileseg->filename;
                tmpseg.filesize = fileseg->filesize;
                tmpseg.startpos = file_off;
                tmpseg.segsize  = fileseg->filesize - file_off;
                tmpsplit.add_seg_file(&tmpseg);
                //printf("%d %s:%ld+%ld\n", proc_rank, 
                //       tmpseg.filename.c_str(),
                //       tmpseg.startpos, tmpseg.segsize);
                block_off += file_blocks;
                file_blocks = 0;
            }else{
                tmpseg.filename = fileseg->filename;
                tmpseg.filesize = fileseg->filesize;
                tmpseg.startpos = file_off;
                tmpseg.segsize  = (proc_blocks - block_off) * FILE_SPLIT_UNIT;
                tmpsplit.add_seg_file(&tmpseg);
                //printf("%d %s:%ld+%ld\n", proc_rank, 
                //       tmpseg.filename.c_str(),
                //       tmpseg.startpos, tmpseg.segsize);
                file_blocks -= (proc_blocks - block_off);
                file_off += (proc_blocks - block_off) * FILE_SPLIT_UNIT;
                block_off = proc_blocks;
            }
            if( block_off == proc_blocks){
                //printf("%d add split!\n", proc_rank);
                files.push_back(tmpsplit);
                tmpsplit.clear();
                proc_rank += 1;
                block_off = 0;
                proc_blocks = _get_proc_count(proc_rank, totalblocks);

            }
        }

    }

     if((int)files.size() != mimir_world_size)
         LOG_ERROR("The split file count is error!\n");
}

void FileSplitter::_split_by_name(InputSplit *input){

    uint64_t totalcount = input->get_file_count();

    int proc_rank = 0;
    uint64_t file_count = 0;

    FileSeg *fileseg = NULL;
    InputSplit tmpsplit;
    while((fileseg = input->get_next_file()) != NULL){
        if(file_count < _get_proc_count(proc_rank, totalcount)){
            tmpsplit.add_seg_file(fileseg);
            file_count++;
        }
        if(file_count == _get_proc_count(proc_rank, totalcount)){
            files.push_back(tmpsplit);
            tmpsplit.clear();
            proc_rank++;
            file_count = 0;
        }
    }
}

InputSplit *FileSplitter::_get_my_split(){
    return &files[mimir_world_rank];
}

uint64_t FileSplitter::_get_proc_count(int rank, uint64_t totalcount){
    uint64_t localcount = totalcount / mimir_world_size;
    if(rank < (int)(totalcount % mimir_world_size)) localcount += 1;

    return localcount;
}


