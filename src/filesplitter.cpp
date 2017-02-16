/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include "log.h"
#include "config.h"
#include "globals.h"
#include "filesplitter.h"

using namespace MIMIR_NS;

FileSplitter* FileSplitter::splitter = NULL;

void FileSplitter::bcast_file_list(InputSplit *input){
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
            tmp.maxsegsize = tmp.filesize;
            tmp.startrank = mimir_world_rank;
            tmp.endrank = mimir_world_rank;
            tmp.readorder = -1;

            input->add_seg_file(&tmp);
        }
        if(off != total_count) LOG_ERROR("Error: broadcast file list!\n");
    }

    delete [] tmp_buf;

    LOG_PRINT(DBG_IO, "Broadcast file list (count=%d)\n", total_count);
}

void FileSplitter::split_files(InputSplit *input, SplitPolicy policy){
    switch(policy){
    case BYSIZE: split_by_size(input); break;
    case BYNAME: split_by_name(input); break;
    }
}

void FileSplitter::split_by_size(InputSplit *input){

    FileSeg *fileseg = NULL;
    uint64_t totalblocks = 0;
    // compute total number of blocks
    while((fileseg = input->get_next_file()) != NULL)
        totalblocks += ROUNDUP(fileseg->filesize, FILE_SPLIT_UNIT);

    InputSplit tmpsplit;
    int max_rank = -1, proc_rank = 0;
    uint64_t proc_off = 0;
    uint64_t proc_blocks = get_proc_count(proc_rank, totalblocks);
    uint64_t offsets[mimir_world_size + 1];

    while((fileseg = input->get_next_file()) != NULL){

        uint64_t maxsegsize = 0;
        uint64_t file_off = 0, file_blocks = 0;
        int start_rank = proc_rank, end_rank = proc_rank;

        file_blocks = ROUNDUP(fileseg->filesize, FILE_SPLIT_UNIT);

        offsets[0] = 0;
        while(file_blocks > 0){
            if(proc_off + file_blocks <= proc_blocks){
                proc_off += file_blocks;
                file_off = fileseg->filesize;
                file_blocks = 0;
                end_rank = proc_rank;
                offsets[proc_rank - start_rank + 1] = file_off;
                uint64_t segsize = file_off - offsets[proc_rank - start_rank];
                if(segsize > maxsegsize) maxsegsize = segsize;
            }else{
                file_blocks -= (proc_blocks - proc_off);
                file_off += (proc_blocks - proc_off) * FILE_SPLIT_UNIT;
                proc_off = proc_blocks;
                offsets[proc_rank - start_rank + 1] = file_off;
                uint64_t segsize = file_off - offsets[proc_rank - start_rank];
                if(segsize > maxsegsize) maxsegsize = segsize;
            }
            if( proc_off == proc_blocks){
                proc_rank += 1;
                proc_off = 0;
                proc_blocks = get_proc_count(proc_rank, totalblocks);
            }
        }

        // this is a share file
        if(end_rank > start_rank){
            for(int i = start_rank; i <= end_rank; i++){
                FileSeg    tmpseg;
                tmpseg.filename = fileseg->filename;
                tmpseg.filesize = fileseg->filesize;
                tmpseg.startpos = offsets[i - start_rank];
                tmpseg.segsize = offsets[i - start_rank + 1] 
                                - offsets[i - start_rank];
                tmpseg.maxsegsize = maxsegsize;
                tmpseg.startrank = start_rank;
                tmpseg.endrank = end_rank;
                if(start_rank > max_rank) tmpseg.readorder = 0;
                else tmpseg.readorder = 1;
                tmpsplit.add_seg_file(&tmpseg);
                if(i < proc_rank){
                    files.push_back(tmpsplit);
                    tmpsplit.clear();
                }
            }
            if(start_rank > max_rank) max_rank = end_rank;
        }else{
            FileSeg    tmpseg;
            tmpseg.filename = fileseg->filename;
            tmpseg.filesize = fileseg->filesize;
            tmpseg.startpos = 0;
            tmpseg.segsize = fileseg->filesize;
            tmpseg.maxsegsize = fileseg->filesize;
            tmpseg.startrank = start_rank;
            tmpseg.endrank = end_rank;
            tmpseg.readorder = -1;
            tmpsplit.add_seg_file(&tmpseg);
            if(proc_rank > end_rank){
                files.push_back(tmpsplit);
                tmpsplit.clear();
            }
        }

    }

    if(tmpsplit.get_file_count() > 0)
        LOG_ERROR("Split state error!\n");

     if((int)files.size() != mimir_world_size)
         LOG_ERROR("The split file count %ld is error!\n", files.size());
}

void FileSplitter::split_by_name(InputSplit *input){

    uint64_t totalcount = input->get_file_count();

    int proc_rank = 0;
    uint64_t file_count = 0;

    FileSeg *fileseg = NULL;
    InputSplit tmpsplit;
    while((fileseg = input->get_next_file()) != NULL){
        if(file_count < get_proc_count(proc_rank, totalcount)){
	    fileseg->startrank = proc_rank;
	    fileseg->endrank = proc_rank;
    	    tmpsplit.add_seg_file(fileseg);
            file_count++;
        }
        if(file_count == get_proc_count(proc_rank, totalcount)){
            files.push_back(tmpsplit);
            tmpsplit.clear();
            proc_rank++;
            file_count = 0;
        }
    }

    tmpsplit.clear();
    for (int i = (int)files.size(); i < mimir_world_size; i++)
        files.push_back(tmpsplit);
}

InputSplit *FileSplitter::get_my_split(){
    return &files[mimir_world_rank];
}

uint64_t FileSplitter::get_proc_count(int rank, uint64_t totalcount){
    uint64_t localcount = totalcount / mimir_world_size;
    if(rank < (int)(totalcount % mimir_world_size)) localcount += 1;

    return localcount;
}


