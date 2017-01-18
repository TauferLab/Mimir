#include <stdio.h>
#include <string.h>

#include "config.h"
#include "memory.h"
#include "const.h"
#include "log.h"
#include "inputstream.h"

using namespace MIMIR_NS;


void InputStream::_file_open(const char *filename){
    if(iotype == CLIBIO){
        union_fp.c_fp = fopen(filename, "r");
        if(union_fp.c_fp == NULL)
            LOG_ERROR("Error: open input file %s\n", filename);
    }else if(iotype == MPIIO){
        MPI_File_open(MPI_COMM_SELF, (char*)filename, MPI_MODE_RDONLY,
                      MPI_INFO_NULL, &(union_fp.mpi_fp));
    }
}

void InputStream::_read_at(char *buf, int64_t offset, int64_t size){
    if(iotype == CLIBIO){
        // set the pointer
        fseek(union_fp.c_fp, offset, SEEK_SET);
        size = fread(buf, 1, size, union_fp.c_fp);
    }else if(iotype == MPIIO){
        MPI_File_read_at(union_fp.mpi_fp, offset, buf, (int)size, MPI_BYTE, NULL);
    }
}

void InputStream::_close(){
    if(iotype == CLIBIO){
        fclose(union_fp.c_fp);
    }else if(iotype == MPIIO){
        MPI_File_close(&(union_fp.mpi_fp));
    }
}

// read files to fullfill the input buffer
// files are read by multiple blocks
void InputStream::read_files(){

    // read files
    for(int64_t i = win.right_file_idx; i < win.file_count; i++){

        // range of file i
        int64_t start_off = 0;
        if(i == win.right_file_idx) start_off = win.right_file_off;
        else start_off = splitter->get_start_offset(i);
        int64_t end_off = splitter->get_end_offset(i);

        if(start_off == end_off) {
            win.right_file_idx += 1;
            win.right_file_off = 0;
            continue;
        }

        // open the file
        if(fp != NULL) fclose(fp);
        fp = fopen(splitter->get_file_name(i), "r");
        if(!fp){
            LOG_ERROR("Error: open input file %s\n",
                      splitter->get_file_name(i));
        }

        // read file
        int64_t rsize;
        // read until end of the file
        if(end_off - start_off <= inbufsize - win.right_buf_off)
            rsize = end_off - start_off;
        // read multiple blocks from the file
        else
            rsize = (inbufsize - win.right_buf_off) 
                / win.block_size * win.block_size;
        // set the pointer
        fseek(fp, start_off, SEEK_SET);
        rsize = fread(inbuf+win.right_buf_off, 1, rsize, fp);

        LOG_PRINT(DBG_IO, "Read input file %s:%ld+%ld\n", 
                  splitter->get_file_name(i), start_off, rsize);

        // reset the window
        win.right_file_idx = i;
        win.right_file_off = start_off + rsize;
        win.right_buf_off += rsize;

        if(win.right_file_idx == win.file_count - 1 && \
           win.right_file_off == end_off){
            fclose(fp);
        }

        // the window
        if(win.right_buf_off >= inbufsize || \
           ((inbufsize - win.right_buf_off) < win.block_size && \
            (end_off - win.right_file_off) > win.block_size))
           break;
    }
}

// Please make sure the file count is not zero
bool InputStream::open_stream(){

    if(win.file_count <= 0)
        return false;

    // allocate input buffer
    if (win.total_size <= INPUT_BUF_SIZE)
        inbufsize = win.total_size;
    else
        inbufsize = INPUT_BUF_SIZE;

    inbuf =  (char*)mem_aligned_malloc(MEMPAGE_SIZE, inbufsize);
    tailbuf = (char*)mem_aligned_malloc(MEMPAGE_SIZE, TAIL_BUF_SIZE);

    // set file pointers
    win.left_file_idx = 0;
    win.left_file_off = splitter->get_start_offset(0);
    win.right_file_idx = 0;
    win.right_file_off = win.left_file_off;
    win.left_buf_off = 0;
    win.right_buf_off = 0;
    win.tail_left_off = 0;
    win.tail_right_off = 0;
    win.tail_done = false;

    read_files();

    return true;
}

void InputStream::close_stream(){
    mem_aligned_free(tailbuf);
    mem_aligned_free(inbuf);
}

void InputStream::send_tail(){
    MPI_Status st;
    tailreq = MPI_REQUEST_NULL;

    while(splitcb && !splitcb(this, splitptr)){

        // wait buffer ready
        if(tailreq != MPI_REQUEST_NULL){
            MPI_Wait(&tailreq, &st);
            tailreq = MPI_REQUEST_NULL;
        }
        // as the pointer moved to next, we need get the previous character
        tailbuf[win.tail_right_off] = *(inbuf + win.left_buf_off);
        win.tail_right_off++;

        // fflush the buffer
        if(win.tail_right_off == TAIL_BUF_SIZE){
            MPI_Isend(tailbuf, win.tail_right_off, 
                      MPI_BYTE, me-1, 0xaa, global_comm, &tailreq);
            win.tail_right_off = 0;
        }

        next();
    }

    // the buffer is not empty
    if(win.tail_right_off > 0){
        MPI_Isend(tailbuf, win.tail_right_off, 
                  MPI_BYTE, me-1, 0xaa, global_comm, &tailreq);
        win.tail_right_off = 0;
    }

    // send end flag
    MPI_Request tmp;
    MPI_Isend(NULL, 0, MPI_BYTE, me-1, 0xaa, global_comm, &tmp);
}

bool InputStream::recv_tail(){
    bool done=false;

    MPI_Status st;
    int count;

    MPI_Irecv(tailbuf, TAIL_BUF_SIZE, 
              MPI_BYTE, me+1, 0xaa, global_comm, &tailreq);
    MPI_Wait(&tailreq, &st);

    MPI_Get_count(&st, MPI_BYTE, &count);

    win.tail_left_off = 0;
    win.tail_right_off = count;
    if(count == 0) done = true;

    return done;
}

// if EOF
bool InputStream::is_eof(){

    // at the end of current file
    if(win.left_file_off == splitter->get_end_offset(win.left_file_idx)){

        // the file is shared by right process
        if(splitter->is_right_sharefile(win.left_file_idx)){
            // has tail data
            if(win.tail_left_off < win.tail_right_off) 
                return false;
            // no tail
            else if(win.tail_done) 
                return true;
            // try to get tail
            else{
                win.tail_done = recv_tail();
                return win.tail_done;
            }
        }

        return true;
    }

    return false;
}

// if empty
bool InputStream::is_empty(){
    if(win.left_file_idx >= win.file_count)
        return true;

    if(win.left_file_idx == win.file_count - 1 && is_eof())
        return true;

    return false;
}

unsigned char InputStream::operator*(){
    return get_byte();
}

// get current byte
// make sure the byte is in memory
unsigned char InputStream::get_byte(){
    if(is_eof()) return 0;

    int64_t start_off = splitter->get_start_offset(win.left_file_idx);
    int64_t end_off = splitter->get_end_offset(win.left_file_idx);
    int64_t fidx = win.left_file_idx;
    bool isleft = splitter->is_left_sharefile(fidx);

    if(iscb == false && isleft && 
       win.left_file_off == start_off ){
        iscb = true;
        send_tail();
        iscb = false;
        if(is_eof()) return 0;
    }

    char ch;
    if(win.left_file_off < end_off)
        ch = *(inbuf + win.left_buf_off);
    else
        ch = *(tailbuf + win.tail_left_off);

     return ch;
}

void InputStream::operator++(){
    next();
}

void InputStream::next(){
    int64_t end_offset = splitter->get_end_offset(win.left_file_idx);

    if(iscb && win.left_file_off == end_offset){
        LOG_ERROR("Error: the midlle file segement of %s is \
                  the tail of previous segement, please \
                  resplit your file\n",
                  splitter->get_file_name(win.left_file_idx));
    }

    if(is_empty()) return;

    if(is_eof()){
        win.left_file_idx += 1;
        win.left_file_off = 
            splitter->get_start_offset(win.left_file_idx);
    }else{
        if(win.left_file_off < end_offset){
            win.left_buf_off++;
            win.left_file_off++;
        }else{
            win.tail_left_off++;
        }
    }

    if(win.left_buf_off >= win.right_buf_off)
        read_files();

    //_print_win();

    return;
}

#if 0
int InputStream::get_char(char& c){

    int ret = 0;

    while(win.right_file_idx < win.file_count){

        // end of file
        if(win.left_file_off == 
           splitter->get_end_offset(win.left_file_idx)){

            // receive data from right process
            if(splitter->is_right_sharefile(win.left_file_idx)){

                //printf("%d[%d] left_file_idx=%ld, %d, %d,\n", me, nprocs, 
                //      win.left_file_idx,
                //       splitter->is_right_sharefile(win.left_file_idx),
                //       splitter->is_left_sharefile(win.left_file_idx));

                if(iscb)
                    LOG_ERROR("Error: the midlle file segement of %s is \
                              the tail of previous segement, please \
                              resplit your file\n",
                              splitter->get_file_name(win.left_file_idx));

                do
                if(win.tail_left_off < win.tail_right_off){
                    ch = 
                    win.tail_left_off++;
                    break;
                }
                }while(ret = recv_tail());

                ret = recv_tail(c);
                if(ret) {
                    win.tail_left_off++;
                    break;
                }
            }
            // jump to next file
            if(win.left_file_idx < win.file_count - 1){
                ret = EOF;
                win.left_file_idx += 1;
                win.left_file_off = 
                        splitter->get_start_offset(win.left_file_idx);
            }
            break;
        // has data in-mmeory
        }else if(win.left_buf_off < win.right_buf_off){
            int64_t fidx = win.left_file_idx;
            bool isleft = splitter->is_left_sharefile(fidx);
            int64_t startoff = splitter->get_start_offset(fidx);

            // the file is shared by other processes on the left side
            if(iscb==false && isleft && win.left_file_off == startoff){
                int64_t my_file_idx = win.left_file_idx;
                // skip tail
                iscb = true;
                // get_char may be invoked in send_tail recursely
                send_tail();
                iscb = false;
                if(win.left_file_idx > my_file_idx){
                    ret = EOF;
                }else{
                    ret = 1;
                    c = *(inbuf + win.left_buf_off - 1);
                }
            }else{
                ret = 1;
                c = *(inbuf+win.left_buf_off);
                win.left_buf_off += 1;
                win.left_file_off += 1;
            }
            break;
        }else read_files();
    }

    printf("%d[%d] %ld->%ld, c=%x, ret=%d\n", 
           me, nprocs, win.left_buf_off, win.right_buf_off, c, ret);

    _print_win();

    return ret;
}

#endif

void CollectiveInputStream::_create_comm(){

    MPI_Group global_group, local_groups[GROUP_SIZE];

    int64_t inbuf_blocks = inbufsize / win.block_size ;
    if(inbufsize < 1) inbuf_blocks = 1;
    int64_t max_blocks[GROUP_SIZE] = {0};

    for(int i = 0; i < GROUP_SIZE; i++){
        max_comm_count[i] = 0;
        cur_comm_count[i] = 0;
    }

    MPI_Comm_group(global_comm, &global_group);

    for(int i = 0; i < GROUP_SIZE; i++){
        int ranks[nprocs], n = 1;
        int low_rank = me, high_rank = me + 1;

        splitter->get_group_ranks(i, low_rank, high_rank);
        n = high_rank - low_rank + 1;

        if(n > 1){
            max_blocks[i] = splitter->get_group_maxblocks(i);
            printf("%d[%d] i=%d, max_blocks=%ld, inbuf_blocks=%ld\n", 
                   me, nprocs, i, max_blocks[i], inbuf_blocks);
            if(inbufsize > splitter->get_total_size())
                max_comm_count[i] = (int)ROUNDUP(max_blocks[i], inbuf_blocks);
            else
                max_comm_count[i] = 1;
        }

        for(int j = 0; j < n; j++) {
            ranks[j] = low_rank + j;
        }

        MPI_Group_incl(global_group, n, ranks, &local_groups[i]);
        MPI_Comm_create(global_comm, local_groups[i], &local_comms[i]);
        MPI_Group_free(&local_groups[i]);

    }

    MPI_Group_free(&global_group);
}

void CollectiveInputStream::_destroy_comm(){
    for(int i = 0; i < GROUP_SIZE; i++)
        MPI_Comm_free(&local_comms[i]);
}

bool CollectiveInputStream::_read_group_files(){

    MPI_Status st;

    for(int groupid = 0; groupid < GROUP_SIZE; groupid++){
        // skip finished group
        while(cur_comm_count[groupid] < max_comm_count[groupid]){
            // get file index
            int64_t fidx = splitter->get_group_fileid(groupid);

            // open the group file
            if(mpi_fp == MPI_FILE_NULL){
                LOG_PRINT(DBG_IO, "Open input file %s\n", 
                          splitter->get_file_name(fidx));

                MPI_File_open(local_comms[groupid], 
                          (char*)splitter->get_file_name(fidx), 
                          MPI_MODE_RDONLY,
                          MPI_INFO_NULL, &mpi_fp);
            }

            // compute offset
            int64_t start_off = 0;
            if(fidx == win.right_file_idx) start_off = win.right_file_off;
            else start_off = splitter->get_start_offset(fidx);
            int64_t end_off = splitter->get_end_offset(fidx);

            // get read size
            int64_t rsize;
            if(end_off - start_off <= inbufsize - win.right_buf_off)
                rsize = end_off - start_off;
            else
                rsize = (inbufsize - win.right_buf_off) 
                    / win.block_size * win.block_size;

            LOG_PRINT(DBG_IO, "Read input file %s:%ld+%ld\n", 
                  splitter->get_file_name(fidx), start_off, rsize);
            MPI_File_read_at_all(mpi_fp, start_off, inbuf + win.right_buf_off,
                                 (int)rsize, MPI_BYTE, &st);

            // reset the window
            win.right_file_idx = fidx;
            win.right_file_off = start_off + rsize;
            win.right_buf_off += rsize;

            cur_comm_count[groupid]++;

            // finish read
            if(win.right_file_off == end_off){
                while(cur_comm_count[groupid] < max_comm_count[groupid]){
                    MPI_File_read_at_all(mpi_fp, 0, NULL, 0, MPI_BYTE, &st);
                    cur_comm_count[groupid]++;
                }
                MPI_File_close(&mpi_fp);
                LOG_PRINT(DBG_IO, "Close input file %s\n", 
                          splitter->get_file_name(fidx));
                mpi_fp = MPI_FILE_NULL;
            }

            return true;
        }
    }

    return false;
}

void CollectiveInputStream::read_files(){

    if(_read_group_files()) return;
    // read files
    for(int64_t i = win.right_file_idx; i < win.file_count; i++){

         // range of file i
        int64_t start_off = 0;
        if(i == win.right_file_idx) start_off = win.right_file_off;
        else start_off = splitter->get_start_offset(i);
        int64_t end_off = splitter->get_end_offset(i);

        if(start_off == end_off) {
            win.right_file_idx += 1;
            win.right_file_off = 0;
            continue;
        }

        // open the file
        if(mpi_fp != MPI_FILE_NULL) MPI_File_close(&mpi_fp);
        MPI_File_open(MPI_COMM_SELF,
                      (char*)(splitter->get_file_name(i)), 
                      MPI_MODE_RDONLY,
                      MPI_INFO_NULL,
                      &mpi_fp);

        // read file
        int64_t rsize;
        // read until end of the file
        if(end_off - start_off <= inbufsize - win.right_buf_off)
            rsize = end_off - start_off;
        // read multiple blocks from the file
        else
            rsize = (inbufsize - win.right_buf_off) 
                / win.block_size * win.block_size;
        // set the pointer
        //fseek(fp, start_off, SEEK_SET);
        //rsize = fread(inbuf, 1, rsize, fp);
        MPI_Status st;
        MPI_File_read_at(mpi_fp, start_off, inbuf + win.right_buf_off, 
                         (int)rsize, MPI_BYTE, &st);

        LOG_PRINT(DBG_IO, "Read input file %s:%ld+%ld\n", 
                  splitter->get_file_name(i), start_off, rsize);

        // reset the window
        win.right_file_idx = i;
        win.right_file_off = start_off + rsize;
        win.right_buf_off += rsize;

        if(win.right_file_idx == win.file_count - 1 && \
           win.right_file_off == end_off){
            MPI_File_close(&mpi_fp);
        }

        // the window
        if(win.right_buf_off >= inbufsize || \
           ((inbufsize - win.right_buf_off) < win.block_size && \
            (end_off - win.right_file_off) > win.block_size))
           break;
    }
}
