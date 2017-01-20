#include <stdio.h>
#include <string.h>

#include "config.h"
#include "memory.h"
#include "const.h"
#include "log.h"
#include "inputstream.h"

using namespace MIMIR_NS;

IStream *IStream::createStream(IOMethod iotype,
                               int64_t blocksize,
                               const char *filepath,
                               int shared,
                               int recurse,
                               MPI_Comm comm,
                               UserSplit splitcb,
                               void *splitptr){

    IStream *in = NULL;
    if(iotype == CLIBIO || iotype == MPIIO){
        in = new IStream(iotype, blocksize, filepath, shared,
                         recurse, comm, splitcb, splitptr);
    }else if(iotype == CMPIIO){
        in = new CIStream(blocksize, filepath, shared,
                          recurse, comm, splitcb, splitptr);
    }
    return in;
}

void IStream::destroyStream(IStream *in){
    if(in != NULL){
        delete in;
    }
}

IStream::IStream(IOMethod iotype, int64_t blocksize, 
                 const char *filepath, int sharedflag, 
                 int recurse, MPI_Comm comm,
                 UserSplit splitcb, void * splitptr) {

    splitter = new FileSplitter(blocksize, 
                                filepath, 
                                sharedflag, 
                                recurse, 
                                comm);
    splitter->split();

    win.file_count = splitter->get_file_count();
    win.total_size = splitter->get_total_size();
    win.block_size = blocksize;

    this->iotype = iotype;
    if(iotype == CLIBIO){
        union_fp.c_fp = NULL;
    }else if(iotype == MPIIO){
        union_fp.mpi_fp = MPI_FILE_NULL;
    }

    //fp = NULL;
    inbuf = NULL;
    inbufsize = 0;

    this->splitcb = splitcb;
    this->splitptr = splitptr;

    global_comm = comm;

    MPI_Comm_rank(global_comm, &me);
    MPI_Comm_size(global_comm, &nprocs);

    iscb = false;

}

IStream::~IStream(){

    delete splitter;
}

// Compute the maximum size which can read to the input buffer
int64_t IStream::_get_max_rsize(){

    if(win.right_buf_off < win.left_buf_off || 
       win.right_buf_off > win.left_buf_off + inbufsize){
        LOG_ERROR("Error window state: %ld->%ld\n", 
                  win.left_buf_off, win.right_buf_off);
    }

    if(win.right_buf_off == win.left_buf_off + inbufsize)
        return 0;

    int64_t right_pointer = win.right_buf_off % inbufsize;
    int64_t left_pointer = win.left_buf_off % inbufsize;
    if(right_pointer >= left_pointer)
        return inbufsize - right_pointer;

    return left_pointer - right_pointer;
}

bool IStream::_file_open(const char *filename){
    if(iotype == CLIBIO){
        union_fp.c_fp = fopen(filename, "r");
        if(union_fp.c_fp == NULL) return false;
    }else if(iotype == MPIIO){
        MPI_File_open(MPI_COMM_SELF, (char*)filename, MPI_MODE_RDONLY,
                      MPI_INFO_NULL, &(union_fp.mpi_fp));
        if(union_fp.mpi_fp == MPI_FILE_NULL) return false;
    }

    return true;
}

void IStream::_file_read_at(char *buf, int64_t offset, int64_t size){
    if(iotype == CLIBIO){
        fseek(union_fp.c_fp, offset, SEEK_SET);
        size = fread(buf, 1, size, union_fp.c_fp);
    }else if(iotype == MPIIO){
        MPI_File_read_at(union_fp.mpi_fp, offset, buf, 
                         (int)size, MPI_BYTE, NULL);
    }
}

void IStream::_file_close(){
    if(iotype == CLIBIO){
        if(union_fp.c_fp != NULL){
            fclose(union_fp.c_fp);
            union_fp.c_fp = NULL;
        }
    }else if(iotype == MPIIO){
        if(union_fp.mpi_fp != MPI_FILE_NULL){
            MPI_File_close(&(union_fp.mpi_fp));
            union_fp.mpi_fp = MPI_FILE_NULL;
        }
    }
}

void IStream::_print_win(std::string prefix){

    printf("%d[%d] %s buffer window: %ld->%ld, \
file window: [%ld %ld]->[%ld %ld], \
filecount=%ld, totalsize=%ld, tail=[%d %d]\n",
        me, nprocs, prefix.c_str(),
        win.left_buf_off, win.right_buf_off, 
        win.left_file_idx, win.left_file_off,
        win.right_file_idx, win.right_file_off,
        win.file_count, win.total_size, 
        win.tail_left_off, win.tail_right_off);
};

// read files to fullfill the input buffer
// files are read by multiple blocks
void IStream::read_files(){

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
        //fclose(fp);
        _file_close();
        if(!_file_open(splitter->get_file_name(i))){
            LOG_ERROR("Error: open input file %s\n",
                      splitter->get_file_name(i));
        }

        // read file
        int64_t rsize;
        // read until end of the file
        if(end_off - start_off <= _get_max_rsize())
            rsize = end_off - start_off;
        // read multiple blocks from the file
        else
            rsize = _get_max_rsize() / win.block_size * win.block_size;

        _file_read_at(inbuf + win.right_buf_off % inbufsize,
                      start_off, rsize);
        LOG_PRINT(DBG_IO, "Read input file %s:%ld+%ld\n", 
                  splitter->get_file_name(i), start_off, rsize);

        // reset the window
        win.right_file_idx = i;
        win.right_file_off = start_off + rsize;
        win.right_buf_off += rsize;

        if(win.right_file_idx == win.file_count - 1 && \
           win.right_file_off == end_off){
            _file_close();
        }

        // the window
        if(_get_max_rsize() == 0 || 
           (_get_max_rsize() < win.block_size && \
            (end_off - win.right_file_off) > win.block_size))
            break;
    }
}

// Please make sure the file count is not zero
bool IStream::open_stream(){

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

void IStream::close_stream(){
    mem_aligned_free(tailbuf);
    mem_aligned_free(inbuf);
}

void IStream::send_tail(){
    MPI_Status st;
    tailreq = MPI_REQUEST_NULL;

    while(splitcb && !splitcb(this, splitptr)){

        // wait buffer ready
        if(tailreq != MPI_REQUEST_NULL){
            MPI_Wait(&tailreq, &st);
            tailreq = MPI_REQUEST_NULL;
        }
        tailbuf[win.tail_right_off] = *(inbuf + win.left_buf_off % inbufsize);
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

bool IStream::recv_tail(){
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
bool IStream::is_eof(){

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
bool IStream::is_empty(){
    if(win.left_file_idx >= win.file_count)
        return true;

    if(win.left_file_idx == win.file_count - 1 && is_eof())
        return true;

    return false;
}

unsigned char IStream::operator*(){
    return get_byte();
}

// get current byte
// make sure the byte is in memory
unsigned char IStream::get_byte(){
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
        ch = *(inbuf + win.left_buf_off % inbufsize);
    else
        ch = *(tailbuf + win.tail_left_off);

     return ch;
}

void IStream::operator++(){
    next();
}

void IStream::next(){
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

CIStream::CIStream(
         int64_t blocksize,
         const char* filepath,
         int sharedflag,
         int recurse,
         MPI_Comm comm,
         UserSplit splitcb,
         void *splitptr) :
IStream(CMPIIO, blocksize, filepath, 
        sharedflag, recurse, 
        comm, splitcb, splitptr){

    mpi_fp = MPI_FILE_NULL;

    splitter->print();

    iotype = CMPIIO;

    _create_comm();
}

CIStream::~CIStream(){
    _destroy_comm();
}

bool CIStream::open_stream(){
    return IStream::open_stream();
}

void CIStream::close_stream(){
    IStream::close_stream();
}


void CIStream::_create_comm(){

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

void CIStream::_destroy_comm(){
    for(int i = 0; i < GROUP_SIZE; i++)
        MPI_Comm_free(&local_comms[i]);
}

bool CIStream::_read_group_files(){

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
            if(end_off - start_off <= _get_max_rsize())
                rsize = end_off - start_off;
            else
                rsize = _get_max_rsize() / win.block_size * win.block_size;

            LOG_PRINT(DBG_IO, "Read input file %s:%ld+%ld\n", 
                  splitter->get_file_name(fidx), start_off, rsize);
            MPI_File_read_at_all(mpi_fp, start_off, 
                                 inbuf + win.right_buf_off % inbufsize,
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

void CIStream::read_files(){

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
        if(end_off - start_off <= _get_max_rsize())
            rsize = end_off - start_off;
        // read multiple blocks from the file
        else
            rsize = _get_max_rsize() / win.block_size * win.block_size;
        // set the pointer
        //fseek(fp, start_off, SEEK_SET);
        //rsize = fread(inbuf, 1, rsize, fp);
        MPI_Status st;
        MPI_File_read_at(mpi_fp, start_off, 
                         inbuf + win.right_buf_off % inbufsize, 
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
        if((_get_max_rsize()==0) || \
           (_get_max_rsize() < win.block_size && \
            (end_off - win.right_file_off) > win.block_size))
           break;
    }
}
