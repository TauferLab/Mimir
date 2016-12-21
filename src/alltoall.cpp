#include <stdio.h>
#include <stdlib.h>
#include "log.h"
#include "config.h"
#include "alltoall.h"
#include "const.h"
#include "memory.h"
#include "keyvalue.h"

using namespace MIMIR_NS;

#include "hash.h"
#include "stat.h"
#include "log.h"

Alltoall::Alltoall(MPI_Comm _comm):Communicator(_comm, 0){
    switchflag=0;
    ibuf=0;
    buf=NULL;
    off=NULL;

    recv_count=NULL;
    recv_buf=NULL;
    recvcounts=NULL;
    type_log_bytes=0;

    reqs=NULL;

    LOG_PRINT(DBG_COMM, "Comm: alltoall create.\n");
}

Alltoall::~Alltoall(){
    for(int i = 0; i < nbuf; i++){
      if(recv_buf != NULL && recv_buf[i] != NULL) mem_aligned_free(recv_buf[i]);
      if(recv_count !=NULL && recv_count[i] != NULL) mem_aligned_free(recv_count[i]);
    }

    if(recv_count != NULL) delete [] recv_count;
    if(recv_buf != NULL) delete [] recv_buf;

    if(recvcounts != NULL) delete [] recvcounts;
    if(reqs != NULL) delete [] reqs;

    LOG_PRINT(DBG_COMM, "Comm: alltoall destroy.\n");
}

int Alltoall::setup(int64_t _sbufsize, KeyValue *_data, \
    MapReduce *_mr, UserCombiner _combiner, UserHash _hash){
    Communicator::setup(_sbufsize, _data, _mr, _combiner, _hash);

    int64_t total_send_buf_size=(int64_t)send_buf_size*size;

    type_log_bytes=0;
    int type_bytes=0x1;
    while((int64_t)type_bytes*(int64_t)MAX_COMM_SIZE<total_send_buf_size){
        type_bytes<<=1;
        type_log_bytes++;
    }

    recv_buf = new char*[nbuf];
    recv_count  = new int*[nbuf];

    for(int i = 0; i < nbuf; i++){
      recv_buf[i] = (char*)mem_aligned_malloc(MEMPAGE_SIZE, total_send_buf_size);
      recv_count[i] = (int*)mem_aligned_malloc(MEMPAGE_SIZE, size*sizeof(int));
    }

    reqs = new MPI_Request[nbuf];
    for(int i=0; i<nbuf; i++)
      reqs[i]=MPI_REQUEST_NULL;

    recvcounts = new int64_t[nbuf];
    for(int i = 0; i < nbuf; i++) recvcounts[i] = 0;

    switchflag=0;
    ibuf = 0;
    buf = send_buffers[0];
    off = send_offsets[0];

    for(int i=0; i<size; i++) off[i] = 0;

    LOG_PRINT(DBG_COMM, "Comm: alltoall setup. (\
comm buffer size=%ld, type_log_bytes=%d)\n", send_buf_size, type_log_bytes);

    return 0;
}

int Alltoall::sendKV(const char *key, int keysize, const char *val, int valsize){
    // compute target process
    target = 0;
    if(myhash != NULL){
        target=myhash(key, keysize)%size;
    }else{
        uint32_t hid = 0;
        hid = hashlittle(key, keysize, 0);
        target = (int)(hid%(uint32_t)size);
    }

    if(target < 0 || target >= size){
        LOG_ERROR("Error: target process (%d) isn't correct!\n", target);
    }

    // get size of the KV
    int kvsize = 0;
    GET_KV_SIZE(kv->ksize,kv->vsize, keysize, valsize, kvsize);
 
    // check if the KV can be fit in one buffer
    if(kvsize>send_buf_size)
        LOG_ERROR("Error: KV size (%d) is larger than send_buf_size (%ld)\n", kvsize, send_buf_size);

    // flag to check if inserted or not
    int inserted=0;
    while(1){
        // get current buffer offset
        int goff=off[target];

        // without combiner
        if(mycombiner==NULL){
            // the KV can be inserted into the buffer
            if((int64_t)goff+(int64_t)kvsize<=send_buf_size){
                // insert the KV
                int64_t global_buf_off=target*(int64_t)send_buf_size+goff;
                char *gbuf=buf+global_buf_off;
                PUT_KV_VARS(kv->ksize,kv->vsize,gbuf,key,keysize,val,valsize,kvsize);
                off[target]+=kvsize;
                inserted=1;
            }
        // with combiner
        }else{
            // check the key
            u = bucket->findElem(key, keysize);
            if(u==NULL){
                CombinerUnique tmp;
                tmp.next=NULL;

                // find a hole to store the KV
                std::unordered_map<char*,int>::iterator iter;
                for(iter=slices.begin(); iter!=slices.end(); iter++){

                    char *sbuf=iter->first;
                    int  ssize=iter->second;

                    // the hole is big enough to store the KV
                    if(ssize >= kvsize){

                        tmp.kv = sbuf+(ssize-kvsize);
                        PUT_KV_VARS(kv->ksize, kv->vsize, tmp.kv, \
                            key, keysize, val, valsize, kvsize);

                        if(iter->second == kvsize)
                            slices.erase(iter);
                        else
                            slices[iter->first]-=kvsize;

                        bucket->insertElem(&tmp);
                        inserted=1;

                        break;
                    }
                }
                // Add the KV at the tail
                if(iter==slices.end()){
                    if((int64_t)goff+(int64_t)kvsize<=send_buf_size){

                        int64_t global_buf_off=target*(int64_t)send_buf_size+goff;
                        tmp.kv=buf+global_buf_off;

                        PUT_KV_VARS(kv->ksize, kv->vsize, tmp.kv,\
                            key, keysize, val, valsize, kvsize);
                        off[target]+=kvsize;

                        bucket->insertElem(&tmp);
                        inserted=1;
                    }
                }
            }else{

                GET_KV_VARS(kv->ksize,kv->vsize,u->kv,\
                    ukey,ukeysize,uvalue,uvaluesize,ukvsize);

                // invoke KV information
                mycombiner(mr,key,keysize,\
                    uvalue,uvaluesize,val,valsize, mr->myptr);

                inserted=1;
            }
        }
        if(inserted) break;
        if(mycombiner!=NULL) {
            gc();
            bucket->clear();
        }
        exchange_kv();
    }

    return 0;
}

int Alltoall::updateKV(const char *newkey, int newkeysize, const char *newval, int newvalsize){
    int inserted=0;

    // check if the key is same 
    if(newkeysize!=ukeysize || \
        memcmp(newkey, ukey, ukeysize)!=0)
        LOG_ERROR("Error: the result key of combiner is different!\n");

    int kvsize;
    // get key size
    GET_KV_SIZE(kv->ksize,kv->vsize, newkeysize, newvalsize, kvsize);

    while(1){
        int goff=off[target];

        // new KV is smaller than previous KV */
        if(kvsize<=ukvsize){
            PUT_KV_VARS(kv->ksize, kv->vsize, u->kv, \
                 ukey, ukeysize, newval, newvalsize, kvsize);
            inserted=1;
            /* record slice */
            if(kvsize < ukvsize)
                slices.insert(std::make_pair(u->kv+ukvsize-kvsize,ukvsize-kvsize));
        }else{
            if((int64_t)goff+(int64_t)kvsize<=send_buf_size){
                slices.insert(std::make_pair(u->kv, ukvsize));
                int64_t global_buf_off=target*(int64_t)send_buf_size+goff;
                char *gbuf=buf+global_buf_off;
                PUT_KV_VARS(kv->ksize,kv->vsize,gbuf,\
                    ukey,ukeysize,newval,newvalsize,kvsize);
                off[target]+=kvsize;
                inserted=1;
            }
        }
        if(inserted) break;
        gc();
        bucket->clear();
        exchange_kv();
    }

    return 0;
}

// wait all procsses done
void Alltoall::wait(){
    //LOG_PRINT(DBG_COMM, "start wait.\n");

    medone = 1;

    // do exchange kv until all processes done
    do{
        if(mycombiner!=NULL) gc();
        exchange_kv();
    }while(pdone < size);

#ifdef MIMIR_COMM_NONBLOCKING
   // wait all pending communication
   for(int i = 0; i < nbuf; i++){
       if(reqs[i] != MPI_REQUEST_NULL){

           //TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);

           MPI_Status mpi_st;
           MPI_Wait(&reqs[i], &mpi_st);
           reqs[i] = MPI_REQUEST_NULL;

           //TRACKER_RECORD_EVENT(0, EVENT_COMM_WAIT);

           uint64_t recvcount = recvcounts[i];

           //PROFILER_RECORD_COUNT(0, COUNTER_COMM_RECV_SIZE, recvcount);

           //LOG_PRINT(DBG_COMM, "Comm: receive data. (count=%ld)\n", recvcount);

           if(recvcount > 0) {
               save_data(i);
           }
       }
   }
#endif

   LOG_PRINT(DBG_COMM, "Comm: finish wait.\n");
}

void Alltoall::save_data(int ibuf){
    mr->phase=CombinePhase;
    char *src_buf=recv_buf[ibuf];
    int k=0;
    for(k=0; k<size; k++){
        int count=0;
        while(count<recv_count[ibuf][k]){
            char *key=0, *value=0;
            int  keybytes=0, valuebytes=0, kvsize=0;
            GET_KV_VARS(kv->ksize,kv->vsize,src_buf,key,keybytes,value,valuebytes,kvsize);
            src_buf+=kvsize;
            kv->addKV(key,keybytes,value,valuebytes);
            count+=kvsize;
        }
        int padding=recv_count[ibuf][k]&((0x1<<type_log_bytes)-0x1);
        src_buf+=padding;
    }
    mr->phase=MapPhase;
}


void Alltoall::gc(){
    if(mycombiner!=NULL && slices.empty()==false){

        LOG_PRINT(DBG_MEM, "Alltoall: garbege collection (size=%ld)\n", slices.size());
 
        int dst_off=0, src_off=0;
        char *dst_buf=NULL, *src_buf=NULL;

        for(int k=0; k<size; k++){
            dst_off = src_off = 0;
            int64_t global_buf_off=k*(int64_t)send_buf_size;
            while(src_off < off[k]){

                src_buf = buf+global_buf_off+src_off;
                dst_buf = buf+global_buf_off+dst_off;

                std::unordered_map<char*,int>::iterator iter=slices.find(src_buf);
                // Skip the hole
                if(iter != slices.end()){
                    src_off+=iter->second;
                }else{
                    char *key=NULL, *value=NULL;
                    int  keybytes=0, valuebytes=0, kvsize=0;
                    GET_KV_VARS(kv->ksize,kv->vsize,src_buf,key,keybytes,value,valuebytes,kvsize);
                    src_buf+=kvsize;
                    if(src_off!=dst_off) memcpy(dst_buf, src_buf-kvsize, kvsize);
                    dst_off+=kvsize;         
                    src_off+=kvsize; 
                }
            }
            off[k] = dst_off;
        }
        slices.clear();
    } 
}

void Alltoall::exchange_kv(){
    int i;
    int64_t sendcount=0;
    for(i=0; i<size; i++) sendcount += (int64_t)off[i];

    PROFILER_RECORD_COUNT(COUNTER_SEND_BYTES, (uint64_t)sendcount, OPSUM);

    TRACKER_RECORD_EVENT(EVENT_COMPUTE_MAP);

    // exchange send and recv counts
    PROFILER_RECORD_TIME_START;
    MPI_Alltoall(off, 1, MPI_INT, recv_count[ibuf], 1, MPI_INT, comm);
    PROFILER_RECORD_TIME_END(TIMER_COMM_A2A);

    TRACKER_RECORD_EVENT(EVENT_COMM_ALLTOALL);

    recvcounts[ibuf] = (int64_t)recv_count[ibuf][0];
    for(i = 1; i < size; i++){
        recvcounts[ibuf] += (int64_t)recv_count[ibuf][i];
    }

    int *a2a_s_count=new int[size];
    int *a2a_s_displs=new int[size];
    int *a2a_r_count=new int[size];
    int *a2a_r_displs= new int[size];

    for(i=0; i<size; i++){
        a2a_s_count[i]=(off[i]+(0x1<<type_log_bytes)-1)>>type_log_bytes;
        a2a_r_count[i]=(recv_count[ibuf][i]+(0x1<<type_log_bytes)-1)>>type_log_bytes;
        a2a_s_displs[i] = (i*(int)send_buf_size)>>type_log_bytes;
    }
    a2a_r_displs[0] = 0;
    for(i=1; i<size; i++)
        a2a_r_displs[i]=a2a_r_displs[i-1]+a2a_r_count[i-1];

    int64_t send_padding_bytes=a2a_s_count[0];
    int64_t recv_padding_bytes=a2a_r_count[0];
    for(i=1;i<size;i++){
      send_padding_bytes+=a2a_s_count[i];
      recv_padding_bytes+=a2a_r_count[i];
    }
    send_padding_bytes<<=type_log_bytes;
    recv_padding_bytes<<=type_log_bytes;
    send_padding_bytes-=sendcount;
    recv_padding_bytes-=recvcounts[ibuf];

    //PROFILER_RECORD_COUNT(0, COUNTER_COMM_SEND_PAD, send_padding_bytes);
    //PROFILER_RECORD_COUNT(0, COUNTER_COMM_RECV_PAD, recv_padding_bytes);

    MPI_Datatype comm_type;
    MPI_Type_contiguous((0x1<<type_log_bytes), MPI_BYTE, &comm_type);
    MPI_Type_commit(&comm_type);

#ifndef MIMIR_COMM_NONBLOCKING
    char *recvbuf=recv_buf[ibuf];

    PROFILER_RECORD_TIME_START;    
    MPI_Alltoallv(send_buffers[ibuf], \
      a2a_s_count, a2a_s_displs, comm_type, \
      recvbuf, a2a_r_count, a2a_r_displs, comm_type, comm);
    PROFILER_RECORD_TIME_END(TIMER_COMM_A2AV);

    TRACKER_RECORD_EVENT(EVENT_COMM_ALLTOALLV);
    
    int64_t recvcount = recvcounts[ibuf];
    //PROFILER_RECORD_COUNT(0, COUNTER_COMM_RECV_SIZE, recvcount);

    LOG_PRINT(DBG_COMM, "Comm: receive data. (count=%ld)\n", recvcount);

    PROFILER_RECORD_COUNT(COUNTER_RECV_BYTES, (uint64_t)recvcount, OPSUM);
    if(recvcount > 0) {
        save_data(ibuf);
    }

    //TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);

#else
    char *recvbuf=recv_buf[ibuf];
    MPI_Ialltoallv(send_buffers[ibuf], a2a_s_count, a2a_s_displs, comm_type, \
        recvbuf, a2a_r_count, a2a_r_displs, comm_type, comm,  &reqs[ibuf]);

    //TRACKER_RECORD_EVENT(0, EVENT_COMM_IALLTOALLV);

    // wait data
    ibuf = (ibuf+1)%nbuf;
    if(reqs[ibuf] != MPI_REQUEST_NULL) {

        MPI_Status mpi_st;
        MPI_Wait(&reqs[ibuf], &mpi_st);
        reqs[ibuf] = MPI_REQUEST_NULL;

        int64_t recvcount = recvcounts[ibuf];

        //TRACKER_RECORD_EVENT(0, EVENT_COMM_WAIT);
        //PROFILER_RECORD_COUNT(0, COUNTER_COMM_RECV_SIZE, recvcount);

        LOG_PRINT(DBG_COMM, "Comm: receive data. (count=%ld)\n", recvcount);

        if(recvcount > 0) {
            save_data(ibuf);
            //SAVE_ALL_DATA(ibuf);
        }

        //TRACKER_RECORD_EVENT(0, EVENT_MAP_COMPUTING);
    }

    // switch buffer
    buf = send_buffers[ibuf];
    off = send_offsets[ibuf];
#endif

    for(int i = 0; i < size; i++) off[i] = 0;
    MPI_Type_free(&comm_type);

    delete [] a2a_s_count;
    delete [] a2a_s_displs;
    delete [] a2a_r_count;
    delete [] a2a_r_displs;

    PROFILER_RECORD_TIME_START;
    MPI_Allreduce(&medone, &pdone, 1, MPI_INT, MPI_SUM, comm);
    PROFILER_RECORD_TIME_END(TIMER_COMM_RDC);

    TRACKER_RECORD_EVENT(EVENT_COMM_ALLREDUCE);

    LOG_PRINT(DBG_COMM, "Comm: exchange KV. (send count=%ld, done count=%d)\n", sendcount, pdone);
}

