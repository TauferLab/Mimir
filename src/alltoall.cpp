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

    //LOG_PRINT(DBG_COMM, "%d[%d] Comm: alltoall create.\n", rank, size);
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

    //LOG_PRINT(DBG_COMM, "%d[%d] Comm: alltoall destroy.\n", rank, size);
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

    //LOG_PRINT(DBG_COMM, "%d[%d] Comm: alltoall setup. (\
comm buffer size=%ld, type_log_bytes=%d)\n", \
      rank, size, send_buf_size, type_log_bytes);

    return 0;
}

int Alltoall::sendKV(char *key, int keysize, char *val, int valsize){
    int target = 0;
    if(myhash != NULL){
        target=myhash(key, keysize)%size;
    }else{
        uint32_t hid = 0;
        hid = hashlittle(key, keysize, 0);
        //printf("hid=%u\n", hid);
        target = hid%(uint32_t)size;
    }

    //printf("target=%d, size=%d\n", target, size); fflush(stdout);

    if(target < 0 || target >= size){
        LOG_ERROR("Error: target process (%d) isn't correct!\n", target);
    }

    //printf("send: key=%s, target=%d\n", key, target); fflush(stdout);

    int kvsize = 0;
    int goff=off[target];
    GET_KV_SIZE(kv->ksize,kv->vsize, keysize, valsize, kvsize);

    int inserted=0;
    while(1){
        /* without combiner */
        if(mycombiner==NULL){
            if((int64_t)goff+(int64_t)kvsize<=send_buf_size){
                int64_t global_buf_off=target*(int64_t)send_buf_size+goff;
                char *gbuf=buf+global_buf_off;
                PUT_KV_VARS(kv->ksize,kv->vsize,gbuf,key,keysize,val,valsize,kvsize);
                off[target]+=kvsize;
                inserted=1;
            }
        /* with combiner */
        }else{
            /* find the unique */
            CombinerUnique *u = bucket->findElem(key, keysize);

            printf("send: key=%s, target=%d, u=%p\n", key, target, u); 
            fflush(stdout);

            if(u==NULL){
                CombinerUnique tmp;
                tmp.next=NULL; 

                /* find a hole to save the KV */
                std::unordered_map<char*,int>::iterator iter;
                for(iter=slices.begin(); iter!=slices.end(); iter++){
                    char *sbuf=iter->first;
                    int  ssize=iter->second;
                    if(ssize >= kvsize){
                        tmp.kv = sbuf+(ssize-kvsize);
                        PUT_KV_VARS(kv->ksize, kv->vsize, tmp.kv, \
                            key, keysize, val, valsize, kvsize);
                        iter->second-=kvsize; 
                        inserted=1;
                        break;
                    }
                }
                /* no hole find */
                if(iter==slices.end()){
                    if((int64_t)goff+(int64_t)kvsize<=send_buf_size){
                        int64_t global_buf_off=target*(int64_t)send_buf_size+goff;
                        tmp.kv=buf+global_buf_off;
                        PUT_KV_VARS(kv->ksize,kv->vsize,tmp.kv,\
                            key,keysize,val,valsize,kvsize);
                        off[target]+=kvsize;
                        inserted=1;
                    }
                }
                bucket->insertElem(&tmp);
            }else{
                int ukvsize;
                char *ukey, *uvalue;
                int  ukeybytes, uvaluebytes, kvsize;
                GET_KV_VARS(kv->ksize,kv->vsize,u->kv,\
                    ukey,ukeybytes,uvalue,uvaluebytes,ukvsize);

                printf("val1=%ld,val2=%ld\n", \
                    *(int64_t*)uvalue,*(int64_t*)val);

                // invoke KV information
                mycombiner(mr,key,keysize,\
                    uvalue,uvaluebytes,val,valsize, mr->myptr);

                // check if the key is same 
                if(mr->newkeysize!=keysize || \
                    memcmp(mr->newkey, ukey, keysize)!=0)
                    LOG_ERROR("%s", "Error: the result key of combiner is different!\n");

                // get key size
                GET_KV_SIZE(kv->ksize,kv->vsize, mr->newkeysize, mr->newvalsize, kvsize);

                /* new KV is smaller than previous KV */
                if(kvsize<=ukvsize){
                    PUT_KV_VARS(kv->ksize, kv->vsize, u->kv, \
                        key, keysize, mr->newval, mr->newvalsize, kvsize);
                    inserted=1;
                    /* record slice */
                    if(kvsize < ukvsize)
                        slices.insert(std::make_pair(u->kv,ukvsize-kvsize));
                }else{
                     if((int64_t)goff+(int64_t)kvsize<=send_buf_size){
                        slices.insert(std::make_pair(u->kv, ukvsize));
                        int64_t global_buf_off=target*(int64_t)send_buf_size+goff;
                        char *gbuf=buf+global_buf_off;
                        PUT_KV_VARS(kv->ksize,kv->vsize,gbuf,\
                            key,keysize,val,valsize,kvsize);
                        off[target]+=kvsize;
                        inserted=1;
                    }                   
                }
            }
        }
        if(inserted) break;
        if(mycombiner!=NULL) gc();
        exchange_kv();
    }

    return 0;
}

// wait all procsses done
void Alltoall::wait(){
    //LOG_PRINT(DBG_COMM, "%d[%d] Comm: start wait.\n", rank, size);

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

           //LOG_PRINT(DBG_COMM, "%d[%d] Comm: receive data. (count=%ld)\n", rank, size, recvcount);

           if(recvcount > 0) {
               save_data(i);
           }
       }
   }
#endif

   //LOG_PRINT(DBG_COMM, "%d[%d] Comm: finish wait.\n", rank, size);
}

void Alltoall::save_data(int ibuf){
    char *src_buf=recv_buf[ibuf];
    int k=0;
    for(k=0; k<size; k++){
        int count=0;
        while(count<recv_count[ibuf][k]){
            char *key, *value;
            int  keybytes, valuebytes, kvsize;
            GET_KV_VARS(kv->ksize,kv->vsize,src_buf,key,keybytes,value,valuebytes,kvsize);
            src_buf+=kvsize;
            kv->addKV(key,keybytes,value,valuebytes);
            count+=kvsize;
        }
        int padding=recv_count[ibuf][k]&((0x1<<type_log_bytes)-0x1);
        src_buf+=padding;
    }
}


void Alltoall::gc(){
    //printf("")
    if(mycombiner!=NULL && slices.empty()==false){
        int dst_off=0, src_off=0;
        char *dst_buf=NULL, *src_buf=NULL;

        int k=0;
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
                    char *key, *value;
                    int  keybytes, valuebytes, kvsize;
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

    PROFILER_RECORD_COUNT(COUNTER_SEND_BYTES, sendcount, OPSUM);

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

    LOG_PRINT(DBG_COMM, rank, size, "Comm: receive data. (count=%ld)\n", recvcount);

    PROFILER_RECORD_COUNT(COUNTER_RECV_BYTES, recvcount, OPSUM);
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

        LOG_PRINT(DBG_COMM, rank, size, "Comm: receive data. (count=%ld)\n", recvcount);

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

    LOG_PRINT(DBG_COMM, rank, size, "Comm: exchange KV. (send count=%ld, done count=%d)\n", sendcount, pdone);
}
