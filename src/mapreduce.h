#ifndef MAP_REDUCE_H
#define MAP_REDUCE_H

#include <stdint.h>
#include <omp.h>
#include <string>
#include <vector>
#include <opa_primitives.h>
#include <mutex>
#include <fstream>
#include <mpi.h>

#include "hash.h"

//#include "log.h"
#include "dataobject.h"
#include "keyvalue.h"
#include "keymultivalue.h"

namespace MAPREDUCE_NS {

class MapReduce {
public:
    MapReduce(MPI_Comm);
    uint64_t map(int, char **, int, int, int, 
      void (*mymap) (MapReduce *, char *, int), 
      int myhash(char *, int), void *);

    uint64_t reduce(void (myreduce)(MapReduce *, char *, int, char *, int, 
       int *, void*), void* );

    uint64_t convert();

    uint64_t add(char *key, int keybytes, char *value, int valuebytes);

private:
    DataObject *data;


/*******************************************************************/

friend class KMV;

// Public variable
public:
    int msize; //# of Mbytes per memory page
    std::string log_base, spill_base, out_base; // base dir for files
    std::ofstream  logf_map_p_of, logf_reduce_p_of; // log file
        
    // time
    double mpi_itime;
    double mpi_wait_s, mpi_wait_e, mpi_iall_s, mpi_iall_e;

    class KMV *kmv;

    //Log log;

// Public function
public:
    MapReduce(MPI_Comm, char *, char *, char *);
    ~MapReduce();

        /*
            Map always reads input from disk file
            Combiner always reads input from kv chunk files, either output
                by map or by reduce
        */

        /*
        int: intput local or lustre
        char *: input path
        int: map read mode, word (1) or line (2)
        int: map type
            4 types of maps
            1 - save kv chunk to file before comm + no combiner
            2 - save kv chunk to file before comm + with combiner 
            3 - no save kv chunk to file before comm + no combiner
            4 - no save kv chunk to file before comm + with combiner
        char *: log path
        char *: spill path
        char *: out path
        */
    /*map_local: only map, no communication*/
    /*
        3 types of combiners
        1 - read kv chunk file from map before comm (requires map to save)
        2 - read kv chunk file from map after comm
        3 - read kv chunk file from reduce after reduce
    */


    // Map and Reduce function
    uint64_t map(char *, int, int,
        void (*mymap) (MapReduce *, char *, char *, int *, char *, int *, int));

    uint64_t map_local(int, char *, int, int, char *, char *, char *,
        void (*mymap) (MapReduce *, char *, char *, int *, char *, int *, int));

    uint64_t reduce(int, uint32_t (*myreduce) (MapReduce *));
    uint64_t reduce(int , uint32_t (*myreduce) (MapReduce *, const char *key, uint32_t keysize, const char *mv, uint32_t mvsize, char *out_kv));
  

    // KV function
    uint64_t kv_add(char *, char *);
    unsigned long int set_kv_buffer_size(unsigned long int );
    unsigned long int set_in_buffer_size(unsigned long int);

private:
    // MPI Commincator
    MPI_Comm comm;
    int me,nprocs; 

    int map_num;//the 1st or 2nd or 3rd... time that one calls map, use for spill
    //file ids
    int reduce_num;

    // #Bytes to hold input file in buffer for work sharing among threads
    unsigned long int in_buffer_size; 
    // #Bytes to hold kv pairs in buffer for double buffering 
    unsigned long int kv_buffer_size;
    // log file, one log file per proc, and one log file per thread
    std::string logf_map_p, logf_reduce_p;
    // output files, for debugging: kv buffer before exchange (after exchange)
    std::vector<std::string> svec_outf_kvbuffer_be;
    std::vector<std::string> svec_outf_kvbuffer_ae;
    int num_kvbuffer_be, num_kvbuffer_ae; //out files need to have diff names

    std::vector<std::string> svec_map_in_files;//input files for this proc
    std::vector<int> ivec_map_in_files;//size of input fils, only useful
    //when reading from kv chunks of map or reduce, not useful when
    //reading from disk input files directly

    //output kv chunk files by reduce, each thread has one or more
    //set by threads in kmv--reduce function
    std::vector<std::string> svec_reduce_out_kv_files;

    //file: kv after alltoallv
    struct FileMeta 
    {
        std::string name;
        int id;
        uint64_t keysize_all;
        uint64_t nkey_all;
        uint64_t kvsize_all;
    };

    //std::vector<FileMeta> fvec_spillf_kv_after_all2all;

    std::vector<std::string> svec_spillf_kv_after_all2all;
    std::vector<int> ivec_spillf_kv_after_all2all;//size of spilled kv fiels after exchange, before merge
    /*spill files for map without communication*/
    std::vector<std::string> svec_spillf_kv_after_map;
    std::vector<int> ivec_spillf_kv_after_map;//size

    std::vector<std::string> svec_spillf_kv_after_map_local;//multithreads assessing
    std::vector<int> ivec_spillf_kv_after_map_local;
    std::mutex svec_spillf_kv_after_map_local_mutex;
    std::mutex ivec_spillf_kv_after_map_local_mutex;


    char *input_buffer, *kv_buffer1, *kv_buffer2, *kv_buffer, *recvbuf1, *recvbuf2;
    int recvbuf_size1, recvbuf_size2;
    OPA_int_t *offset1, *offset2, *offset;
    MPI_Request *map_exchange_request;
    MPI_Status *map_exchange_status;
    char **ele_ptr;
    int debug_num_input_buffer;//when print the output files, use this as part of the file name
    int debug_num_kv_buffer;
    uint32_t map_num_exchange; //number of map exchage of kv buffers that has been issued
    int spill_num_kv_after_all2all;
    int spill_num_kv_after_map;//for map without communication, spill kv files
       

    void findfiles(char *);
    void add_files_from_map();
    void add_files_from_reduce();


    uint64_t read_to_input_buffer(int , int *, long int *, uint64_t *, uint64_t *);
    uint64_t read_word_to_input_buffer(int , int *, long int *, uint64_t *, uint64_t *);
    uint64_t read_line_to_input_buffer(int , int *, long int *, uint64_t *, uint64_t *);
    uint64_t read_to_input_buffer_from_map_kvfiles(int , int *, long int *, uint64_t *, uint64_t *);
    uint64_t write_output_files(char *, char *, uint64_t );
    uint64_t exchange_kv(int *, int *, int *, int *, int *, int *, int *, int *);
    void wait_last_ialltoallv(uint32_t );
    void wait_previous_ialltoallv_switch_buffer(uint32_t , bool *);
    void spill(char *, char *, uint64_t );
};//class MapReduce

}//namespace

#endif
