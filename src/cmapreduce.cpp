#include "cmapreduce.h"
#include "mapreduce.h"

using namespace MIMIR_NS;

void* create_mr_object(MPI_Comm _comm){
    MapReduce *mr = new MapReduce(_comm);
    return (void*)mr;
}

void destroy_mr_object(void *_mr){
    MapReduce *mr=(MapReduce*)_mr;
    delete mr;
}

uint64_t init_key_value(void* _mr, CUserInitKV _myinit, void *_ptr, int _comm){
    MapReduce *mr=(MapReduce*)_mr;
    UserInitKV myinit=(UserInitKV)_myinit;
    return mr->init_key_value(myinit, _ptr, _comm);
}

uint64_t map_text_file(void *_mr, char *_filename, int _shared, int _recurse,
      const char *_seperator, CUserMapFile _mymap, void *_ptr, int _comm){
    MapReduce *mr=(MapReduce*)_mr;
    UserMapFile mymap=(UserMapFile)_mymap;
    return mr->map_text_file(_filename, _shared, _recurse, _seperator, mymap, _ptr, _comm);
}

uint64_t map_key_value(void *_mr, void *_imr, CUserMapKV _mymap,  void *_ptr, int _comm){
    MapReduce *mr=(MapReduce*)_mr;
    MapReduce *imr=(MapReduce*)_imr;
    UserMapKV mymap=(UserMapKV)_mymap;
    return mr->map_key_value(imr, mymap, _ptr, _comm);
}

uint64_t reduce(void *_mr, CUserReduce _myreduce, void* ptr){
    MapReduce *mr=(MapReduce*)_mr;
    UserReduce myreduce=(UserReduce)_myreduce;
    return mr->reduce(myreduce, ptr);
}

void scan(void *_mr, CUserScan _myscan, void * _ptr){
    MapReduce *mr=(MapReduce*)_mr;
    UserScan myscan=(UserScan)_myscan;
    mr->scan(myscan, _ptr);  
}

uint64_t bcast(void *_mr, int _rank){
    MapReduce *mr=(MapReduce*)_mr;
    return mr->bcast(_rank);
}

uint64_t collect(void *_mr, int _rank){
    MapReduce *mr=(MapReduce*)_mr;
    return mr->collect(_rank);
}

void add_key_value(void *_mr, const char *_key, int _keybytes, const char *_value, int _valuebytes){
    MapReduce *mr=(MapReduce*)_mr;
    mr->add_key_value(_key, _keybytes, _value, _valuebytes);
}

void update_key_value(void *_mr, const char *_key, int _keybytes, const char *_value, int _valuebytes){
    MapReduce *mr=(MapReduce*)_mr;
    mr->update_key_value(_key, _keybytes, _value, _valuebytes);
}

void set_combiner(void *_mr, CUserCombiner _combiner){
    MapReduce *mr=(MapReduce*)_mr;
    UserCombiner combiner = (UserCombiner)_combiner;
    mr->set_combiner(combiner);
}

void set_hash(void *_mr, CUserHash _myhash){
    MapReduce *mr=(MapReduce*)_mr;
    UserHash myhash=(UserHash)_myhash;
    mr->set_hash(myhash);
}

void set_key_length(void *_mr, int _ksize){
    MapReduce *mr=(MapReduce*)_mr;
    mr->set_key_length(_ksize);
}

void set_value_length(void *_mr, int _vsize){
    MapReduce *mr=(MapReduce*)_mr;
    mr->set_value_length(_vsize);
}

const void *get_first_value(void *_mr){
    MapReduce *mr=(MapReduce*)_mr;
    return mr->get_first_value();
}

const void *get_next_value(void *_mr){
    MapReduce *mr=(MapReduce*)_mr;
    return mr->get_next_value();
}



