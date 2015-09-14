#ifndef KMV_H
#define KMV_H

#include "mpi.h"
#include "stdint.h"
#include <omp.h>
#include <string>
#include <vector>
#include <list>
#include <unordered_map>
#include "mapreduce.h"
#include <tuple>
#include <mutex> //updating the svec_reduce_out_kv_files in mr->svec_reduce_out_kv_files



namespace MAPREDUCE_NS {

class KMV{
	friend class MapReduce;

public:
	int spill_num_mv_after_convert, spill_num_mv_after_merge, spill_num_dict;
	//int num_f_after_merge;//each thread store its own file
	//int num_f_reduce;//per thread
	std::string  logf_reduce_p;
	std::ofstream  logf_reduce_p_of;

	class MapReduce *mr;

	KMV(MapReduce *);
	~KMV();

	//convert buffer of kv pairs to kmv, in which keys are unique
	//void convert(uint64_t);
	uint64_t reduce(uint64_t i, uint32_t (*myreduce) (MapReduce *, const char *, 
			uint32_t , const char *, uint32_t , char *));
	//merge all kmv files to kmv files, new files have overall unique keys
	//void merge();
	uint64_t  reduce(int , uint32_t (*myreduce) (MapReduce *, const char *, char *, int));


private:
	int me, nprocs;
	MPI_Comm comm;
	int num_key_total;//number of unique keys in my kmv in this proc
	int mv_buffer_size;

	int reduce_num;//the 1st, 3nd, 3rd time you call reduce, use for spill file ids

	std::vector<std::string> svec_spillf_after_convert;
	std::vector<std::string> svec_spillf_after_merge;
	std::vector<std::string> svec_spillf_dict;

	//for new hash
	std::vector<std::string> svec_spillf_dict_merge;
	std::vector<std::string> svec_spillf_mv_merge;
	std::vector<int> ivec_spillf_dict_merge, ivec_spillf_mv_merge; //sizes

	std::vector<int> ivec_spillf_after_convert, ivec_spillf_after_merge, ivec_spillf_dict;
	std::mutex svec_reduce_out_kv_files_mutex;

	//char **dict_convert, *mv_convert, *dict_merge, *mv_merge;//private to thread
	//uint32_t dict_convert_offset, mv_convert_offset, dict_merge_offset, mv_merge_offset;
	//uint64_t num_key_dict_c, num_key_dict_m;

	struct meta_mv {
		std::string fname; //in which kmv file
		int pos_s; //the start position in file
		int pos_e; //the end position in file
	};

	//new hash table
	struct UniqueC //convert
	{
		char *key;
		uint32_t mv_size;//size of mv for this key
		std::list<std::pair<uint32_t, uint32_t>> locs;    	
	}; 
   // struct UniqueM //merge
	//{
   // 	uint32_t offset;
   // 	std::list<meta_mv> locs;//string need to be id
	//};
	struct UniqueM //convert
	{
		char *key;
		uint32_t mv_size;//size of mv for this key in total
		std::list<std::tuple<int, uint32_t, uint32_t>> locs;    	
	}; 
	//unique key hash list
	int hashmask;
	//std::vector<std::list<UniqueC>> ht_convert;
	//std::vector<std::list<UniqueM>> ht_merge;


	//for using uthash
	









	void spill(char *, const char *, uint64_t );
	int hash(char *, int );










};



}
#endif