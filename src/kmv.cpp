#include "kmv.h"
#include <iostream>
#include <mpi.h>
#include <omp.h>
#include <vector>
#include <string>
#include <cstdio>
#include <string.h>
#include "hash.h"
#include <fstream>
#include <cstdlib>
#include <utility>
#include <cmath>//pow
#include <tuple>
#include <ctime>
#include <iomanip>


#ifdef FUSION
	#define DICTM_SIZE (32*1024*1024) //merge
	#define MVM_SIZE (512*1024*1024)
	#define MVC_SIZE (512*1024*1024)
	#define REDUCE_KVB_SIZE (32*1024*1024)//reduce kv buffer size
	#define N_THREADS 12
#else
	#define DICTM_SIZE (32*1024*1024) //merge
	#define MVM_SIZE (256*1024*1024)//per thread
	#define MVC_SIZE (32*1024*1024)//per thread
	#define REDUCE_KVB_SIZE (32*1024*1024)//per thread
	#define N_THREADS 4
#endif


#define INT_MAX 0x7FFFFFFF
#define MIN(A,B) ((A) < (B)) ? (A) : (B)
#define MAX(A,B) ((A) > (B)) ? (A) : (B)



using namespace MAPREDUCE_NS;


KMV::KMV(MapReduce *caller){
	spill_num_mv_after_convert = 0;
	spill_num_mv_after_merge = 0;
	spill_num_dict = 0;
	//num_f_after_merge = 0;
	//num_f_reduce = 0;
	mr = caller;
	me = caller->me;
	nprocs = caller->nprocs;
	num_key_total=0;
	comm = caller->comm;

	caller->kmv = this;
	mv_buffer_size = caller->kv_buffer_size;

	/*dict_convert_offset = 0;
	mv_convert_offset = 0; 
	dict_merge_offset = 0; 
	mv_merge_offset = 0;

	num_key_dict_c = 0;
	num_key_dict_m = 0;*/

	reduce_num=caller->reduce_num;

	//logf_reduce_p = mr->log_base + "/log_reduce_p" + std::to_string(me);
	//logf_reduce_p_of.open(logf_reduce_p,  std::ofstream::out|std::ofstream::app);

	//print info to log files to debug
	//logf_reduce_p_of<<"DICTM_SIZE: "<<DICTM_SIZE<<std::endl << "MVM_SIZE: "
	//	<<MVM_SIZE<<std::endl<<"MVC_SIZE: "<<MVC_SIZE<<std::endl
	//	<<"REDUCE_KVB_SIZE: "<<REDUCE_KVB_SIZE<<std::endl;
}

KMV::~KMV(){
	logf_reduce_p_of.close();
}

/*
allocate memory for kv chunk
each thread
	for every kv chunk
		bring to memr by t0
		for every kv pari in kv chunk
		write dict c and ht c
		for every k in dic c
		write ht m and dict m, mv c
		for every k in dict m
		write mv m, htm
*/


uint64_t KMV::reduce(uint64_t max_kv_file_size, 
	uint32_t (*myreduce) (MapReduce *, const char *, 
			uint32_t , const char *, uint32_t , char *))
{
	//if (DEBUG)
	//{
	//	logf_reduce_p_of<<"Enter convert, max kv file size is: "<<max_kv_file_size<<std::endl;
	//}

	//allocate memory for each kv chunk
	//TODO: what if the size of kv chunk is too large??
	uint32_t num_kv_p=0;
	reduce_num++;

	char *kv_buffer = new char[max_kv_file_size]; //for kv chunk
	uint64_t kv_file_size = 0;//current kv chunk size

	double thread_time, thread_time_s, thread_time_e;

	thread_time_s = omp_get_wtime();
	//for every kv chunk
	//

        //if(me ==0 ) Log::output("begin convert");

	#pragma omp parallel 
	{

	int tid = omp_get_thread_num();
	int nthreads = omp_get_num_threads();

	uint32_t num_kv_t=0;

	//std::string logf_reduce_t = mr->log_base + "/log_reduce_p" + std::to_string(me)+"t"+std::to_string(tid);
	//std::ofstream logf_reduce_t_of;
	//if (DEBUG)
	//{
	//	logf_reduce_t_of.open(logf_reduce_t,std::ofstream::out|std::ofstream::app);
	//	logf_reduce_t_of<<"Enter convert. max kv file size is: "<<max_kv_file_size<<std::endl
	//		<<"number of kv chunk files is: "<<mr->svec_spillf_kv_after_all2all.size()<<std::endl;
	//}

	//double convert_time=0, merge_time=0, reduce_time=0;
	//double convert_s_time=0, convert_e_time=0, merge_s_time=0, merge_e_time=0, reduce_s_time=0, reduce_e_time=0;
	//double io_time=0, omp_sync_time=0;
	//double io_time_s=0, io_time_e=0, omp_sync_time_s=0, omp_sync_time_e=0;
	//convert_s_time = omp_get_wtime();
	//logf_reduce_t_of<<"Convert start time: "<<std::setprecision(15)<<convert_s_time<<std::endl;

	char  *mv_convert, *dict_merge, *mv_merge;//private to thread
	uint32_t  mv_convert_offset = 0, dict_merge_offset = 0, mv_merge_offset = 0;
	uint64_t  num_key_dict_m = 0;
	std::vector<std::list<UniqueC>> ht_convert;
	std::vector<std::list<UniqueM>> ht_merge;

	dict_merge = new char[DICTM_SIZE];
	uint32_t estimate = pow(2,20); //1000000;
	ht_merge.reserve(estimate);
	for (int i = 0; i<estimate;i++)
	{
		ht_merge.emplace_back();
	}
	//ht_merge.resize(estimate);
	hashmask = estimate - 1;

	mv_merge = new char[MVM_SIZE];
	mv_convert = new char[MVC_SIZE];

	size_t kv_offset = 0, tmp_len = 1; //when looking at every kv pair in the kv buffer chunk
	const char *white_space = " ";
	int key_s, key_e, val_s, val_e; //key start, end, value start,end pos
	char *key, *value;
	uint32_t keysize = 0;
	int hash_id = 0;//the hash number to decide which tid process this key
	uint32_t valuesize = 0;
	char *my_buffer = kv_buffer;
	int ibucket;

	//for spill mv_c and mv_m to disk
	bool flag_mv_c_inmem = true;//true: all in mem, false, there is been spill
	bool flag_mv_m_inmem = true;
	int num_spill_mv_c = 0;
	int num_spill_mv_m = 0;
	std::vector<std::string> svec_spillf_mv_c, svec_spillf_mv_m;
	std::string spill_mv_c_file, spill_mv_m_file;
	spill_mv_c_file =  mr->spill_base + "/spill_mv_c.p" + std::to_string(me)+
		"t"+std::to_string(tid)+"."+std::to_string(num_spill_mv_c)+"."+
		std::to_string(reduce_num);
	spill_mv_m_file =  mr->spill_base + "/spill_mv_m.p" + std::to_string(me)+
		"t"+std::to_string(tid)+"."+std::to_string(num_spill_mv_m)+"."+
		std::to_string(reduce_num);

	//(1) for every kv chunk
	for (int i = 0; i< mr->svec_spillf_kv_after_all2all.size(); i++)
	{
		//if (DEBUG)
		//{
			//logf_reduce_t_of<<"1: processing file: "<<mr->svec_spillf_kv_after_all2all.at(i)
		        //		<<std::endl;
		//}
		//thread 0 reads the kv file into memory
		if (tid == 0)
		{
			//io_time_s = omp_get_wtime();
			std::ifstream kv_file (mr->svec_spillf_kv_after_all2all.at(i),std::ios::in|std::ios::binary|std::ios::ate);
			if (kv_file.is_open()){
				kv_file_size = kv_file.tellg();
				if (kv_file_size == 0)
					continue;//skip empty files
				kv_file.seekg(0, std::ios::beg);
				kv_file.read(kv_buffer, kv_file_size);
				//if (DEBUG)
				//{
				//	logf_reduce_t_of<<"The entire kv file: "<< 
				//		mr->svec_spillf_kv_after_all2all.at(i)<<
				//		" is in memory, size: "
				//		<<kv_file_size<<std::endl;
				//}          
				kv_file.close();

			}else{
				std::cout<<"Proc: "<<me <<" unable to kv open file."<<std::endl;
			}
			//io_time_e = omp_get_wtime();
			//io_time += (io_time_e - io_time_s);
			//logf_reduce_t_of<<"IO s: "<<io_time_s<<std::endl
			//	<<"IO e: "<<io_time_e<<std::endl;
		}
		//omp_sync_time_s = omp_get_wtime();
		#pragma omp barrier
		//omp_sync_time_e = omp_get_wtime();
		//omp_sync_time += (omp_sync_time_e - omp_sync_time_s);
		//logf_reduce_t_of<<"Barrier s: "<<omp_sync_time_s<<std::endl
		//	<<"Barrier e: "<<omp_sync_time_e<<std::endl;
		//to ensure the kv chunk is in memory before threads process it
		//kv chunk in memory: kv_buffer

		//make empty dict_convert, ht_convert
		//dict_convert = new char*[DICTC_SIZE];

		//new: remove dict_convert, change it to the the hashtable_convert
		//(1.1) empty hashtable convert
		ht_convert.clear();
		uint32_t estimate2 = pow(2, 18);//2^18 = 256K,300000;
		ht_convert.reserve(estimate2);
		for (int i = 0; i<estimate2;i++)
		{
			ht_convert.emplace_back();
		}


		//(1.2) for every kv pair in the kv chunk

		//logf_reduce_t_of<<"1.2, begin."<<std::endl;

		uint32_t num_key_dict_c = 0;
		uint32_t hash_id;
		int hash_id2;
		//dict_convert_offset=0;
		kv_offset = 0;
		tmp_len = 1;

		//if (DEBUG)
		//{
		//	logf_reduce_t_of<<"I1: enter for every key in kv chunk."<<std::endl;
		//}

		while ((kv_offset < kv_file_size) && (tmp_len !=0))
		{

			tmp_len = strlen(&my_buffer[kv_offset]);
			/*if (DEBUG)
			{
				logf_reduce_t_of<<"Location entering while for key: "
					<<kv_offset<<"Key size: "<<tmp_len<<std::endl;
			}*/
			key_s = kv_offset;//start and end position of actual key, 
			//counting the space after it (1space)
			key_e = key_s + tmp_len ;//counting the null, without +1

			key = &my_buffer[key_s];
			keysize = tmp_len; //not including the space or null, since use
			//this as key in the hashtable
			kv_offset += (tmp_len + 1);//the start location of value

			//read next token, (value)
			tmp_len = strlen(&my_buffer[kv_offset]);
			/*if (DEBUG)
			{
				logf_reduce_t_of<<"Location in while for value: "<<kv_offset
					<<"Value size: "<<tmp_len<<std::endl;
			}*/
			//location of value start and end
			val_s = kv_offset;
			val_e = val_s + tmp_len;
			value = &my_buffer[val_s];

			kv_offset += (tmp_len + 1);//the start location of next value


			hash_id = hashlittle(key, keysize,0);
			//hash_id2 = hash_id % nthreads;//todo &(nthreads-1)
			hash_id2 = hash_id &  (nthreads-1);//todo &(nthreads-1)
			//if ((hash_id2 == tid) &&(keysize!=0))
			if (hash_id2 == tid) 
			{
				//ibucket = hash(key, keysize);
				ibucket = hash_id & (estimate2-1);
				//if (DEBUG)
				//{
				//	logf_reduce_t_of<<"I1: key: "<<key <<" keysize: "<<keysize
				//		<<"htc ibucket: "<<ibucket<<std::endl;
				//}

				std::list<UniqueC>& ul = ht_convert[ibucket];
				char *key_hit = 0;


				for (auto u = ul.begin(); u != ul.end(); ++u){//u is unique

					if (strncmp(u->key,key, keysize) == 0){
						//the key is already in unique list
						key_hit = u->key;
						//update hashtable
						//u.locs.push_back(std::pair<uint32_t, uint32_t> tmp_pair (val_s, val_e));
						u->locs.push_back(std::make_pair(val_s, val_e));
						u->mv_size += (tmp_len+1);
						//if (DEBUG)
						//{
						//	logf_reduce_t_of<<"I1: htc buket hit, push back val_s and val_e "
						//		<<val_s<<"-"<<val_e<<std::endl;
						//}

						break;

					}
				}

				//new key has no hit in search
				if (!key_hit){
					//update hashtable, update dictionary

					ul.emplace_back();
					//UniqueC& new_unique = ul.back();

					ul.back().key = key;//pass ptr is ok, since no change to kv buffer
					ul.back().mv_size = (tmp_len+1);
					//new_uniquet.locs.push_back(std::pair<uint32_t, uint32_t> tmp_pair (val_s, val_e));
					ul.back().locs.push_back(std::make_pair(val_s, val_e));
					//dict_convert[dict_convert_offset] = key;
					//dict_convert_offset++;
					//num_key_dict_c++;
					//if (DEBUG)
					//{
					//	logf_reduce_t_of <<"I1: htc bucket miss, push back val_s and val_e "
					//		<<val_s<<"-"<<val_e<<std::endl;
					//	logf_reduce_t_of<<"I1: number of keys in dictc: "<<num_key_dict_c<<std::endl
							//<<"dict_convert_offset: "<<dict_convert_offset<<std::endl
					//		<<"current key: "<<key<<std::endl ;
					//}

					//ul.push_back(new_unique);

				}

			}//end if hash_id2==tid

			/*if (DEBUG)
			{
				logf_reduce_t_of<<"Location end of while for next key: "<<kv_offset<<std::endl;
			}*/

		}//end while kv_offset < kv file size

		//logf_reduce_t_of<<"1.2, end."<<std::endl;

		//if (DEBUG)
		//{
		//	logf_reduce_t_of<<"I1: exit for every key in kv chunk."<<std::endl;
		//	logf_reduce_t_of<<"I2: enter for every key in dictc."<<std::endl;
		//}

		//(1.3) loops over the hashtable_convert
		size_t tmp_offset = 0;
		int ii = 0;
		uint32_t mv_s = 0, mv_e = 0;//counting the terminating null

		for (auto ul = ht_convert.begin(); ul != ht_convert.end(); ++ul)
		{
			//for every unique list in the hashtable convert


			for (auto u = ul->begin(); u != ul->end(); ++u)
			{
				//for every uniqueC
				if (u->key != NULL)
				{
					//cp to mv_convert
					mv_s = mv_convert_offset;
					mv_e = mv_convert_offset;
					//if (DEBUG){
					//	logf_reduce_t_of<<"13-C, key: "<<u->key<<" size: "<<u->mv_size
					//		<<" mv_convert_offset: "<<mv_convert_offset<<std::endl;
					//}
					

					if ((mv_convert_offset + u->mv_size)<MVC_SIZE){
						//the mv fit in the mv_c, cp directly to memory
						//if (DEBUG)
						//{
						//	logf_reduce_t_of<<"131, key: "
						//		<<u->key<<" mv_convert offset: "
						//		<<mv_convert_offset<<std::endl;
						//}
						for (auto l = u->locs.begin(); l != u->locs.end(); ++l)
						{
							valuesize = (l->second - l->first );//not include the null
							memcpy(&mv_convert[mv_convert_offset], &my_buffer[l->first], (valuesize+1));
							mv_convert_offset += (valuesize+1);
							

						}
						mv_e = mv_convert_offset-1;

					}else{
						//mv does not fit in the mv_c, spill the current mv_c to disk, then cp
						//if (DEBUG)
						//{
						//	logf_reduce_t_of<<"132, key: "
						//		<<u->key<<" mv_convert offset: "
						//		<<mv_convert_offset<<std::endl;
						//}

						//io_time_s = omp_get_wtime();
						this->spill(mv_convert, spill_mv_c_file.c_str(), mv_convert_offset);
						//io_time_e = omp_get_wtime();
						//io_time += (io_time_e - io_time_s);
						//logf_reduce_t_of<<"Spill s: "<<io_time_s<<std::endl
						//	<<"Spill e: "<<io_time_e<<std::endl;

						svec_spillf_mv_c.push_back(spill_mv_c_file);
						num_spill_mv_c++;
						spill_mv_c_file = mr->spill_base + "/spill_mv_c.p" + std::to_string(me)+
							"t"+std::to_string(tid)+"."+std::to_string(num_spill_mv_c)+"."+
							std::to_string(reduce_num);
						flag_mv_c_inmem = false;//true=all in mem
						//reset mv_convert_offset
						mv_convert_offset=0;
						mv_s = mv_convert_offset;
						//cp start from the beginning 
						for (auto l = u->locs.begin(); l != u->locs.end(); ++l)
						{
							valuesize = (l->second - l->first );//not include the null
							memcpy(&mv_convert[mv_convert_offset], &my_buffer[l->first], (valuesize+1));
							mv_convert_offset += (valuesize+1);


						}
						
						mv_e = mv_convert_offset-1;

					}//end if mv_convert_offset + u->mv_size)<MVC_SIZE




					//query and update hashtable merge, dict merge
					//if (DEBUG)
					//{
					//	logf_reduce_t_of<<"13-M, mv_convert_offset: "<<mv_convert_offset
					//		<<"mv_merge_offset: "<<mv_merge_offset
					//	<<std::endl;
					//}
					keysize = strlen(u->key);
					ibucket = (hashlittle(u->key, keysize,0)) & (estimate-1);

					std::list<UniqueM>& uml = ht_merge[ibucket];
					//if (DEBUG)
					//{
					//	logf_reduce_t_of<<"I2: key: "<<u->key<<" keysize: "<<keysize
					//		<<"htm ibucket: "<<ibucket<<std::endl
					//		<<"uml size: "<<uml.size()<<std::endl;
					//}
					
					char *key_hit = 0;
					for (auto um = uml.begin(); um != uml.end(); ++um){
						if (strncmp(um->key, u->key, keysize) == 0){
							//the key is already in unique list
							key_hit = um->key;
							um->locs.push_back(std::make_tuple(num_spill_mv_c, mv_s, mv_e));
							um->mv_size += (u->mv_size);
							//if (DEBUG)
							//{
							//	logf_reduce_t_of<<"133, key: "<<u->key
							//		<<" loc: "
							//		<<mv_s<<"-"<<mv_e<<std::endl;
							//}
							break;
						}
					}

					if (!key_hit)
					{
						memcpy(&dict_merge[dict_merge_offset], u->key, (keysize+1));

						uml.emplace_back();
						//UniqueM& new_uniqe = uml.back();
						uml.back().key = &dict_merge[dict_merge_offset];
						uml.back().mv_size = u->mv_size;
						uml.back().locs.push_back(std::make_tuple(num_spill_mv_c,mv_s,mv_e));

						dict_merge_offset += (keysize+1);
						num_key_dict_m++;
						//if (DEBUG)
						//{
						//	logf_reduce_t_of<<"134 key: "<<u->key
						//		<<" loc: "
						//		<<mv_s<<"-"<<mv_e<<" dict_m: "
						//		<<dict_merge_offset<<" #unique key: "<<num_key_dict_m<<std::endl;
						//}
					}

				}//if u->key != NULL
			}//end for auto u in ul 
		}//end for atuo ul in hashtable convert


		//if (DEBUG)
		//{
		//	logf_reduce_t_of<<"I2: after update the dictionary merge and ht merge, "
		//		<<"offset of dictionary merge is: "<<dict_merge_offset
		//		<<"number of keys in dictionary merge is: "<<num_key_dict_m<<std::endl;
		//}


		//delete [] dict_convert;
		//delete [] mv_convert;
		//omp_sync_time_s = omp_get_wtime();
		#pragma omp barrier //do not write kv_buffer, dict_convert before every thread is done with current kv chunk
		                //omp_sync_time_e = omp_get_wtime()
		//;
		//omp_sync_time += (omp_sync_time_e - omp_sync_time_s);
		//logf_reduce_t_of<<"Barrier2 s: "<<omp_sync_time_s<<std::endl
		//	<<"Barrier2 e: "<<omp_sync_time_e<<std::endl;

		//if (tid == 0){
		//	logf_reduce_t_of<<"Remove the svec_spillf_kv_after_all2all file at position: "<<io_time_s	
		//		<<std::endl;
		//	mr->svec_spillf_kv_after_all2all.erase(mr->svec_spillf_kv_after_all2all.begin() + i);
		//}

	
	}//end for every kv chunk

	//convert_e_time = omp_get_wtime();
	//convert_time=convert_e_time -  convert_s_time;
	//logf_reduce_t_of<<"Convert end time: "<<convert_e_time<<std::endl;



	//merge_s_time = omp_get_wtime();

	//logf_reduce_t_of<<"Merge start time: "<<merge_s_time<<std::endl;


	//(2) for every key in dict_merge
	//if (DEBUG)
	//{
	//	logf_reduce_t_of<<"O1: exit for every kv chunk."<<std::endl;
	//	logf_reduce_t_of<<"O2: enter for every key in dictm."<<std::endl;
	//	logf_reduce_t_of<<"dict_merge::::"<<std::endl;
	//	int tmp2=0;
	//	for (int ii = 0;ii <num_key_dict_m;ii++){
	//		int tmp = strlen(&dict_merge[tmp2]);
	//		logf_reduce_t_of<<"["<<ii<<"] = "<<&dict_merge[tmp2]<<std::endl;
	//		tmp2+=tmp+1;
	//	}

	//}

        //if(me == 0 && tid == 0) Log::output("begin merge");	

	int ii = 0;
	dict_merge_offset = 0;
	uint64_t mvsize =0;
	uint32_t mv_s=0, mv_e=0;
	size_t tmp_offset=0;//mv_e is the location of the terminating null

	uint32_t mv_c_start= 0, mv_c_end = 0;//start, end loc for mv of each key
	int mv_c_id=0;

	while (ii<num_key_dict_m)
	{
		//query ht merge
		mv_s = mv_merge_offset;
		mv_e = mv_s;

		tmp_offset = strlen(&dict_merge[dict_merge_offset]);
		key = &dict_merge[dict_merge_offset];
		ibucket = hash(key, tmp_offset);

		std::list<UniqueM>& ul = ht_merge[ibucket];
		char *key_hit = 0;

		//if (DEBUG)
		//{
		//	logf_reduce_t_of<<"O2: processing key: "<<key<<" keysize: "<<tmp_offset
		//		<<"htm ibucket: "<<ibucket<<std::endl;
		//}

		for (auto u = ul.begin(); u != ul.end(); ++u){//uniqueM in ht merge
			if (strncmp(u->key,key, tmp_offset) == 0){
				//the key is already in unique list

				key_hit = u->key;
				//cp mvs from mv_c to mv_m and update hashtable
				if ((mv_merge_offset+u->mv_size)<MVM_SIZE){//fit in the mv_m memory
					//cp from mv_c to mv_m in memory
					for (auto l = u->locs.begin(); l !=u->locs.end(); ++l) //l is a pair of uint32_t
					{
						//mvsize = (l->second - l->first );
						mv_c_start = std::get<1>(*l);
						mv_c_end = std::get<2>(*l);
						mv_c_id = std::get<0>(*l);
						mvsize = (mv_c_end - mv_c_start);//not including terminating null
						if (mv_c_id == num_spill_mv_c){
							//cp from mv_c in memory
							//if (DEBUG)
							//{
							//	logf_reduce_t_of<<"21, key: "<<key <<" mv_c loc: "<<mv_c_start
							//		<<"-"<<mv_c_end<<" mv_m: "<<mv_merge_offset<< std::endl;
							//}
							memcpy(&mv_merge[mv_merge_offset], &mv_convert[mv_c_start], mvsize+1 );
							mv_merge_offset += (mvsize+1);


						}else{
							//cp from spill file of mv_c to mv_m memory
							//if (DEBUG)
							//{
							//	logf_reduce_t_of<<"21 key(file): "<<key<<" mv_c loc:"
							//		<<mv_c_start
							//		<<"-"<<mv_c_end<<" mv_m: "<<mv_merge_offset<< std::endl;
							//}

							//io_time_s = omp_get_wtime();

							std::ifstream is(svec_spillf_mv_c[mv_c_id]);
							is.seekg(mv_c_start);//relative to the begining of the file
							is.read(&mv_merge[mv_merge_offset], mvsize);//+1 or not??
							is.close();
							//io_time_e = omp_get_wtime();
							//io_time += (io_time_e - io_time_s);
							//logf_reduce_t_of<<"Read s: "<<io_time_s<<std::endl
							//	<<"Read e: "<<io_time_e<<std::endl;
							mv_merge_offset += (mvsize+1);

						}
						
					}
					mv_e = mv_merge_offset-1;//including terminating null

					u->locs.clear();
					//u->locs.push_back(std::make_pair(mv_s, mv_e));
					//u->mv_size += (mvsize); //not update mv_size, does not chage
					u->locs.push_back(std::make_tuple(num_spill_mv_m, (uint32_t)mv_s, (uint32_t)mv_e));
					//if (DEBUG)
					//{
					//	logf_reduce_t_of<<"O2: htm buket hit. copy mv from mvc to mvm, mvm locations are "
					//		<<mv_s<<"-"<<mv_e<<std::endl
					//	<<"mv size for key: "<<key<<" is: "<<(mv_e - mv_s+1)<<std::endl;
					//}
					

				}else{//not fit in the mv_m memory
					//spill mv_m, reset, cp, handle the case of new mv >>> mvm_size here
					if (false){
						//the new mv itself >>> mvm_size
					}else{
						//the new mv itself < mvm_size
						//io_time_s = omp_get_wtime();
						this->spill(mv_merge, spill_mv_m_file.c_str(), mv_merge_offset);
						//io_time_e = omp_get_wtime();
						//io_time += (io_time_e - io_time_s);
						//logf_reduce_t_of<<"Spill s: "<<io_time_s<<std::endl
						//	<<"Spill e: "<<io_time_e<<std::endl;

						svec_spillf_mv_m.push_back(spill_mv_m_file);
						num_spill_mv_m++;
						spill_mv_m_file = mr->spill_base + "/spill_mv_m.p" + std::to_string(me)+
							"t"+std::to_string(tid)+"."+std::to_string(num_spill_mv_m)+"+"+
							std::to_string(reduce_num);
						flag_mv_m_inmem = false;//true=all in mem
						//reset mv_merge_offset
						mv_merge_offset=0;
						//cp mvs from mv_c to mv_m start form offset=0
						//TODO: bzhang when one mvs is larger than the entire memory buffer
						mv_s = mv_merge_offset;

						for (auto l = u->locs.begin(); l !=u->locs.end(); ++l) //l is a pair of uint32_t
						{
							//mvsize = (l->second - l->first );
							mv_c_start = std::get<1>(*l);
							mv_c_end = std::get<2>(*l);
							mv_c_id = std::get<0>(*l);
							mvsize = (mv_c_end - mv_c_start);
							if (mv_c_id == num_spill_mv_c){
								//cp from mv_c in memory
								//if (DEBUG)
								//{
								//	logf_reduce_t_of<<"22, key:"<<key
								//		<<" mv_c loc: "<<mv_c_start
								//		<<"-"<<mv_c_end<<" mv_m: "<<mv_merge_offset<< std::endl;
								//}
								memcpy(&mv_merge[mv_merge_offset], &mv_convert[mv_c_start], mvsize+1 );
								mv_merge_offset += (mvsize+1);

							}else{
								//cp from spill file of mv_c to mv_m memory
								//if (DEBUG)
								//{
								//	logf_reduce_t_of<<"22, key (file):"<<key
								//		<<" mv_c loc: "<<mv_c_start
								//		<<"-"<<mv_c_end<<" mv_m: "<<mv_merge_offset<< std::endl;
								//}

								//io_time_s = omp_get_wtime();
								std::ifstream is(svec_spillf_mv_c[mv_c_id]);
								is.seekg(mv_c_start);//relative to the begining of the file
								is.read(&mv_merge[mv_merge_offset], mvsize);//+1 or not??
								is.close();
								//io_time_e = omp_get_wtime();
								//io_time += (io_time_e - io_time_s);
								//logf_reduce_t_of<<"Read s: "<<io_time_s<<std::endl
								//	<<"Read e: "<<io_time_e<<std::endl;
								mv_merge_offset += (mvsize+1);

							}
							
						}
						mv_e = mv_merge_offset-1;//including terminating null

						u->locs.clear();
						//u->locs.push_back(std::make_pair(mv_s, mv_e));
						//u->mv_size += (mvsize);
						u->locs.push_back(std::make_tuple(num_spill_mv_m,(uint32_t) mv_s,(uint32_t) mv_e));
						//if (DEBUG)
						//{
						//	logf_reduce_t_of<<"O2: htm buket hit. copy mv from mvc to mvm, mvm locations are "
						//		<<mv_s<<"-"<<mv_e<<std::endl
						//	<<"mv size for key: "<<key<<" is: "<<(mv_e - mv_s+1)<<std::endl;
						//}
					}//end of new mv itself >>> mvm_size
				}//end if mv_merge_offset+mv_size < MVM_SIZE

				break;
			}//end if strcmp == 0
		}

		//if ((!key_hit)&&(DEBUG))
		//{
		//	logf_reduce_t_of<<"O2: Error when query htm, key "
		//		<<key<<" not hit."
		//		<<"key offset in the dictionary merge is: "<< dict_merge_offset<<std::endl;

		//}


		dict_merge_offset += (tmp_offset+1);
		ii++;
	}//end while ii<num_key_dict_m

	//delete [] dict_merge;
	//delete [] mv_merge;
	//(3) spill dict_merge, mv_merge, mv_convert, for debug
	//io_time_s = omp_get_wtime();

	//std::string tmp_file;
	//tmp_file = mr->spill_base+"spill_dict_merge." + std::to_string(me) +
	//	"."+std::to_string(tid)+"."+std::to_string(reduce_num);
	//this->spill(dict_merge, tmp_file.c_str(), dict_merge_offset);

	//this->spill(mv_merge, spill_mv_m_file.c_str(), mv_merge_offset);
	//svec_spillf_mv_m.push_back(spill_mv_m_file);

	//this->spill(mv_convert, spill_mv_c_file.c_str(), mv_convert_offset);
	//svec_spillf_mv_c.push_back(spill_mv_c_file);	
	//io_time_e = omp_get_wtime();
	//io_time += (io_time_e - io_time_s);
	//logf_reduce_t_of<<"Spill s: "<<io_time_s<<std::endl
	//	<<"Spill e: "<<io_time_e<<std::endl;

	//if (DEBUG)
	//{
	//	logf_reduce_t_of<<"Exit convert."<<std::endl;
	//	logf_reduce_t_of.close();
	//}

	//merge_e_time = omp_get_wtime();
	//logf_reduce_t_of<<"Merge end time: "<<merge_e_time<<std::endl;
	//merge_time=merge_e_time - merge_s_time;

	//BOYU: now all the kv buffer/chunks are processed
	//into key mv ready for reduce
	//this is the reduce part:::::::::::::::::REDUCE:::::::::::::::::::::


	//if (DEBUG)
	//{
	//	logf_reduce_t_of.open(logf_reduce_t,std::ofstream::out|std::ofstream::app);
	//	logf_reduce_t_of<<"Enter reduce. dict merge size: "<<dict_merge_offset
	//		<<"mv merge size: "<<mv_merge_offset<<"ht merge size: "<<ht_merge.size()<<std::endl;
	//}

	//reduce_s_time = omp_get_wtime();
	//logf_reduce_t_of<<"Redue start time: "<<reduce_s_time<<std::endl;
        
        //if(me == 0 && tid == 0) Log::output("begin reduce");

	//for every key in the dict merge, get the mv from the mv merge, apply reduce
	ii = 0;
	mv_s = 0;
	mv_e = 0;
	dict_merge_offset=0;
	char *reduce_kv_buffer =  new char[REDUCE_KVB_SIZE];
	uint32_t reduce_kv_buffer_offset = 0;
	uint32_t reduce_kv_size = 0;

	int mv_m_id=0;

	//TODO: what if the reduce_kv_buffer is not large enough?
	while (ii<num_key_dict_m)
	{
		//query ht merge

		tmp_offset = strlen(&dict_merge[dict_merge_offset]);
		key = &dict_merge[dict_merge_offset];
		ibucket = hash(key, tmp_offset);

		std::list<UniqueM>& ul = ht_merge[ibucket];
		char *key_hit = 0;

		//if (DEBUG)
		//{
		//	logf_reduce_t_of<<"R: processing key: "<<key<<" keysize: "<<tmp_offset
		//		<<"htm ibucket: "<<ibucket<<std::endl;
		//}

		for (auto u = ul.begin(); u != ul.end(); ++u){
			if (strncmp(u->key,key, tmp_offset) == 0){
				//the key is already in unique list
				key_hit = u->key;
				mv_m_id = std::get<0>(u->locs.back());
				mv_s = std::get<1>(u->locs.back());
				mv_e = std::get<2>(u->locs.back());
				mvsize = u->mv_size;

				//logf_reduce_t_of<<"Key: "<<key<<" hit. mv_m_id: "<<mv_m_id	
				//	<<" mv_s: "<<mv_s<<" mv_e: "<<mv_e<<" mvsize: "<<mvsize
				//	<<" reduce kv buffer offset: "<<reduce_kv_buffer_offset <<std::endl;

				//get the mv start and end, apply reduce function to mv
				//TODO: if mv not in memory, need to bring from file
				if (mv_m_id == num_spill_mv_m){

					//mvsize = mv_e - mv_s;//not inclding the NULL

					//call myreduce, tmp_offset = keysize
					//myreduce copy key, mv to the reduce_kv_buffer
					reduce_kv_size = myreduce(this->mr, key, tmp_offset, &mv_merge[mv_s], 
							mvsize, &reduce_kv_buffer[reduce_kv_buffer_offset]);
					if (reduce_kv_size != 0)
						num_kv_t++;

					//if (DEBUG)
					//{

					//	logf_reduce_t_of<<"R: mv in mem. after apply myreduce."<<std::endl
					//		<<"input key: "<<key <<" size: "<<tmp_offset 
					//		<<" input value offset: "<<mv_s<<"-"<<mv_e
					//		<<" input value size: "<<mvsize
					//		<<" output kv to buffer start: "<<reduce_kv_buffer_offset
					//		<<" output value is: "<< &reduce_kv_buffer[reduce_kv_buffer_offset+tmp_offset+1]
					//		<<" output kv size: "<<reduce_kv_size<<std::endl;
					//}
					reduce_kv_buffer_offset += (reduce_kv_size);
				}else{
					//bring the mvs from file to buffer, then apply reduce
					char *tmp_mv = new char[mvsize];

					std::ifstream is(svec_spillf_mv_m[mv_m_id]);
					is.seekg(mv_s);//relative to the begining of the file
					is.read(tmp_mv, mvsize);//+1 or not??
					is.close();
					//delete [] tmp_mv;
					reduce_kv_size = myreduce(this->mr, key, tmp_offset, tmp_mv, 
							mvsize, &reduce_kv_buffer[reduce_kv_buffer_offset]);
					if (reduce_kv_size != 0)
						num_kv_t++;

					delete [] tmp_mv;

					//if (DEBUG)
					//{

					//	logf_reduce_t_of<<"R: mv read from file. after apply myreduce."<<std::endl
					//		<<"input key: "<<key <<" size: "<<tmp_offset 
					//		<<" input value offset: "<<mv_s<<"-"<<mv_e
					//		<<" input value size: "<<mvsize
					//		<<" output kv to buffer start: "<<reduce_kv_buffer_offset
					//		<<" output value is: "<< &reduce_kv_buffer[reduce_kv_buffer_offset+tmp_offset+1]
					//		<<" output kv size: "<<reduce_kv_size<<std::endl;
					//}
					reduce_kv_buffer_offset += (reduce_kv_size);
				}

				break;

			}
		}

		//if ((!key_hit)&&(DEBUG))
		//{
		//	logf_reduce_t_of<<"R: Error when query htm, key "
		//		<<key<<" not hit."
		//		<<"key offset in the dictionary merge is: "<< dict_merge_offset<<std::endl;

		//}


		dict_merge_offset += (tmp_offset+1);
		ii++;


	}//end while ii<num_key_dict_m


	//spill reduce kv buffer 
	//std::string reduce_file;
	//reduce_file = mr->spill_base+"spill_reduce_kv_buffer."+std::to_string(me) + "."+std::to_string(tid)+"."+std::to_string(reduce_num);
	//this->spill(reduce_kv_buffer, reduce_file.c_str(), reduce_kv_buffer_offset);
	//add to mr->svec_reduce_out_kv_files so map can read

	//svec_reduce_out_kv_files_mutex.lock();
	//mr->svec_reduce_out_kv_files.push_back(reduce_file);
	//svec_reduce_out_kv_files_mutex.unlock();


	delete [] reduce_kv_buffer;


	//omp_sync_time_s = omp_get_wtime();
	#pragma omp barrier
	//omp_sync_time_e = omp_get_wtime();
	//omp_sync_time += (omp_sync_time_e - omp_sync_time_s);
	//logf_reduce_t_of<<"Barrier s: "<<omp_sync_time_s<<std::endl
		//<<"Barrier e: "<<omp_sync_time_e<<std::endl;

	//reduce_e_time = omp_get_wtime();
	//logf_reduce_t_of<<"Redue end time: "<<reduce_e_time<<std::endl;	
	//reduce_time = reduce_e_time - reduce_s_time;

	//logf_reduce_t_of<<"IO time: "<<io_time<<std::endl
	//	<<"OMP sync time: "<<omp_sync_time<<std::endl;
	//logf_reduce_t_of<<"Convert time: "<<convert_time<<" seconds."<<std::endl;
	//logf_reduce_t_of<<"Merge time: "<<merge_time<< " seconds." <<std::endl;
	//logf_reduce_t_of<<"Redue time: "<<reduce_time<<" seconds."<<std::endl;

	#pragma omp atomic
	num_kv_p += num_kv_t;

	//if (DEBUG)
	//{
	//	logf_reduce_t_of<<"Exit reduce."<<std::endl;
	//	logf_reduce_t_of.close();
	//}


	}//end pragma omp parallel
	int num_kv_all = 0;
	MPI_Allreduce(&num_kv_p, &num_kv_all, 1, MPI_INT, MPI_SUM, comm);
	//if (DEBUG)
	//{
	//	logf_reduce_p_of<<"Total number of kv pairs output by reduce is: "<<num_kv_all<<std::endl;
	//}

	//thread_time_e = omp_get_wtime();
	//thread_time = thread_time_e - thread_time_s;
	//logf_reduce_p_of<<"Thread start: "<<thread_time_s<<std::endl
	//	<<" Thread end: "<<thread_time_e<<std::endl
	//	<<" Thread time: "<<thread_time<<std::endl;

	delete [] kv_buffer;

	//if (DEBUG)
	//{
	//	logf_reduce_p_of<<"Exiting reduce, the set of kv files after reduce are:"<<std::endl;
	//	for (auto it = mr->svec_reduce_out_kv_files.begin(); it != mr->svec_reduce_out_kv_files.end(); ++it)
	//	{
	//		logf_reduce_p_of << *it <<std::endl;
	//	}

	//}

	//if(me == 0) Log::output("end reduce");	


	return num_kv_all;
}





void KMV::spill(char *page, const char *fname, uint64_t size)
{
	/*spill the kv page onto a disk file*/
	std::ofstream spill_file;
	spill_file.open(fname, std::ofstream::app);
	spill_file.write(page, size);
	spill_file.close();


}

int KMV::hash(char *key, int keybytes)
{
	uint32_t ubucket = hashlittle(key,keybytes,0);
	int ibucket = ubucket & hashmask;
	return ibucket;
}

