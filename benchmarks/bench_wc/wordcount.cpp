#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "mapreduce.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <cstdlib>
#include "string.h"



using namespace MAPREDUCE_NS;

void mymap( MapReduce *, char *, char *, int *, char *, int *);
uint32_t myreduce(MapReduce *, const char *, uint32_t, const char *, uint32_t, char * );


int me, nprocs;
//char *log_base;

int main(int argc, char *argv[])
{
	int provided;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	if (provided < MPI_THREAD_FUNNELED)
		MPI_Abort(MPI_COMM_WORLD, 1);
	MPI_Comm_rank(MPI_COMM_WORLD, &me);
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

	char *in_path = argv[1];
	char *log_base = argv[2];
	char *spill_base = argv[3];
	char *out_base = argv[4];

	char *in_file = new char[256];
	sprintf(in_file, "%s/%d", in_path, me);

	printf("P%d: in main, in path %s.\n", me, in_file);

	MapReduce *mr = new MapReduce(MPI_COMM_WORLD, log_base, spill_base, out_base);
	mr->set_in_buffer_size(32*1024*1024);
	mr->set_kv_buffer_size(16*1024*1024);

	//uint64_t nwords = mr->map(1, in_file, mymap);
	/** map args: [1] 0--read from local; 1--read from gpfs
			[2] path to read input files TODO: what happens when read from global
			[3] 0--pass back each line to the mymap funciton; 1--pass back each word
			[4] mymap function 
	 **/
	printf("P%d, before map.\n",me);

	/*
	0:
	in_file
	1:
	2: map_type=1, map with communication at the end*/
	uint64_t nwords = mr->map(0, in_file, 1,2, mymap);

	/*
		2nd map, map type =2, read input from another map
	*/
	//nwords = mr->map(0, in_file, 1,2, mymap);

	printf("P%d, after map.\n",me);

	printf("P%d, before shuffle.\n", me);

	uint64_t nkmv = mr->reduce(1, myreduce);
	printf("P%d, after shuffle.\n", me);


	//uint64_t nresl = mr->reduce(0, myreduce);


	delete [] in_file;
	delete mr;
	MPI_Finalize();

}

void mymap( MapReduce *mr, char *word, char *key, int *keysize, char *value, int *vsize)
{
	*keysize = strlen(word);
	memcpy(key,word,(*keysize)+1);
	char *myval ="1";
	*vsize = 1;
	memcpy(value, myval, sizeof(myval)+1);    
	//printf("P%d, input word is: %s, output key is %s, value is %s.\n", me, word, key, value);

}

uint32_t myreduce(MapReduce *mr, const char *key, uint32_t keysize, const char *mv,  
	uint32_t mvsize, char *out_kv)
	//char *mv, int mvsize)
{

	uint32_t kvsize = 0;

	uint64_t sum = 0;
	//char *value = mv;
	uint32_t mv_offset = 0;
	size_t vsize = 0;//size of value in mv

	while (mv_offset < mvsize)
	{
		sum += atoi(mv);
		vsize = strlen(mv);
		mv += (vsize + 1);
		mv_offset += (vsize+1);
	}	
	//printf("P%d, sum is %d.\n", me,sum);

	char *tmp_out = out_kv;
	memcpy(tmp_out, key, keysize);
	tmp_out += keysize;
	*tmp_out = '\0';
	tmp_out++;

	char tmp_v[1024];
	sprintf(tmp_v, "%d", sum);
	size_t vlen = strlen(tmp_v);
	//memcpy(tmp_out, &sum, sizeof(sum));
	memcpy(tmp_out, tmp_v, (vlen+1));
	//tmp_out += (vlen+1));
	//*tmp_out = '\0';

	kvsize += (keysize + vlen +2);
	return kvsize;


}















