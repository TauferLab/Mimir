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
#include <cmath>


using namespace MAPREDUCE_NS;

/*---------------functions definition----------------------------------------------------------*/
void generate_octkey(MapReduce *, char *, char *, int *, char *, int *, int);


double slope(double[], double[], int); //function inside generate_octkey
void explore_level(int, int, MapReduce * ); //explore the int level of the tree
void gen_leveled_octkey(MapReduce *, char *, char *, int *, char *, int *, int );
uint32_t sum(MapReduce *, const char *, uint32_t, const char *, uint32_t, char * );




/* --------------------------------global vars-------------------------------------- */
int me,nprocs; //for mpi process, differenciate diff processes when print debug info
int digits=15; //width of the octkey, 15 default, set by main, used by map
int thresh=50; //number of points within the octant to be considered dense, and to be further subdivide, set by main, used by reduce
bool result=true; //save results to file, true= yes, set by main, used by map and reduce
bool branch=false;//true branch down, false branch up; used by main and sum functions
//char base_dir[] = "/home/bzhang/csd173/bzhang/mrmpi-20Jun11/mrmpi_clustering/scripts/";
//char base_dir[] = "/home/zhang/mrmpi-20Jun11/mrmpi_clustering/";
const char *base_dir;
int level = 8; //level: explore this level of the oct-tree, used by main and gen_leveled_octkey
//bool realdata=false; //use real dataset or synthetic dataset, true means using real, false means using synthetic, set by main, used by map generate octkey

/*-------------------------------------------------------------------------*/

int main(int argc, char **argv)
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
	thresh = atoi(argv[5]);

	char *in_file = new char[256];
	sprintf(in_file, "%s/%d", in_path, me);

	printf("P%d: in main, in path %s.\n", me, in_file);

	//////////
	
	
	/*var initilization*/
	int min_limit, max_limit; //level: explore this level of the oct-tree
	min_limit=0;
	max_limit=digits+1;
	level=floor((max_limit+min_limit)/2);
	
	
	/*map: (1) compute octey for each ligand; (2) based on the level to explore x,  partial count of each ocatant (3) output octant id, and partial counts*/
	MapReduce *mr = new MapReduce(MPI_COMM_WORLD, log_base, spill_base, out_base);
	//unsigned long int size=2147483648L;
	//printf("P%d: size is: %lu.\n", me, size);
	mr->set_in_buffer_size(32*1024*1024);
	mr->set_kv_buffer_size(16*1024*1024);

	printf("P%d, before map_local.\n",me);
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
	char map_local_spillpath[2048];
	sprintf(map_local_spillpath,"%s/%s_%d",spill_base, "spill", me);
	printf("P%d, spill path: %s.\n", me, map_local_spillpath);

	uint64_t nwords = mr->map_local(0, in_file, 2,2, log_base,
		map_local_spillpath, out_base, generate_octkey);
	printf("P%d, after map_local.\n",me);

	/*map (at the end do the communication), so it has to pair with reduce
	map: take first 5 dig of the octkey, emilt octkey[0--4], 1
	reduce: sum all the 1s*/



	while ((min_limit+1) != max_limit){

		printf("P%d, before map, leve %d.\n",me, level );

		uint64_t nkeys = mr->map(0, map_local_spillpath, 1, 2, 
				log_base, spill_base, out_base, gen_leveled_octkey);
		printf("P%d, after map.\n",me);

		printf("P%d, before reduce.\n", me);

		uint64_t nkv = mr->reduce(1, sum);
		printf("P%d, after reduce.\n", me);

		if (nkv >0){
			/*there exsit an octant that is dense enough,
			branch down the tree*/
			printf("P%d, nkv is %d.\n",me, nkv);
			min_limit=level;
			level =  floor((max_limit+min_limit)/2);

		}else{
			/*there is no octant that is dense enough,
			branch up*/
			printf("P%d, nkv is 0...\n", me);
			max_limit=level;
			level =  floor((max_limit+min_limit)/2);
		}
	}

	delete [] in_file;
	delete mr;
	MPI_Finalize();
	
	
}

/*---------------function implementation-----------------------------*/

uint32_t sum(MapReduce *mr, const char *key, uint32_t keysize, const char *mv,  
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
	if (sum >= thresh){
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
	}else{
		return 0;
	}



}


void gen_leveled_octkey(MapReduce *mr, char *word, char *key, int *keysize, char *value, int *vsize, int tid)
{
	/*std::string logf_map_t = mr->log_base + "/log_lg_p" + std::to_string(me)
		+ "t"+std::to_string(tid);
	std::ofstream logf_map_t_of;	
	logf_map_t_of.open(logf_map_t,  std::ofstream::out|std::ofstream::app);


	logf_map_t_of<<"In gen_leveled_octkey, the word passed by lib is: "<<std::endl<<"|"
		<<word<<"|."<<std::endl;*/

	memcpy(key, word, level);
	*keysize=level;
	char *myval ="1";
	*vsize = 1;
	memcpy(value, myval, sizeof(myval)+1); 

	/*logf_map_t_of<<"Exit gen_leveled_octkey."<<std::endl;
	logf_map_t_of.close();*/
}

void generate_octkey(MapReduce *mr, char *word, char *key, int *keysize, char *value, int *vsize, int tid)
{
	/*key code for wordcount, memcpy(to, from, len)*/
	//*keysize = strlen(word);
	//memcpy(key,word,(*keysize)+1);

	std::string logf_map_t = mr->log_base + "/log_lg_p" + std::to_string(me)
		+ "t"+std::to_string(tid);
	std::ofstream logf_map_t_of;	
	logf_map_t_of.open(logf_map_t,  std::ofstream::out|std::ofstream::app);

	logf_map_t_of<<"In generate_octkey, the word passed by lib is: "<<std::endl<<"|"
		<<word<<"|."<<std::endl;


	double tmp=0, range_up=10.0, range_down=-10.0; //the upper and down limit of the range of slopes
	char octkey[digits];
	bool realdata = false;//the last one is the octkey

	double coords[512];//hold x,y,z
	char ligand_id[256];
	const int word_len = strlen(word);
	char word2[word_len+2];
	memcpy(word2, word, word_len);
	word2[word_len]=' ';
	word2[word_len+1]='\0';

	logf_map_t_of<<"The word2 is: "<<std::endl<<"|"<<word2<<"|."<<std::endl;
	logf_map_t_of<<"The length of word is: "<<strlen(word)<<". the length of word2 is: "<<strlen(word2)<<"."<<std::endl;
	logf_map_t_of<<"The const int word_len is: "<<word_len<<std::endl;

	int num_coor=0;
	//char *token = strtok(word2, " ");
	char *saveptr;
	char *token = strtok_r(word2, " ", &saveptr);
	logf_map_t_of<<"Before while, token: |"<<token<<"|."<<std::endl;
	memcpy(ligand_id,token,strlen(token));
	while (token != NULL)
	{

		//test_index += (strlen(token)+1);
		//token = strtok(NULL, " ");
		token = strtok_r(NULL, " ", &saveptr);
		if (token)
		{
			//logf_map_t_of<<"In while, token: |"<<token<<"|."<<std::endl;
			coords[num_coor] = atof(token);
			num_coor++;

		}
	}
	logf_map_t_of<<"After while, num_coor: "<<num_coor<<std::endl;
	/* actual x,y,z are from coords[1] to coords[num_coords-2] */
	/*if (realdata){
		const int num_atoms = (num_coor-2)/3;//1st: id, last 2: energy and rmsd
	}else{
		const int num_atoms = (num_coor-3)/3;//1st: id, last 3: energy, rmsd and real octkey
	}*/
	const int num_atoms = floor((num_coor-2)/3);
	
	logf_map_t_of<<"The num_atoms is: "<<num_atoms<<std::endl;
	double x[num_atoms], y[num_atoms], z[num_atoms];
	/*x,y,z double arries store the cooridnates of ligands */
	for (int i=0; i!=(num_atoms); i++){
		x[i] = coords[3*i];
		y[i] = coords[3*i+1]; 
		z[i] = coords[3*i+2]; 
	}
	/*compute the b0, b1, b2 using linear regression*/
	double b0 = slope(x, y, num_atoms);
	double b1 = slope(y, z, num_atoms);
	double b2 = slope(x, z, num_atoms);

	/*compute octkey, "digit" many digits*/
	int count=0;//count how many digits are in the octkey
	double minx = range_down, miny = range_down, minz = range_down;
	double maxx = range_up, maxy = range_up, maxz = range_up;
	while (count < digits){
		int m0 = 0, m1 = 0, m2 = 0;
		double medx = minx + ((maxx - minx)/2);
		if (b0>medx){
			m0=1;
			minx=medx;
		}else{
		
			maxx=medx;
		}
		double medy = miny + ((maxy-miny)/2);
		if (b1>medy){
			m1=1;
			miny=medy;
		}else{
		
			maxy=medy;
		}
		double medz = minz + ((maxz-minz)/2);
		if (b2>medz){
			m2=1;
			minz=medz;
		}else{
		
			maxz=medz;
		}
		/*calculate the octant using the formula m0*2^0+m1*2^1+m2*2^2*/
		int bit=m0+(m1*2)+(m2*4);
		char bitc=(char)(((int)'0') + bit); //int 8 => char '8'
		//keys[count] =  bit;
		//printf("Proc %d: the %d level octkey is: %c.\n",me, count,bitc);
		octkey[count] = bitc;
	
		++count;
	}
	logf_map_t_of<<"The octkey is: |"<<octkey<<"|."<<std::endl;
	logf_map_t_of<<"The real octkey is: |"<<coords[num_coor - 1]<<"|."<<std::endl;

	if (realdata == false){
	//use the real octkey from reading the line of synthetic dataset
		//strcpy(key, real_key.c_str());
		//printf("from process: %d, the real octkey is: %s, the copied real octkey is: %s.\n", itask, real_key.c_str(), key);
		//memcpy(key, (char *) &coords[num_coor - 1], digits);
		double realkey = coords[num_coor - 1];
		logf_map_t_of<<"The realkey is: "<<realkey<<std::endl;
		sprintf(key, "%f", realkey);
		logf_map_t_of<<"The key is: "<<key<<std::endl;
		*keysize = digits;

	}else{
		//using true data, use the octkey computed as the key
		memcpy(key, octkey, (digits));
		*keysize = digits;
	}

	/*no value, set the vsize to -1, since in the lib, it +1 to account for the null*/
	//char *myval ="1";
	*vsize = -1;
	logf_map_t_of.close();

}


double slope(double x[], double y[], int num_atoms){
	double slope=0.0;
	double sumx=0.0, sumy=0.0;
	for (int i=0; i!=num_atoms; ++i){
		sumx += x[i];
		sumy += y[i];
	}
	double xbar = sumx/num_atoms;
	double ybar = sumy/num_atoms;

	double xxbar =0.0, yybar =0.0, xybar =0.0;
	for (int i=0; i!=num_atoms; ++i){
		xxbar += (x[i] - xbar) * (x[i] - xbar);
		yybar += (y[i] - ybar) * (y[i] - ybar);
		xybar += (x[i] - xbar) * (y[i] - ybar);

	}
	slope = xybar / xxbar;
//	printf("Proc %d: the xybar, xxbar, and slope are: %f, %f, %f.\n",me, xybar, xxbar, slope);
	return slope;
	
}











