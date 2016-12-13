#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <string.h>
#include <sys/stat.h>

#include <random>
#include <string>
#include <vector>
#include <unordered_set>

#include "mapreduce.h"

using namespace MIMIR_NS;

int rank, size;

int generate_unique_words(int, std::vector<std::string>&);

void insert_unique_words(MapReduce *,void *);
void remove_dup_words(MapReduce *,char *,int, void*);
void get_unique_words(MapReduce *, char *, int, char *, int, void *);

const char *prefix = NULL;
const char *outdir = NULL;
int64_t fsize = 0;
int nunique = 0;

std::vector<std::string> unique_words;

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if(argc < 5){
        if(rank == 0)
            printf("Syntax: <prefix> <outdir> <fsize> <uniquewords>\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    const char *prefix = argv[1];
    const char *outdir = argv[2];
    int64_t fsize = strtoull(argv[3],NULL,0);
    int nunique = atoi(argv[4]);

    if(rank==0){
        printf("prefix=%s\n", prefix);
        printf("outdir=%s\n", outdir);
        printf("fsize=%ld\n", fsize);
        printf("nunqiue=%d\n", nunique);
    }

    printf("generate unique words...\n");

    int local_unique_words = nunique/size;
    generate_unique_words(local_unique_words, unique_words);

    printf("remove duplicate unique words...\n");

    MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
    // Insert unique words
    mr->init_key_value(insert_unique_words);
    // Remove duplicate words
    mr->reduce(remove_dup_words);
    // Collect all unique words to rank 0
    mr->collect();
    
    // Insert unique words to vector
    unique_words.clear(); 
    mr->map_key_value(mr, get_unique_words, NULL, 0);

    // Generate unique words to satisfy the requirement
    if(rank==0){
        int remain_nunique=nunique-(int)unique_words.size();
        generate_unique_words(remain_nunique, unique_words);
    }
    // Insert unique words again
    mr->init_key_value(insert_unique_words, NULL, 0);
    // Broadcast unique words to all processes
    mr->bcast();
    // Get final unique words list
    unique_words.clear();
    mr->map_key_value(mr, get_unique_words, NULL, 0);
    delete mr;

    printf("nunique=%d\n", (int)unique_words.size());
    printf("generate words...\n");

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, nunique-1);

    int64_t lfsize=fsize/size;
    if(rank<fsize%size)  lfsize+=1;

    // Get filename
    char filename[1024];
    memset(filename, 0, 128);
    sprintf(filename, "%s/%s.unique.%d.txt", outdir, prefix, rank);

    // Write files
    FILE *fp=fopen(filename, "w");
    int64_t foff=0;
    while(foff < lfsize){
        int idx = dis(gen);
        int word_len=(int)strlen(unique_words[idx].c_str());
        if(word_len+foff<lfsize){
            fprintf(fp, "%s", unique_words[idx].c_str());
            foff+=word_len;
            if(foff<lfsize) {
                fprintf(fp, " ");
                foff+=1;
            }
        }else{
            int64_t nwhite=lfsize-foff;
            for(int64_t i=0; i<nwhite; i++)
                fprintf(fp, " ");
            foff+=nwhite;
        }
    }
    fclose(fp);

    MPI_Finalize();

}

// Insert unique words from vector to MR
void insert_unique_words(MapReduce *mr, void *ptr){
    auto iter = unique_words.begin();
    for(; iter != unique_words.end(); iter++){
        mr->add_key_value(iter->c_str(), int(strlen(iter->c_str())+1), \
            NULL, 0);
    }
}
// Reduce unique words
void remove_dup_words(MapReduce *mr, char *key, int keybytes, void *ptr){
    mr->add_key_value(key, keybytes, NULL, 0);
}
// Insert unique words from MR to vector
void get_unique_words(MapReduce *mr, char *key, int keybytes, \
    char *val, int valbytes, void *ptr){
    unique_words.push_back(key);
}

// Get word length
int get_word_length(int mean, double sd)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::normal_distribution<double> dis(mean, sd);
    return (int)std::lround(dis(gen));
}

// Generate unique words locally
int generate_unique_words(int n_unique_words, std::vector<std::string>& words)
{

    std::unordered_set<std::string> output;

    for(auto word : words){
        output.insert(word);
    }

    words.clear();

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0,25);

    int nword=0;
    while(nword < n_unique_words){
        char word[100];
        memset(word, 0, 100);
        int wordlen = get_word_length(6, 1);
        for (int i = 0; i < wordlen; i++) {
            word[i] = 'a' + dis(gen);
        }
        word[wordlen]='\0';

        std::string new_word = word;
        
        auto found = output.find(new_word);
        if(found == output.end()){
            output.insert(new_word);
            nword += 1;
        }
    }

    for(auto word : output){
        words.push_back(word);
    }

    return nword;
}

