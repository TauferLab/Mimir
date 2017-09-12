/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unordered_set>
#include <stdint.h>

#include "mimir.h"

using namespace MIMIR_NS;

#define DATA1_TAG   0xaa
#define DATA2_TAG   0xbb

typedef char*   KeyType;
typedef int64_t ValType1;
typedef int64_t ValType2;

union ValType {
    ValType1 val1;
    ValType2 val2;
};

struct SingleVal {
    int      tag;   // dataset id
    ValType  val;   // autual value

    std::stringstream& operator>>(std::stringstream& ss)
    {
        ss << this->tag;
        return ss;
    }
};

struct JoinedVal {
    ValType1     val1; // value from dataset 1
    ValType2     val2; // value from dataset 2

    std::stringstream& operator>>(std::stringstream& ss)
    {
        ss << this->val1;
        ss << " ";
        ss << this->val2;
        return ss;
    }
};

int rank, size;

void read_dataset (Readable<char*,void> *input,
                   Writable<KeyType,SingleVal> *output, void *ptr);
void map (Readable<KeyType, SingleVal> *input,
          Writable<KeyType, SingleVal> *output, void *ptr);
void reduce (Readable<KeyType, SingleVal> *input,
             Writable<KeyType, JoinedVal> *output, void *ptr);

#ifdef SPLIT_HINT
std::unordered_map<std::string,std::vector<ValType>> ds1;
std::unordered_map<std::string,std::vector<ValType>> ds2;
std::unordered_set<std::string>           split_keys;
void get_split_key(KeyType *key, void *ptr);
void join_split_key(std::string&);
#endif

int main (int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 4) {
        if (rank == 0)
            fprintf(stdout, "Usage: %s output input1 input2\n", argv[0]);
        return 0;
    }

    std::string output = argv[1];
    std::vector<std::string> input1, input2;
    input1.push_back(argv[2]);
    input2.push_back(argv[3]);

    // Get Dataset1
    int datatag = 0;
    MimirContext<KeyType, SingleVal, char*, void>* data1 
        = new MimirContext<KeyType, SingleVal, char*, void>(input1);
    datatag = DATA1_TAG;
    data1->map(read_dataset, &datatag, false);

    // Get Dataset2
    MimirContext<KeyType, SingleVal, char*, void>* data2 
        = new MimirContext<KeyType, SingleVal, char*, void>(input2);
    datatag = DATA2_TAG;
    data2->map(read_dataset, &datatag, false);

    // Merge Dataset1 and Dataset2
    MimirContext<KeyType, SingleVal, KeyType, SingleVal, KeyType, JoinedVal>* ctx 
        = new MimirContext<KeyType, SingleVal, KeyType, SingleVal, KeyType, JoinedVal>(
            std::vector<std::string>(), output, MPI_COMM_WORLD);
    ctx->insert_data_handle(data1->get_data_handle());
    ctx->insert_data_handle(data2->get_data_handle());
#ifndef SPLIT_HINT
    ctx->map(map);
#else
    ctx->map(map, NULL, true, false, "", true);
    ctx->scan_split_keys(get_split_key);
#endif
    delete data1;
    delete data2;

    ctx->reduce(reduce, NULL, true, "text");

#ifdef SPLIT_HINT
    printf("%d[%d] Join split keys=%ld, ds1=%ld, ds2=%ld\n",
            rank, size, split_keys.size(), ds1.size(), ds2.size());
    join_split_key(output);
    printf("%d[%d] Join split keys done!\n", rank, size);
#endif

    delete ctx;

    MPI_Finalize();
}

void read_dataset (Readable<char*,void> *input,
                   Writable<KeyType,SingleVal> *output, void *ptr)
{
    int datatag = *(int*)ptr;
    char      *line = NULL;
    char      *key = NULL;
    SingleVal  val;

    while (input->read(&line, NULL) == true) {

        char *saveptr = NULL, *word = NULL;

        word = strtok_r(line, " ", &saveptr);
        if (word == NULL) {
            fprintf(stderr, "Input file format error!\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        key = word;

        word = strtok_r(NULL, " ", &saveptr);
        if (word == NULL) {
            fprintf(stderr, "Input file format error!\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        if (datatag == DATA1_TAG) {
            val.val.val1 = strtoull(word, NULL, 0);
        }
        else if (datatag == DATA2_TAG) {
            val.val.val2 = strtoull(word, NULL, 0);
        } else {
            fprintf(stderr, "Error dataset tag!\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        val.tag = datatag;

        output->write(&key, &val);
    }
}

void map (Readable<KeyType, SingleVal> *input,
          Writable<KeyType, SingleVal> *output, void *ptr)
{
    KeyType key;
    SingleVal val;
    while (input->read(&key, &val) == true) {
        output->write(&key, &val);
    }
}

void reduce (Readable<KeyType,SingleVal> *input,
             Writable<KeyType,JoinedVal> *output, void *ptr) 
{
    std::vector<ValType> db;
    KeyType key;
    SingleVal val;
    int datatag = 0;

    // Set datatag to the tag of smaller dataset
    uint64_t val1count = 0, val2count = 0;

#ifdef SPLIT_HINT
    if (input->read(&key, &val) == true) {
        if (val.tag == DATA1_TAG) val1count +=1;
        else if (val.tag == DATA2_TAG) val2count += 1;
        // This is a splited key
        if (split_keys.find(std::string(key)) != split_keys.end()) {
            if (val.tag == DATA1_TAG) {
                auto iter = ds1.find(std::string(key));
                if (iter != ds1.end()) {
                    iter->second.push_back(val.val);
                } else {
                    ds1[key] = std::vector<ValType>();
                    ds1[key].push_back(val.val);
                }
            }
            else if (val.tag == DATA2_TAG) {
                auto iter = ds2.find(key);
                if (iter != ds2.end()) {
                    iter->second.push_back(val.val);
                } else {
                    ds2[key] = std::vector<ValType>();
                    ds2[key].push_back(val.val);
                }
            }
            while (input->read(&key, &val) == true) {
                if (val.tag == DATA1_TAG) {
                    auto iter = ds1.find(std::string(key));
                    if (iter != ds1.end()) {
                        iter->second.push_back(val.val);
                    } else {
                        ds1[key] = std::vector<ValType>();
                        ds1[key].push_back(val.val);
                    }
                }
                else if (val.tag == DATA2_TAG) {
                    auto iter = ds2.find(key);
                    if (iter != ds2.end()) {
                        iter->second.push_back(val.val);
                    } else {
                        ds2[key] = std::vector<ValType>();
                        ds2[key].push_back(val.val);
                    }
                }
            }

            return;
        }
    }
#endif

    while (input->read(&key, &val) == true) {
        if (val.tag == DATA1_TAG) val1count +=1;
        else if (val.tag == DATA2_TAG) val2count += 1;
    }

    if (val1count <= val2count) datatag = DATA1_TAG;
    else datatag = DATA2_TAG;

    // Get values of smaller dataset
    while (input->read(&key, &val) == true) {
        if (val.tag == datatag) {
            db.push_back(val.val);
        }
    }

    // Get final results
    while (input->read(&key, &val) == true) {
        if (val.tag != datatag) {
            auto iter = db.begin();
            for (; iter != db.end(); iter ++) {
                JoinedVal jval;
                // Smaller dataset is Dataset1
                if (datatag == DATA1_TAG) {
                    jval.val1 = iter->val1;
                    jval.val2 = val.val.val2;
                // Smaller dataset is Dataset2
                } else if (datatag == DATA2_TAG) {
                    jval.val2 = iter->val2;
                    jval.val1 = val.val.val1;
                }
                output->write(&key, &jval);
            }
        }
    }
}

#ifdef SPLIT_HINT
void get_split_key(KeyType *key, void *ptr) {
    printf("split key=%s\n", *key);
    split_keys.insert(std::string(*key));
}

void join_split_key(std::string& output) {

    uint64_t ds1_local, ds1_global;
    uint64_t ds2_local, ds2_global;
    int small_idx = 0;
    std::unordered_map<std::string,std::vector<ValType>> *small_ds = NULL;
    std::unordered_map<std::string,std::vector<ValType>> *large_ds = NULL;

    // Get the small and large dataset
    ds1_local = ds1.size();
    ds2_local = ds2.size();

    MPI_Allreduce(&ds1_local, &ds1_global, 1,
                  MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&ds2_local, &ds2_global, 1,
                  MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    if (ds1_global == 0 || ds2_global == 0) return;
    if (ds1_global <= ds2_global) {
        small_ds = &ds1;
        large_ds = &ds2;
        small_idx = 1;
    }
    else {
        small_ds = &ds2;
        large_ds = &ds1;
        small_idx = 2;
    }

    // Get send and recv counts
    int sendcount, recvcount;
    int recvcounts[size], displs[size];
    sendcount = 0;
    for (auto iter : *small_ds) {
        sendcount += (int)strlen(iter.first.c_str()) + 1;
        sendcount += (int)sizeof(int);
        sendcount += (int)sizeof(ValType) * (int)(iter.second.size());
    }
    MPI_Allgather(&sendcount, 1, MPI_INT,
                  recvcounts, 1, MPI_INT, MPI_COMM_WORLD);
    recvcount  = recvcounts[0];
    displs[0] = 0;
    for (int i = 1; i < size; i++) {
        displs[i] = displs[i - 1] + recvcounts[i - 1];
        recvcount += recvcounts[i];
    }

    printf("%d[%d] sendcount=%d, recvcount=%d\n",
           rank, size, sendcount, recvcount);

    // Allgather data
    char sendbuf[sendcount], recvbuf[recvcount];
    int off = 0;
    for (auto iter1 : *small_ds) {
        memcpy(sendbuf+off, iter1.first.c_str(), (int)(iter1.first.size()) + 1);
        off += (int)(iter1.first.size()) + 1;
        *(int*)(sendbuf+off) = (int)iter1.second.size();
        off += (int)sizeof(int);
        for (auto iter2 : iter1.second) {
            memcpy(sendbuf+off, &iter2, sizeof(ValType));
            off += (int)sizeof(ValType);
        }
    }
    if (sendcount != off) {
        fprintf(stderr, "Error while join split keys!\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Allgatherv(sendbuf, sendcount, MPI_BYTE,
                   recvbuf, recvcounts, displs, MPI_BYTE, MPI_COMM_WORLD);

    small_ds->clear();

    off = 0;
    while (off < recvcount) {
        KeyType key = recvbuf + off;
        off += (int)strlen(key) + 1;
        (*small_ds)[key] = std::vector<ValType>();
        int valcount = *(int*)(recvbuf+off);
        off += (int)sizeof(int);
        for (int i = 0; i < valcount; i++) {
            ValType val;
            if (small_idx == 1) val.val1 = *(int64_t*)(recvbuf+off);
            else if (small_idx == 2) val.val2 = *(int64_t*)(recvbuf+off);
            (*small_ds)[key].push_back(val);
            off += (int)sizeof(int64_t);
        }
    }

    char filename[1024];
    sprintf(filename, "%s.split.%d.%d", output.c_str(), size, rank);
    FILE *fp = fopen(filename, "w");
    if (!fp) {
        fprintf(stderr, "Output error!\n");
        exit(1);
    }
    for (auto iter1 : *small_ds) {
        auto iter2 = (*large_ds).find(iter1.first.c_str());
        if (iter2 != (*large_ds).end()) {
            for (auto item2: iter2->second) {
                for (auto item1 : iter1.second) {
                    if (small_idx == 1) {
                        fprintf(fp, "%s %ld %ld\n", iter1.first.c_str(), item1.val1, item2.val2);
                    } else if (small_idx == 2) {
                        fprintf(fp, "%s %ld %ld\n", iter1.first.c_str(), item2.val2, item1.val1);
                    }
                }
            }
        }
    }
    fclose(fp);
    printf("%d[%d] Output split keys to %s.\n", rank, size, filename);
}

#endif


