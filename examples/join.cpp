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
        = new MimirContext<KeyType, SingleVal, char*, void>(MPI_COMM_WORLD,
                                                            read_dataset,NULL,
                                                            input1,output,
                                                            NULL, NULL, NULL,
                                                            false);
    datatag = DATA1_TAG;
    data1->map(NULL, &datatag);

    // Get Dataset2
    MimirContext<KeyType, SingleVal, char*, void>* data2 
        = new MimirContext<KeyType, SingleVal, char*, void>(MPI_COMM_WORLD,
                                                            read_dataset, NULL,
                                                            input2, output,
                                                            NULL, NULL, NULL,
                                                            false);
    datatag = DATA2_TAG;
    data2->map(NULL, &datatag);

    // Merge Dataset1 and Dataset2
    MimirContext<KeyType, SingleVal, KeyType, SingleVal, KeyType, JoinedVal>* ctx 
        = new MimirContext<KeyType, SingleVal, KeyType, SingleVal, KeyType, JoinedVal>(MPI_COMM_WORLD,
            map, reduce,
            input1, output,
            NULL,
            NULL,
            NULL,
            true,
            IMPLICIT_OUTPUT);
    ctx->set_outfile_format("text");
    ctx->insert_data(data1->get_output_handle());
    ctx->insert_data(data2->get_output_handle());
    ctx->map();
    delete data1;
    delete data2;

    ctx->reduce();
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

    while (input->read(&line, NULL) == 0) {

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
    while (input->read(&key, &val) == 0) {
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
    while (input->read(&key, &val) == 0) {
        if (val.tag == DATA1_TAG) val1count +=1;
        else if (val.tag == DATA2_TAG) val2count += 1;
    }

    if (val1count <= val2count) datatag = DATA1_TAG;
    else datatag = DATA2_TAG;

    // Get values of smaller dataset
    while (input->read(&key, &val) == 0) {
        if (val.tag == datatag) {
            db.push_back(val.val);
        }
    }

    // Get final results
    while (input->read(&key, &val) == 0) {
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
