#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <stdint.h>

#include "common.h"
#include "mimir.h"

//#define VALUE_STRING

using namespace MIMIR_NS;

int rank, size;

void map (Readable *input, Writable *output, void *ptr);
void countword (Readable *input, Writable *output, void *ptr);
void combine (Combinable *combiner, KVRecord *kv1, KVRecord *kv2, void *ptr);

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);
    Mimir_Init(&argc, &argv, MPI_COMM_WORLD);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (argc < 5) {
        if (rank == 0)
            printf("Syntax: wordcount filepath prefix outdir tmpdir\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    char *filedir = argv[1];
    const char *prefix = argv[2];
    const char *outdir = argv[3];
    const char *tmpdir = argv[4];

    if (rank == 0) {
        printf("input dir=%s\n", filedir);
        printf("prefix=%s\n", prefix);
        printf("output dir=%s\n", outdir);
        printf("tmp dir=%s\n", tmpdir);
    }

    check_envars(rank, size);

    InputSplit* splitinput = FileSplitter::getFileSplitter()->split(filedir);
    StringRecord::set_whitespace(" \n");
    FileReader<StringRecord> reader(splitinput);

    KVContainer container;
    MimirContext context;
    context.set_map_callback(map);
    context.set_reduce_callback(countword);
    context.mapreduce(&reader, &container, NULL);

    container.open();
    KVRecord *record = NULL;
    while ((record = container.read()) != NULL) {
        printf("%s, %ld\n", record->get_key(), 
               *(int64_t*)record->get_val());
    }
    container.close();

    output(rank, size, prefix, outdir);

     Mimir_Finalize();
    MPI_Finalize();
}

void map (Readable *input, Writable *output, void *ptr)
{
    char *word = NULL;
    int len = 0;

    BaseRecordFormat *input_record = NULL;
    while ((input_record = input->read()) != NULL) {
        word = input_record->get_record();
        len = input_record->get_record_size();
        if (len <= 1024) {
#ifdef VALUE_STRING
            char tmp[10] = { "1" };
            KVRecord output_record(word, len, tmp, 2);
            output->write(&output_record);
#else
            int64_t one = 1;
            KVRecord output_record(word, len, (char*)&one, sizeof(one));
            output->write(&output_record);
#endif
        }
    }
}

void countword (Readable *input, Writable *output, void *ptr) {
    int64_t count = 0;
    KMVRecord *kmv = NULL;
    char *val = NULL;

    BaseRecordFormat *input_record = NULL;
    while ((input_record = input->read()) != NULL) {
        kmv = (KMVRecord*)input_record;
        count = 0;
        val = NULL;

        while ((val = kmv->get_next_val()) != NULL) {
#ifdef VALUE_STRING
            count += strtoull((const char *) val, NULL, 0);
#else
            count += *(int64_t *) val;
#endif
        }
        KVRecord output_record(kmv->get_key(),
                               kmv->get_key_size(),
                               (const char*)&count,
                               (int)sizeof(count));
        output->write(&output_record);
    }
}

void combine(Combinable *combiner, KVRecord *kv1, KVRecord *kv2, void *ptr)
{

#ifdef VALUE_STRING
    int64_t count = strtoull(kv1->get_val(), NULL, 0) 
        + strtoull(get_val(), NULL, 0);
    char tmp[20] = { 0 };
    sprintf(tmp, "%ld", count);
    KVRecord update_record(kv1->get_key(), 
                           kv1->get_key_size(), 
                           tmp, (int)strlen(tmp)+1);
    combiner->update(&update_record);
#else
    int64_t count = *(int64_t *) (kv1->get_val()) 
        + *(int64_t *) (kv2->get_val());
    KVRecord update_record(kv1->get_key(), 
                           kv1->get_key_size(), 
                           (char *) &count, sizeof(count));
    combiner->update(&update_record);
#endif
}

