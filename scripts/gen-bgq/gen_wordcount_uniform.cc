#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <random>
#include <cmath>
#include <unordered_set>

#include "mpi.h"

using namespace std;

int get_word_length(int mean, double sd)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(max(0.0, mean - 3 * sd), mean + 3 * sd);
    return round(dis(gen));
}

int generate_unique_words(int n_unique, vector<string>& output)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0,25);

    int n_buckets = 1, n_word = 0;
    while (n_buckets < n_unique) n_buckets <<= 1;
    unordered_set<string> new_words(n_buckets);
    output.reserve(n_unique);
    while (n_word < n_unique) {
        char word[16];
        memset(word, 0, 16);
        int wordlen = get_word_length(6, 1);
        for (int i = 0; i < wordlen; i++) {
            word[i] = 'a' + dis(gen);
        }
        word[wordlen] = 0;
        string new_word = word;

        auto found = new_words.find(new_word);
        if (found == new_words.end()) {
            new_words.insert(new_word);
            n_word += 1;
        }
    }

    for (auto word : new_words) {
        output.push_back(word);
    }

    return 0;
}

int main(int argc, char *argv[])
{
    int ret = 0;
    int worldsize = 0, worldrank = 0;
    int rank_per_node = 0;

    unsigned long long fsize = 0;
    int n_unique = 0;

    string key_file_prefix;
    string outdir;
    char path[256];

    if (argc < 6) {
        printf("./exe <prefix> <outdir> <fsize> <n_unique_words> <process per node>\n");
        return 0;
    }

    std::random_device rd;
    std::mt19937 gen(rd());

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &worldsize);
    MPI_Comm_rank(MPI_COMM_WORLD, &worldrank);

    key_file_prefix = string(argv[1]);
    outdir = string(argv[2]);
    fsize = strtoull(argv[3], NULL, 10);
    n_unique = atoi(argv[4]);
    rank_per_node = atoi(argv[5]);

    printf("The file prefix is: %s\n", key_file_prefix.c_str());
    printf("filesize: %llu n_unique words %d\n", fsize, n_unique);

    MPI_Comm splitcomm;
    MPI_Comm_split(MPI_COMM_WORLD, worldrank / rank_per_node, worldrank, &splitcomm);
    int rank;
    MPI_Comm_rank(splitcomm, &rank);

    vector<string> unique_words;
    unique_words.reserve(n_unique);
    if (rank == 0) {
        generate_unique_words(n_unique, unique_words);
        cout << "rank " << rank << " generate" << endl;
        //broadcast
        int* size = (int*) malloc(sizeof(int) * n_unique);
        int sum = 0;
        int i = 0;
        for (auto word : unique_words) {
            size[i] = word.size();
            sum += size[i];
            i++;
        }
        MPI_Bcast(size, n_unique, MPI_INT, 0, splitcomm);
        char* buf = (char*) malloc(sizeof(char) * sum);
        char* ptr = buf;
        for (auto word : unique_words) {
            memcpy(ptr, word.c_str(), word.size());
            ptr += word.size();
        }
        MPI_Bcast(buf, sum, MPI_BYTE, 0, splitcomm);
        free(size);
        free(buf);
    } else {
        //receive
        int* size = (int*) malloc(sizeof(int) * n_unique);
        MPI_Bcast(size, n_unique, MPI_INT, 0, splitcomm);
        int sum = 0;
        for (int i = 0; i < n_unique; i++) {
            sum += size[i];
        }
        char* buf = (char*) malloc(sizeof(char) * sum);
        char* ptr = buf;
        MPI_Bcast(buf, sum, MPI_BYTE, 0, splitcomm);
        for (int i = 0; i < n_unique; i++) {
            char* localbuf = (char*) malloc(sizeof(char) * size[i]);
            memcpy(localbuf, ptr, size[i]);
            ptr += size[i];
            unique_words.push_back(string(localbuf));
            free(localbuf);
        }
        free(size);
        free(buf);
    }

    cout << "generate words" << endl;

    std::uniform_int_distribution<> dis(0, n_unique - 1);

    char filename[128];
    memset(filename, 0, 128);
    sprintf(filename, "%s/%s.unique.%d.txt", outdir.c_str(), key_file_prefix.c_str(), worldrank);
    ofstream out_file(filename, ios::out);

    for (unsigned long long size = 0; size < fsize; ) {
        int idx = dis(gen);
        out_file << unique_words[idx] << " ";
        size += strlen(unique_words[idx].c_str()) + 1;
    }

    MPI_Finalize();
    return 0;
}
