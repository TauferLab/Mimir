//
// (c) 2017 by University of Delaware, Argonne National Laboratory, San Diego
//     Supercomputer Center, National University of Defense Technology,
//     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
//
//     See COPYRIGHT in top-level directory.
//

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <limits.h>

#include <fstream>
#include <iostream>
#include <map>
#include <vector>
#include <string>
#include <random>
#include <cmath>
#include <set>
#include <unordered_set>
#include <algorithm>

#include <mpi.h>
#include "mimir.h"

const char dict[]
    = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
       'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
       'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
       'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
       '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

std::string get_unique_word(uint64_t idx, int len)
{
    std::string word;
    uint64_t mod_base = sizeof(dict);
    for (int i = 0; i < len; i++) {
        uint64_t char_idx = idx % mod_base;
        word += dict[char_idx];
        idx = (idx - char_idx) / mod_base;
    }
    return word;
}

int get_word_length(int mean, double sd)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(std::max(0.0, mean - 3 * sd),
                                        mean + 3 * sd);
    return round(dis(gen));
}

int generate_unique_words(uint64_t n_unique, std::vector<std::string>& output,
                          int len_mean, double len_sd)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 61);

    int n_buckets = 1, n_word = 0;
    while (n_buckets < n_unique) n_buckets <<= 1;
    std::unordered_set<std::string> new_words(n_buckets);
    output.reserve(n_unique);
    while (n_word < n_unique) {
        char word[16];
        memset(word, 0, 16);
        int wordlen = get_word_length(len_mean, len_sd);
        for (int i = 0; i < wordlen; i++) {
            word[i] = dict[dis(gen)];
        }
        word[wordlen] = 0;
        std::string new_word = word;

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

void print_dist_map(uint64_t zipf_n, double zipf_alpha, double* dist_map)
{
    int proc_rank;
    int proc_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_size);

    if (proc_size > zipf_n) {
        fprintf(stderr, "The process count should larger n!\n");
        exit(1);
    }

    if (proc_rank == 0) {
        fprintf(stdout, "zipf (%ld,%.2lf) distribution:\n", zipf_n, zipf_alpha);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    uint64_t div_size = zipf_n / proc_size;
    uint64_t div_off = div_size * proc_rank;
    if (proc_rank < (zipf_n % proc_size)) div_size += 1;
    if (proc_rank < (zipf_n % proc_size))
        div_off += proc_rank;
    else
        div_off += (zipf_n % proc_size);

    for (uint64_t i = 0; i < div_size; i++) {
        fprintf(stdout, "%ld:%.6lf\n", div_off + i, dist_map[i]);
    }

    return;
}

void gen_dist_map(uint64_t zipf_n, double zipf_alpha, double* dist_map,
                  uint64_t* div_idx_map, double* div_dist_map)
{
    int proc_rank;
    int proc_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_size);

    if (proc_size > zipf_n) {
        fprintf(stderr, "The process count should larger n!\n");
        exit(1);
    }

    uint64_t div_size = zipf_n / proc_size;
    uint64_t div_off = div_size * proc_rank;
    if (proc_rank < (zipf_n % proc_size)) div_size += 1;
    if (proc_rank < (zipf_n % proc_size))
        div_off += proc_rank;
    else
        div_off += (zipf_n % proc_size);

    double tmp[div_size];
    double zeta[div_size];

    for (uint64_t i = 0; i < div_size; i++) dist_map[i] = 0.0;

    for (uint64_t i = 0; i < div_size; i++) {
        tmp[i] = 1.0 / (pow(i + 1 + div_off, zipf_alpha));
    }

    double prev_sum = 0.0, next_sum = 0.0, total_sum = 0.0;

    if (proc_rank != 0) {
        MPI_Status st;
        MPI_Recv(&prev_sum, 1, MPI_DOUBLE, proc_rank - 1, 0x11, MPI_COMM_WORLD,
                 &st);
    }

    zeta[0] = tmp[0] + prev_sum;
    for (uint64_t i = 1; i < div_size; i++) {
        zeta[i] = zeta[i - 1] + tmp[i];
    }

    total_sum = zeta[div_size - 1];
    if (proc_rank != proc_size - 1) {
        MPI_Send(&total_sum, 1, MPI_DOUBLE, proc_rank + 1, 0x11,
                 MPI_COMM_WORLD);
    }

    MPI_Bcast(&total_sum, 1, MPI_DOUBLE, proc_size - 1, MPI_COMM_WORLD);

    for (uint64_t i = 0; i < div_size; i++) {
        dist_map[i] = zeta[i] / total_sum;
    }
    prev_sum /= total_sum;

    if (proc_rank != 0) {
        MPI_Send(&dist_map[0], 1, MPI_DOUBLE, proc_rank - 1, 0x11,
                 MPI_COMM_WORLD);
    }

    if (proc_rank != proc_size - 1) {
        MPI_Status st;
        MPI_Recv(&next_sum, 1, MPI_DOUBLE, proc_rank + 1, 0x11, MPI_COMM_WORLD,
                 &st);
    }

    double mean_dist = 1.0 / proc_size;
    std::vector<uint64_t> idxsep;
    std::vector<double> distsep;
    uint64_t ntimes = 0;
    if (proc_rank != 0) {
        MPI_Status st;
        MPI_Recv(&ntimes, 1, MPI_UINT64_T, proc_rank - 1, 0x11, MPI_COMM_WORLD,
                 &st);
    }

    //printf("%d[%d] prev_sum=%lf, ntimes=%ld\n",
    //       proc_rank, proc_size, prev_sum, ntimes);
    //fflush(stdout);

    for (uint64_t i = 0; i < div_size; i++) {
        while (dist_map[i] > mean_dist * (ntimes + 1)) {
            printf("%d[%d] find sep=%d\n", proc_rank, proc_size,
                   div_off + i + 1);
            fflush(stdout);
            idxsep.push_back(div_off + i + 1);
            prev_sum = dist_map[i];
            distsep.push_back(prev_sum);
            ntimes += 1;
        }
    }
    if (proc_rank == proc_size - 1) {
        idxsep.push_back(zipf_n);
        distsep.push_back(1.0);
    }
    if (proc_rank != proc_size - 1) {
        MPI_Send(&ntimes, 1, MPI_UINT64_T, proc_rank + 1, 0x11, MPI_COMM_WORLD);
    }

    int sendcount = (int) idxsep.size(), recvcounts[proc_size],
        displs[proc_size + 1];
    MPI_Allgather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT,
                  MPI_COMM_WORLD);

    displs[0] = 0;
    for (int i = 0; i < proc_size; i++) {
        displs[i + 1] = displs[i] + recvcounts[i];
    }

    int recvcount = 0;
    for (int i = 0; i < proc_size; i++) {
        recvcount += recvcounts[i];
    }
    if (recvcount != proc_size) {
        fprintf(stderr, "Repartition error (sep count=%d)!\n", recvcount);
        exit(1);
    }

    uint64_t sendidx[sendcount];
    double senddist[sendcount];
    for (size_t i = 0; i < idxsep.size(); i++) {
        sendidx[i] = idxsep[i];
        senddist[i] = distsep[i];
    }
    MPI_Allgatherv(sendidx, sendcount, MPI_UINT64_T, div_idx_map + 1,
                   recvcounts, displs, MPI_UINT64_T, MPI_COMM_WORLD);
    MPI_Allgatherv(senddist, sendcount, MPI_DOUBLE, div_dist_map + 1,
                   recvcounts, displs, MPI_DOUBLE, MPI_COMM_WORLD);
    div_idx_map[0] = 0;
    div_dist_map[0] = 0.0;

    if (proc_rank == 0) {
        for (int i = 0; i < proc_size + 1; i++) {
            printf("%ld %.6lf\n", div_idx_map[i], div_dist_map[i]);
        }
    }
}

void repartition_dist_map(uint64_t zipf_n, double* dist_map,
                          uint64_t* div_idx_map, double* dist_new_map)
{
    int proc_rank, proc_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_size);

    uint64_t div_size = zipf_n / proc_size;
    uint64_t div_off = div_size * proc_rank;
    if (proc_rank < (zipf_n % proc_size)) div_size += 1;
    if (proc_rank < (zipf_n % proc_size))
        div_off += proc_rank;
    else
        div_off += (zipf_n % proc_size);

    int sendcounts[proc_size], recvcounts[proc_size];
    int sdispls[proc_size + 1], rdispls[proc_size + 1];

    for (int i = 0; i < proc_size; i++) {
        uint64_t low = div_idx_map[i];
        uint64_t high = div_idx_map[i + 1];

        // on overlapping
        if (low >= div_off + div_size || high <= div_off) sendcounts[i] = 0;
        // contain
        else if (low >= div_off && high <= div_off + div_size) {
            sendcounts[i] = high - low;
            // partial overlapping
        }
        else if (high <= div_off + div_size && high >= div_off
                 && low <= div_off) {
            sendcounts[i] = high - div_off;
            // partial overlapping
        }
        else if (low >= div_off && low <= div_off + div_size
                 && high >= div_off + div_size) {
            sendcounts[i] = div_off + div_size - low;
        }
        else if (low <= div_off && high >= div_off + div_size) {
            sendcounts[i] = div_size;
        }
        else {
            fprintf(stderr, "%d Error while computing the send counts!\n",
                    proc_rank);
            exit(1);
        }
    }

    int sendcount = 0;
    for (int i = 0; i < proc_size; i++) {
        sendcount += sendcounts[i];
    }

    if (sendcount != div_size) {
        fprintf(stderr,
                "%d[%d] the sendcount (%d) doest not match size (%ld)\n",
                proc_rank, proc_size, sendcount, div_size);
        exit(1);
    }

    MPI_Alltoall(sendcounts, 1, MPI_INT, recvcounts, 1, MPI_INT,
                 MPI_COMM_WORLD);

    sdispls[0] = rdispls[0] = 0;
    int recvcount = 0;
    for (int i = 1; i < proc_size + 1; i++) {
        sdispls[i] = sdispls[i - 1] + sendcounts[i - 1];
        rdispls[i] = rdispls[i - 1] + recvcounts[i - 1];
        recvcount += recvcounts[i - 1];
    }

    if (div_idx_map[proc_rank + 1] - div_idx_map[proc_rank] != recvcount) {
        fprintf(stderr,
                "%d[%d] the recvcount (%d) doest not match size (%ld)\n",
                proc_rank, proc_size, recvcount,
                div_idx_map[proc_rank + 1] - div_idx_map[proc_rank]);
        exit(1);
    }

    MPI_Alltoallv(dist_map, sendcounts, sdispls, MPI_DOUBLE, dist_new_map + 1,
                  recvcounts, rdispls, MPI_DOUBLE, MPI_COMM_WORLD);

    dist_new_map[0] = 0.0;
    if (proc_rank != proc_size - 1) {
        MPI_Send(&dist_new_map[recvcount], 1, MPI_DOUBLE, proc_rank + 1, 0x11,
                 MPI_COMM_WORLD);
    }
    if (proc_rank != 0) {
        MPI_Status st;
        MPI_Recv(&dist_new_map[0], 1, MPI_DOUBLE, proc_rank - 1, 0x11,
                 MPI_COMM_WORLD, &st);
    }
}

void random_exchange(std::vector<std::string>& unique_words)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, unique_words.size() - 1);
    for (size_t i = 0; i < unique_words.size(); i++) {
        size_t p0 = dis(gen);
        size_t p1 = dis(gen);
        std::string tmp = unique_words[p0];
        unique_words[p0] = unique_words[p1];
        unique_words[p1] = tmp;
    }
}

void repartition_unique_words(std::vector<std::string>& unique_words,
                              std::vector<std::string>& unique_new_words,
                              uint64_t* div_idx_map)
{
    int proc_rank, proc_size;
    uint64_t word_off, word_size;

    MPI_Comm_rank(MPI_COMM_WORLD, &proc_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_size);

    word_off = 0;
    word_size = unique_words.size();

    if (proc_rank != 0) {
        MPI_Status st;
        MPI_Recv(&word_off, 1, MPI_UINT64_T, proc_rank - 1, 0x11,
                 MPI_COMM_WORLD, &st);
    }

    if (proc_rank != proc_size - 1) {
        uint64_t tmp = word_off + word_size;
        MPI_Send(&tmp, 1, MPI_UINT64_T, proc_rank + 1, 0x11, MPI_COMM_WORLD);
    }

    char *sendbuf = NULL, *recvbuf = NULL;
    int sendcounts[proc_size], recvcounts[proc_size];
    int sdispls[proc_size + 1], rdispls[proc_size + 1];

    for (int i = 0; i < proc_size; i++) {
        sendcounts[i] = recvcounts[i] = 0;
    }

    for (int i = 0; i < proc_size; i++) {
        uint64_t low = div_idx_map[i];
        uint64_t high = div_idx_map[i + 1];
        // on overlapping
        if (low >= word_off + word_size || high <= word_off) sendcounts[i] = 0;
        // contain
        else if (low >= word_off && high <= word_off + word_size) {
            //sendcounts[i] = high - low;
            for (uint64_t kk = low; kk < high; kk++) {
                sendcounts[i] += unique_words[kk - word_off].size() + 1;
            }
            // partial overlapping
        }
        else if (high <= word_off + word_size && high >= word_off
                 && low <= word_off) {
            //sendcounts[i] = high - word_off;
            for (uint64_t kk = word_off; kk < high; kk++) {
                sendcounts[i] += unique_words[kk - word_off].size() + 1;
            }
            // partial overlapping
        }
        else if (low >= word_off && low <= word_off + word_size
                 && high >= word_off + word_size) {
            //sendcounts[i] = div_off + div_size - low;
            for (uint64_t kk = low; kk < word_off + word_size; kk++) {
                sendcounts[i] += unique_words[kk - word_off].size() + 1;
            }
        }
        else if (low <= word_off && high >= word_off + word_size) {
            for (uint64_t kk = word_off; kk < word_off + word_size; kk++) {
                sendcounts[i] += unique_words[kk - word_off].size() + 1;
            }
            //sendcounts = div_size;
        }
        else {
            fprintf(stderr, "Error while computing the send counts!\n");
            exit(1);
        }
    }

    MPI_Alltoall(sendcounts, 1, MPI_INT, recvcounts, 1, MPI_INT,
                 MPI_COMM_WORLD);

    sdispls[0] = rdispls[0] = 0;
    int recvcount = 0, sendcount = 0;
    for (int i = 1; i < proc_size + 1; i++) {
        sdispls[i] = sdispls[i - 1] + sendcounts[i - 1];
        rdispls[i] = rdispls[i - 1] + recvcounts[i - 1];
        recvcount += recvcounts[i - 1];
        sendcount += sendcounts[i - 1];
    }

    sendbuf = new char[sendcount];
    recvbuf = new char[recvcount];

    int off = 0;
    for (auto iter : unique_words) {
        memcpy(sendbuf + off, iter.c_str(), iter.size() + 1);
        off += iter.size() + 1;
    }
    if (off != sendcount) {
        fprintf(stderr, "Prepare send buffer error!\n");
        exit(1);
    }

    MPI_Alltoallv(sendbuf, sendcounts, sdispls, MPI_CHAR, recvbuf, recvcounts,
                  rdispls, MPI_CHAR, MPI_COMM_WORLD);

    off = 0;
    while (off < recvcount) {
        //printf("%d[%d] alltoallv end, off=%d, str=%s\n",
        //       proc_rank, proc_size, off, recvbuf + off);

        unique_new_words.push_back(recvbuf + off);
        off += strlen(recvbuf + off) + 1;
    }
    if (off != recvcount) {
        fprintf(stderr, "Prepare recv buffer error! recvcount=%d, %d\n",
                recvcount, off);
        exit(1);
    }

    delete[] recvbuf;

    delete[] sendbuf;

    unique_words.clear();
}
