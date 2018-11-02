/*
 * (c) 2017 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */

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

#include "mpi.h"

using namespace std;

int main(int argc, char *argv[])
{
    int ret = 0;
    int worldsize = 0, worldrank = 0;

    double stdiv = 0.0;
    int start_idx = 0, num_nodes = 0, num_clusters = 0;

    int index = 7;
    int num_digit = 15, range_min = -4, range_max = 4;

    vector<vector<double> > cluster_centers;
    vector<int> points_num;
    vector<string> distr;

    string key_file_prefix;
    string outdir;
    char path[256];

    if (argc < 10) {
        printf("Usage: %s prefix outdir start_idx num_nodes stdiv num_clsters \n", argv[0]);
        return 0;
    }

    std::random_device rd;
    std::minstd_rand gen(rd());

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &worldsize);
    MPI_Comm_rank(MPI_COMM_WORLD, &worldrank);

    key_file_prefix = string(argv[1]);
    outdir = string(argv[2]);
    start_idx = atoi(argv[3]);
    num_nodes = atoi(argv[4]);
    stdiv = atof(argv[5]);
    num_clusters = atoi(argv[6]);

    printf("The file prefix is: %s\n", key_file_prefix.c_str());
    printf("The number of cluster is: %d\n", num_clusters);

    for (int i = 0; i < num_clusters; i++) {
        vector<double> center;
        center.push_back(atof(argv[index  ]));
        center.push_back(atof(argv[index+1]));
        center.push_back(atof(argv[index+2]));
        cluster_centers.push_back(center);
        points_num.push_back(atoi(argv[index+3]));
        distr.push_back(string(argv[index+4]));
        index += 5;
    }

    // cout << "the cluster centers: " << cluster_centers << endl;
    // cout << "the points number: " <<points_num << endl;
    // cout << "the distribution: " << distr << endl;

    for (vector<vector<double> >::iterator cluster = cluster_centers.begin();
         cluster != cluster_centers.end(); cluster++) {
        int num_point_incluster = points_num.front();
        string dist = distr.front();

        if (dist == "normal") {
            // Assume normal dist
            std::normal_distribution<> dx((*cluster)[0], stdiv);
            std::normal_distribution<> dy((*cluster)[1], stdiv);
            std::normal_distribution<> dz((*cluster)[2], stdiv);

            int point_count = 0;
            char filename[128];
            memset(filename, 0, 128);
            sprintf(filename, "%s/%s.points.%d.txt", outdir.c_str(), key_file_prefix.c_str(), worldrank);
            ofstream out_file(filename, ios::out);
            char points_coords[128];
            for (int point_count = 0; point_count < num_point_incluster; point_count++) {
                memset(points_coords, 0, 128);
                sprintf(points_coords, " %.17f %.17f %.17f\n", dx(gen), dy(gen), dz(gen));
                out_file << points_coords;

            }
        } else if (dist == "uniform") {
            // Assume uniform dist
            std::uniform_real_distribution<> dx(range_min, range_max);
            std::uniform_real_distribution<> dy(range_min, range_max);
            std::uniform_real_distribution<> dz(range_min, range_max);

            int point_count = 0;
            char filename[128];
            memset(filename, 0, 128);
            sprintf(filename, "%s/%s.points.%d.txt", outdir.c_str(), key_file_prefix.c_str(), worldrank);
            ofstream out_file(filename, ios::out);
            char points_coords[128];
            for (int point_count = 0; point_count < num_point_incluster; point_count++) {
                memset(points_coords, 0, 128);
                sprintf(points_coords, " %.17f %.17f %.17f\n", dx(gen), dy(gen), dz(gen));
                out_file << points_coords;

            }
        }

    }

    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Finalize();
    return 0;
}
