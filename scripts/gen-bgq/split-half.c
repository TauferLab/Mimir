#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "mpi.h"

int main(int argc, char *argv[])
{
    int ret = 0;
    int worldsize = 0, worldrank = 0;

    int lines = 0;

    char* key_file_prefix;
    char* outdir;
    char path[256];

    if (argc < 4) {
        printf("Insufficient params\n");
        return 0;
    }

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &worldsize);
    MPI_Comm_rank(MPI_COMM_WORLD, &worldrank);

    key_file_prefix = argv[1];
    outdir = argv[2];
    lines = atoi(argv[3]);

    char filename[128];
    char filename2[128];
    memset(filename, 0, 128);
    sprintf(filename, "%s/%s.%d.txt", outdir, key_file_prefix, worldrank);
    printf("filename is %s\n", filename);
    int fd = open(filename, O_RDONLY);
    int file_size = lseek(fd, 0, SEEK_END);
    close(fd);
    fd = open(filename, O_RDONLY);
    if (file_size == 0) {
        close(fd);
        remove(filename);
        MPI_Finalize();
        return 0;
    }
    char* buf = (char*) malloc(sizeof(char)*file_size);
    read(fd, buf, file_size);
    close(fd);
    remove(filename);

    printf("The file prefix is: %s\n", key_file_prefix);
    printf("split by: %d lines\n", lines);

    int is_end = 0;
    char* start = buf;
    char* match = start;
    int count = 0;
    int fcount = 0;
    for (int offset = 0; offset < file_size; ) {
        while (*match != '\n' && (match - buf) < file_size) {
            // printf("%c\n", *match);
            match++;
        }
        match += 1;
        count += 1;
        offset = match - buf;
        // printf("offset: %d count: %d\n", offset, count);
        if ((count >= lines && (count % lines) == 0) || offset >= file_size) {
            int writesize = match - start;
            sprintf(filename2, "%s.split.%d", filename, fcount);
            printf("write file: %s\n", filename2);
            int out_fd = open(filename2, O_CREAT | O_RDWR, 0644);
            write(out_fd, start, writesize);
            close(out_fd);
            start = match;
            fcount++;
        }
    }

    free(buf);

    MPI_Finalize();
    return 0;
}
