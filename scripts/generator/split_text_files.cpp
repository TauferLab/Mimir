#include <stdio.h>
#include <stdlib.h>

#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#include <omp.h>

#include <vector>
#include <string>

#define MAXLINE 2048
//#define CHUNK_UNIT (1024*1024)
#define CHUNK_UNIT 1

// get file list
void get_file_list(const char *filepath,int recurse);

std::vector<std::string> ifiles;

int main(int argc, char *argv[]){
  // get argument of program
  if(argc < 7){
    printf("Usage: ./split inputpath outputfile outprefix filesize(MB) startidx maxnfile\n");
    exit(1);
  }
  char *filepath=argv[1];
  char *outpath=argv[2];
  char *outprefix=argv[3];
  //int64_t chunksize=atoi(argv[4]);
  int64_t chunksize = strtoll(argv[4], NULL, 10);
  int startidx=atoi(argv[5]);
  int maxnfile=atoi(argv[6]);

  int64_t maxfilesize=(int64_t)chunksize*(CHUNK_UNIT);
  char *buf=(char*)malloc(maxfilesize+1);

  // get filelist
  get_file_list(filepath, 1);
  int fcount = ifiles.size();
  int fid = 0;

#pragma omp parallel shared(fid) shared(fcount)
{
  int tid = omp_get_thread_num();
  int out_fd=-1;
  int end_flag=0;

  // thread tmp file
  char tfilename[100];
  sprintf(tfilename, "%s/t%d.tmp", outpath, tid);

  // open output file
  int64_t outfilesize=0;
  out_fd=creat(tfilename, S_IRUSR|S_IWUSR);

  //printf("tfilename=%s\n", tfilename);
 
#pragma omp for
  for(int i = 0; i < fcount; i++){

    // handle a singe input file
    struct stat stbuf;
    stat(ifiles[i].c_str(), &stbuf);
    int64_t fsize = stbuf.st_size;
   
    printf("thread %d filename=%s, filesize=%ld\n", tid, ifiles[i].c_str(), fsize);

    if(fsize == 0 || end_flag == 1) continue;
    
    int64_t foff=0;
    int in_fd=open(ifiles[i].c_str(), O_RDONLY);

    //int counter=0;
    while(foff < fsize){
      // get read size
      int64_t readsize=0;
      if(fsize-foff<=maxfilesize-outfilesize) readsize=fsize-foff;
      else readsize=maxfilesize-outfilesize;

      //printf("readsize=%ld, fsize=%ld, foff=%ld, maxfsize=%ld, outfsize=%ld\n", \
        readsize, fsize, foff, maxfilesize, outfilesize);
 
      // read file
      lseek(in_fd, foff, SEEK_SET);
      int64_t read_bytes = read(in_fd, buf, readsize);
      if(read_bytes<readsize){
        fprintf(stderr, "Read bytes %ld less than read size %ld\n", read_bytes, readsize);
        exit(1);
      }

      // this is the last chunk of this file
      if(readsize==fsize-foff && outfilesize+readsize<maxfilesize){
        // seperate words in different files
        if(buf[readsize-1] != '\n' && buf[readsize-1] != ' '){
          buf[readsize] = '\n';
          readsize++;
        }
      // this is not the last chunk of this file
      }else if(readsize<fsize-foff){
        do{
          readsize--;
        }while(readsize>0 && buf[readsize-1] != '\n' && buf[readsize-1] != ' ');
      }

      // we need write some data into output file
      if(readsize != 0){
        write(out_fd, buf, readsize);
        foff+=readsize;
        outfilesize+=readsize;  
      }   

      //printf("readsize=%ld, foff=%ld, outfilesize=%ld\n", readsize, foff, outfilesize);

      // change output file
      if(readsize==0 || outfilesize==maxfilesize){
        close(out_fd);
        char tmp[100];
        int i=__sync_fetch_and_add(&fid,1);
        if(i<maxnfile){
          sprintf(tmp, "%s/%s.%d.txt", outpath, outprefix, startidx+i);
          rename(tfilename, tmp);
          outfilesize=0;
          out_fd=creat(tfilename, S_IRUSR|S_IWUSR);
        }else{
          //close(in_fd);
          end_flag=1;
          //goto end;
          break;
        }
      }
      //counter++;
      //if(counter>=2) exit(1);
    }
    close(in_fd);
    //if(end_flag) break;
  } 
  close(out_fd);
}

  free(buf);
 
  return 0;
}

// get input file list
void get_file_list(const char *filepath,int recurse){
  struct stat inpath_stat;
  int err = stat(filepath, &inpath_stat);
  if(err) fprintf(stderr, "Error in get input files, err=%d\n", err);
    
  // regular file
  if(S_ISREG(inpath_stat.st_mode)){
    ifiles.push_back(std::string(filepath));
  // dir
  }else if(S_ISDIR(inpath_stat.st_mode)){
    struct dirent *ep;
    DIR *dp = opendir(filepath);
    if(!dp) fprintf(stderr, "Error in get input files\n");
      
    while(ep = readdir(dp)){
      if(ep->d_name[0] == '.') continue;
       
      char newstr[MAXLINE]; 
      sprintf(newstr, "%s/%s", filepath, ep->d_name);
      err = stat(newstr, &inpath_stat);
      if(err) fprintf(stderr, "Error in get input files, err=%d\n", err);
        
      // regular file
      if(S_ISREG(inpath_stat.st_mode)){
        ifiles.push_back(std::string(newstr));
      // dir
      }else if(S_ISDIR(inpath_stat.st_mode) && recurse){
        get_file_list(newstr, recurse);
      }
    }
  }
}
