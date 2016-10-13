#include <math.h>
#include <omp.h>

#include "keymultivalue.h"

#include "log.h"

#include "const.h"

using namespace MAPREDUCE_NS;

KeyMultiValue::KeyMultiValue(
  int _kmvtype,
  int blocksize,
  int maxblock,
  int maxmemsize,
  int outofcore,
  std::string filename,
  int threadsafe):
  DataObject(KMVType, blocksize,
    maxblock, maxmemsize, outofcore, filename, threadsafe){
  kmvtype = _kmvtype;

  ksize = vsize = 0;

  LOG_PRINT(DBG_DATA, "%s", "DATA: KMV create.\n");
}


KeyMultiValue::~KeyMultiValue(){
  LOG_PRINT(DBG_DATA, "%s", "DATA: KMV destroy.\n");
}

int KeyMultiValue::getNextKMV(int blockid, int offset, char **key, int &keybytes,
  int &nvalue, char **values, int **valuebytes){

  if(offset >= blocks[blockid].datasize) return -1;

  int bufferid = blocks[blockid].bufferid;
  char *buf = buffers[bufferid].buf + offset;

  int mvbytes=0,kmvsize=0;
  GET_KMV_VARS(kmvtype,buf,*key,keybytes,nvalue,*values,*valuebytes,mvbytes,kmvsize,this);
  offset += kmvsize;

  return offset;
}

#if 0
int KeyMultiValue::addKMV(int blockid,char *key,int &keysize, char *val, int &nval, int &valbytes, int *valuesizes){
  int kmvbytes = 0;

  //int valbytes = 0;
  //for(int i = 0; i < nval; i++) valbytes += valuesizes[i];

  if(kmvtype == 0) kmvbytes = sizeof(int)+keysize+(nval+1)*sizeof(int)+valbytes;
  else if(kmvtype == 1) kmvbytes = keysize + sizeof(int) + valbytes;
  else if(kmvtype == 2) kmvbytes = keysize + sizeof(int) + valbytes;
  else if(kmvtype == 3) kmvbytes = keysize + sizeof(int);
  else LOG_ERROR("Error: undefined KMV type %d.\n", kmvtype);

  if(kmvbytes > blocksize){
    LOG_ERROR("Error: KMV size is larger than block size. (KMV size=%d, block size=%d)\n", kmvbytes, blocksize);
  }

  int datasize = blocks[blockid].datasize;
  if(kmvbytes+datasize > blocksize) return -1;

  int bufferid = blocks[blockid].bufferid;
  char *buf = buffers[bufferid].buf;

  if(kmvtype == 0){
    memcpy(buf+datasize, &keysize, sizeof(int));
    datasize += sizeof(int);
    memcpy(buf+datasize, key, keysize);
    datasize += keysize;
    memcpy(buf+datasize, &nval, sizeof(int));
    datasize += sizeof(int);
    memcpy(buf+datasize, valuesizes, nval*sizeof(int));
    datasize += nval*sizeof(int);
    memcpy(buf+datasize, val, valbytes);
    datasize += valbytes;
  }else if(kmvtype == 1){
    memcpy(buf+datasize, key, keysize);
    datasize += keysize;
    memcpy(buf+datasize, &nval, sizeof(int));
    datasize += sizeof(int);
    memcpy(buf+datasize, val, valbytes);
    datasize += valbytes;
  }else if(kmvtype == 2){
    memcpy(buf+datasize, key, keysize);
    datasize += keysize;
    memcpy(buf+datasize, &nval, sizeof(int));
    datasize += sizeof(int);
    memcpy(buf+datasize, val, valbytes);
    datasize += valbytes;
  }else if(kmvtype == 3){
    memcpy(buf+datasize, key, keysize);
    datasize += keysize;
    memcpy(buf+datasize, &nval, sizeof(int));
    datasize += sizeof(int);
  }else LOG_ERROR("Error undefined KMV type %d.\n", kmvtype);

  blocks[blockid].datasize = datasize;
  return 0;
}
#endif

void KeyMultiValue::print(int type, FILE *fp, int format){
  char *key, *values;
  int keybytes, nvalue, *valuebytes;

  fprintf(fp, "KMV Object:\n");

  for(int i = 0; i < nblock; i++){
    int offset = 0;

    acquire_block(i);

    offset = getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);

    while(offset != -1){

      if(type==0) fprintf(fp, "%s", key);
      else if(type==1) fprintf(fp, "%d", *(int*)key);
      else if(type==2) fprintf(fp, "%ld", *(int64_t*)key);
      else LOG_ERROR("%s", "Error undefined output type\n");

      if(kmvtype==0){
        for(int j = 0; j < nvalue; j++){
          if(valuebytes != 0){
            if(type==0) fprintf(fp, ",%s", values);
            else if(type==1) fprintf(fp, ",%d", *(int*)values);
            else if(type==2) fprintf(fp, ",%ld", *(int64_t*)values);
            else LOG_ERROR("%s", "Error undefined output type\n");
          }
          values += valuebytes[j];
        }
      }else if(kmvtype==1){
        for(int j = 0; j < nvalue; j++){
            if(type==0) fprintf(fp, ",%s", values);
            else if(type==1) fprintf(fp, ",%d", *(int*)values);
            else if(type==2) fprintf(fp, ",%ld", *(int64_t*)values);
            else LOG_ERROR("%s", "Error undefined output type\n");
            values += (strlen(values)+1);
        }
      }else if(kmvtype==2){
        for(int j = 0; j < nvalue; j++){
            if(type==0) fprintf(fp, ",%s", values);
            else if(type==1) fprintf(fp, ",%d", *(int*)values);
            else if(type==2) fprintf(fp, ",%ld", *(int64_t*)values);
            else LOG_ERROR("%s", "Error undefined output type\n");
            values += vsize;
        }
      }else if(kmvtype==3){
      }

      fprintf(fp, "\n");
      offset = getNextKMV(i, offset, &key, keybytes, nvalue,  &values, &valuebytes);
    }

    release_block(i);

  }

}
