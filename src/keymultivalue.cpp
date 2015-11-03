#include "keymultivalue.h"

#include "log.h"

using namespace MAPREDUCE_NS;

KeyMultiValue::KeyMultiValue(
  int _kmvtype,
  int blocksize, 
  int maxblock,
  int maxmemsize,
  int outofcore,
  std::string filename):
  DataObject(KMVType, blocksize, 
    maxblock, maxmemsize, outofcore, filename){
  kmvtype = _kmvtype;

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
  
  keybytes = *(int*)buf;
  buf += sizeof(int);
  *key = buf;
  buf += keybytes;
  nvalue = *(int*)buf;
  buf += sizeof(int);
  *valuebytes = (int*)buf;
  buf += nvalue*sizeof(int);
  *values = buf;

  offset += sizeof(int)+keybytes+sizeof(int)*(nvalue+1);

  int *valsize = *valuebytes;
  for(int i = 0; i < nvalue; i++) offset += valsize[i];

  return offset;
}

void KeyMultiValue::print(int type, FILE *fp, int format){
  char *key, *values;
  int keybytes, nvalue, *valuebytes;

  fprintf(fp, "KMV Object:\n");

  for(int i = 0; i < nblock; i++){
    int offset = 0;

    acquireblock(i);

    offset = getNextKMV(i, offset, &key, keybytes, nvalue, &values, &valuebytes);

    while(offset != -1){

      if(type==0) fprintf(fp, "%s", key);
      else if(type==1) fprintf(fp, "%d", *(int*)key);
      else if(type==2) fprintf(fp, "%ld", *(int64_t*)key);
      else LOG_ERROR("%s", "Error undefined output type\n");

      for(int j = 0; j < nvalue; j++){
        if(valuebytes != 0){ 
          if(type==0) fprintf(fp, ",%s", values);
          else if(type==1) fprintf(fp, ",%d", *(int*)values);
          else if(type==2) fprintf(fp, ",%ld", *(int64_t*)values);
          else LOG_ERROR("%s", "Error undefined output type\n");
        }
        values += valuebytes[j];
      }
      fprintf(fp, "\n");
      offset = getNextKMV(i, offset, &key, keybytes, nvalue,  &values, &valuebytes);
    }

    releaseblock(i);

  }

}
