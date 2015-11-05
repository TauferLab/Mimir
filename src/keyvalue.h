#ifndef KEY_VALUE_H
#define KEY_VALUE_H

#include <stdio.h>
#include <stdlib.h>

#include "dataobject.h"

namespace MAPREDUCE_NS {


class KeyValue : public DataObject{
private:
  int kvtype; // 0 for string, 1 for binary, 2 for constant key, value
  int ksize, vsize;

public:
  KeyValue(int, 
    int blocksize=1, 
    int maxblock=4, 
    int memsize=4,
    int outofcore=0, 
    std::string a6=std::string(""));

  void setKVsize(int _ksize, int _vsize){
    ksize = _ksize;
    vsize = _vsize;
  }

  ~KeyValue();

  int getKVtype(){
    return kvtype;
  }

  int getNextKV(int, int, char **, int &, char **, int &, 
    int *kff=NULL, int *vff=NULL);

  int addKV(int, char *, int &, char *, int &);
  
   /* used for debug */
   void print(int type=0, FILE *fp=stdout, int format=0);
};

}

#endif
