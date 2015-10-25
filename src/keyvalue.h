#ifndef KEY_VALUE_H
#define KEY_VALUE_H

#include <stdio.h>
#include <stdlib.h>

#include "dataobject.h"

namespace MAPREDUCE_NS {


class KeyValue : public DataObject{
private:
  int kvtype; // 0 for string, 1 for binary

public:
  KeyValue(int, 
    int blocksize=1, 
    int maxblock=1, 
    int memsize=1,
    int outofcore=0, 
    std::string a6=std::string(""));
  ~KeyValue();

  int getNextKV(int, int, char **, int &, char **, int &, 
    int *kff=NULL, int *vff=NULL);

  int addKV(int, char *, int &, char *, int &);
  
   /* used for debug */
   void print();
};

}

#endif
