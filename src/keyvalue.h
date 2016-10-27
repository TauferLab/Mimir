/**
 * @file   keyvalue.h
 * @Author Tao Gao (taogao@udel.edu)
 * @date   September 1st, 2016
 * @brief  This file includes <Key,Value> object.
 *
 *
 */
#ifndef KEY_VALUE_H
#define KEY_VALUE_H

#include <stdio.h>
#include <stdlib.h>

#include "dataobject.h"

namespace MIMIR_NS {


class KeyValue : public DataObject{
public:
  int kvtype; // 0 for string, 1 for binary, 2 for constant key, value

public:
  KeyValue(int,
    uint64_t blocksize=1,
    int maxblock=4,
    int memsize=4,
    int outofcore=0,
    std::string a6=std::string(""),
    int threadsafe=1);

  ~KeyValue();

  int getKVtype(){
    return kvtype;
  }

  int64_t getNextKV(int, int64_t, char **, int &, char **, int &,
    int *kff=NULL, int *vff=NULL);

  int addKV(int, char *, int &, char *, int &);

   /* used for debug */
   void print(int type=0, FILE *fp=stdout, int format=0);
};

}

#endif
