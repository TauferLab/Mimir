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
  KeyValue(int a1=0, int a2=1, int a3=1, int a4=1, int a5=0, 
    std::string a6=std::string(""));
  ~KeyValue();

  int getNextKV(int, int, char **, int &, char **, int &);
};

}

#endif
