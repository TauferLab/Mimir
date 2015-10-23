#ifndef KEY_VALUE_H
#define KEY_VALUE_H

#include <stdio.h>
#include <stdlib.h>

#include "dataobject.h"

namespace MAPREDUCE_NS {


class KeyValue : public DataObject{
public:
  KeyValue();
  ~KeyValue();

  int add(int, char *, int, char *, int);
};

}

#endif
