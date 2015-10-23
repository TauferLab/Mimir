#include "keyvalue.h"

using namespace MAPREDUCE_NS;

KeyValue::KeyValue():DataObject(1024*1024, 8){
  ;
}

KeyValue::~KeyValue(){
}

int KeyValue::add(int blockid, char *key, int keybytes, char *value, int valuebytes){
  return -1;
} 



