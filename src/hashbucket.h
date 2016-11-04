#ifndef HASH_BUCKET_H
#define HASH_BUCKET_H

#include "keyvalue.h"

namespace MIMIR_NS {

template<class ElemType>
class HashBucket{
public:
    HashBucket(KeyValue *);
    virtual ~HashBucket();

    // Comapre key with elem
    virtual int compare(char *key, int keybytes, ElemType *)=0;

    virtual int getkey(ElemType *, char **pkey, int *pkeybytes)=0;

    ElemType* findElem(char *key, int keybytes);
    ElemType* insertElem(ElemType *);

protected:
    int        nbucket;
    ElemType **buckets;
    
    int     maxbuf;
    int     usize, nbuf;
    char  **buffers;

    KeyValue *kv;
};

class CombinerUnique{
public:
    char *kv;
    CombinerUnique *next;
};

class CombinerHashBucket : public HashBucket<CombinerUnique>{
public:
    CombinerHashBucket(KeyValue *);
    // Comapre key with elem
    virtual int compare(char *key, int keybytes, CombinerUnique *);
    virtual int getkey(CombinerUnique *, char **pkey, int *pkeybytes);
};

}

#endif
