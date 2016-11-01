#ifndef HASH_BUCKET_H
#define HASH_BUCKET_H

namespace MIMIR_NS {

template<class ElemType>
class HashBucket{
public:
    HashBucket();
    ~HashBucket();

    // Comapre key with elem
    int compare(char *key, int keybytes, ElemType &);

    // Find the elem
    ElemType* findElem(char *key, int keybytes);

    ElemType* insertElem(ElemType &);

private:
    ElemType *buckets;
};

}

#endif
