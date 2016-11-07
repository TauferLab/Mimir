#include <stdio.h>
#include <stdlib.h>
#include "hashbucket.h"
#include "const.h"
#include "hash.h"
#include "memory.h"

using namespace MIMIR_NS;


int CombinerHashBucket::compare(char *key, int keybytes, CombinerUnique *u){
    char *kvbuf = u->kv;
    char *ukey, *uvalue;
    int  ukeybytes, uvaluebytes, kvsize;

    printf("comprare: key=%s, ptr=%p, kv=%p\n", key, u, kvbuf);
 
    GET_KV_VARS(kv->kvtype,kvbuf,ukey,ukeybytes,uvalue,uvaluebytes,kvsize,kv);
    if(keybytes==ukeybytes && memcmp(key, ukey, keybytes)==0)
        return 1;

    return 0;
}

int CombinerHashBucket::getkey(CombinerUnique *u, char **pkey, int *pkeybytes){

    char *kvbuf = u->kv;
    char *ukey, *uvalue;
    int  ukeybytes, uvaluebytes, kvsize;
    
    GET_KV_VARS(kv->kvtype,kvbuf,ukey,ukeybytes,uvalue,uvaluebytes,kvsize,kv);
    *pkey=ukey;
    *pkeybytes=ukeybytes;

    return 0; 
}

