#ifndef CONSTANT_H
#define CONSTANT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NIN(A,B) ((A) < (B)) ? (A) : (B)
#define MAX(A,B) ((A) > (B)) ? (A) : (B)

#define MEMPAGE_SIZE               4096

#define MAXLINE                    2048
#define MAX_COMM_SIZE        0x40000000

/// KV Type
enum KVType{
    KVGeneral=-2, // variable length bytes
    KVString,     // string
    KVFixed};     // fixed-size KV

enum ElemType{
    StringType,
    Int32Type,
    Int64Type};

// Get KV information
#define GET_KV_VARS(ksize,vsize,buf,key,keybytes,value,valuebytes,kvsize) \
{\
    char *kvbuf_start=buf;\
    char *kvbuf=buf;\
    if(ksize==KVGeneral){\
        keybytes = *(int*)(kvbuf);\
        kvbuf += oneintlen;\
    }\
    if(vsize==KVGeneral){\
        valuebytes = *(int*)(kvbuf);\
        kvbuf += oneintlen;\
    }\
    key=kvbuf;\
    if(ksize==KVString){\
        keybytes = (int)strlen(key)+1;\
    }else if(ksize!=KVGeneral){\
        keybytes = ksize;\
    }\
    kvbuf+=keybytes;\
    value=kvbuf;\
    if(vsize==KVString){\
        valuebytes = (int)strlen(value)+1;\
    }else if(vsize!=KVGeneral){\
        valuebytes = vsize;\
    }\
    kvbuf+=valuebytes;\
    kvsize=(int)(kvbuf-kvbuf_start);\
}

#define GET_KV_SIZE(ksize, vsize, keybytes, valuebytes, kvsize) \
{\
    int end=0;\
    int start=0;\
    if(ksize==KVGeneral) end+=oneintlen;\
    if(vsize==KVGeneral) end+=oneintlen;\
    end+=keybytes;\
    end+=valuebytes;\
    kvsize=end-start;\
}

#define PUT_KV_VARS(ksize,vsize,buf,key,keybytes,value,valuebytes,kvsize) \
{\
    char *kvbuf_start=buf;\
    char *kvbuf=buf;\
    if(ksize==KVGeneral){\
        *(int*)kvbuf=keybytes;\
        kvbuf+=oneintlen;\
    }\
    if(vsize==KVGeneral){\
        *(int*)kvbuf=valuebytes;\
        kvbuf+=oneintlen;\
    }\
    memcpy(kvbuf, key, keybytes);\
    kvbuf+=keybytes;\
    memcpy(kvbuf, value, valuebytes);\
    kvbuf+=valuebytes;\
    kvsize=(int)(kvbuf-kvbuf_start);\
}


extern int oneintlen;
extern int twointlen;
extern int threeintlen;

extern int oneptrlen;

extern int kalignm;
extern int valignm;
extern int talignm;

#endif
