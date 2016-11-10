#ifndef CONSTANT_H
#define CONSTANT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

//#define ALIGNKV 4
//#define ALIGNK  ALIGNKV
//#define ALIGNV  ALIGNKV
//#define ALIGNT  ALIGNKV

//#define INTMAX 0x7FFFFFFF

//extern int me,nprocs,tnum;

#define NIN(A,B) ((A) < (B)) ? (A) : (B)
#define MAX(A,B) ((A) > (B)) ? (A) : (B)

//#define CACHELINE_SIZE               64
#define MEMPAGE_SIZE               4096

#define MAXLINE                    2048
#define MAX_COMM_SIZE        0x40000000

//#define ROUNDUP(A,B) (char *) (((uint64_t) A + B) & ~B);

/// KVType represents KV Type
enum KVType{
    KVGeneral=-2,
    KVString,
    KVFixed};

enum ElemType{
    StringType,
    Int32Type,
    Int64Type};

// general type
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
    kvsize=kvbuf-kvbuf_start;\
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

#if 0
#define GET_KMV_VARS(kmvtype,kvbuf,key,keybytes,nvalue,values,valuebytes,mvbytes,kmvsize,data) \
{\
    char *kvbuf_start=kvbuf;\
    if(kmvtype==GeneralKV || \
kmvtype==StringKGeneralV || \
kmvtype==FixedKGeneralV){\
        keybytes=*(int*)kvbuf;\
        kvbuf+=oneintlen;\
        mvbytes=*(int*)kvbuf;\
        kvbuf+=oneintlen;\
        nvalue=*(int*)kvbuf;\
        kvbuf+=oneintlen;\
        valuebytes=(int*)kvbuf;\
        kvbuf+=nvalue*oneintlen;\
        key=kvbuf;\
        kvbuf+=keybytes;\
        values=kvbuf;\
        kvbuf+=mvbytes;\
    }else if(kmvtype==StringKV || kmvtype==FixedKV || \
        kmvtype==StringKFixedV || kmvtype==FixedKStringV || \
        kmvtype==GeneralKStringV || kmvtype==GeneralKFixedV){\
        keybytes=*(int*)kvbuf;\
        kvbuf+=oneintlen;\
        mvbytes=*(int*)kvbuf;\
        kvbuf+=oneintlen;\
        nvalue=*(int*)kvbuf;\
        kvbuf+=oneintlen;\
        valuebytes=NULL;\
        key=kvbuf;\
        kvbuf+=keybytes;\
        values=kvbuf;\
        kvbuf+=mvbytes;\
    }\
    kmvsize=(int)(kvbuf-kvbuf_start);\
}
#endif

#if 0
  else if(kmvtype==3){\
    keybytes=*(int*)kvbuf;\
    kvbuf+=oneintlen;\
    mvbytes=*(int*)kvbuf;\
    kvbuf+=oneintlen;\
    nvalue=*(int*)kvbuf;\
    kvbuf+=oneintlen;\
    valuebytes=NULL;\
    key=kvbuf;\
    kvbuf+=keybytes;\
    values=NULL;\
  }
#endif

extern int oneintlen;
extern int twointlen;
extern int threeintlen;

extern int oneptrlen;

extern int kalignm;
extern int valignm;
extern int talignm;

#endif
