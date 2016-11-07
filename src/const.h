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
    GeneralKV,
    StringKV,
    FixedKV,
    StringKFixedV,
    FixedKStringV,
    GeneralKStringV,
    GeneralKFixedV,
    StringKGeneralV,
    FixedKGeneralV};

enum ElemType{
    StringType,
    Int32Type,
    Int64Type};

// general type
#define GET_KV_VARS(kvtype,kvbuf,key,keybytes,value,valuebytes,kvsize,data) \
{\
    /* <general, general> */\
    if(kvtype==GeneralKV){\
        char *kvbuf_start=kvbuf;\
        keybytes = *(int*)(kvbuf);\
        valuebytes = *(int*)(kvbuf+oneintlen);\
        kvbuf += twointlen;\
        key = kvbuf;\
        kvbuf += keybytes;\
        value = kvbuf;\
        kvbuf += valuebytes;\
        kvsize=(int)(kvbuf-kvbuf_start);\
    /* <string, string> */\
    }else if(kvtype==StringKV){\
        char *kvbuf_start=kvbuf;\
        key = kvbuf; \
        keybytes = (int)strlen(key)+1;\
        kvbuf += keybytes;\
        value = kvbuf;\
        valuebytes = (int)strlen(value)+1;\
        kvbuf += valuebytes;\
        kvsize=(int)(kvbuf-kvbuf_start);\
    /* <fixed, fixed> */\
    }else if(kvtype==FixedKV){\
        char *kvbuf_start=kvbuf;\
        keybytes = data->ksize;\
        valuebytes = data->vsize;\
        key = kvbuf;\
        kvbuf += keybytes;\
        value = kvbuf;\
        kvbuf += valuebytes;\
        kvsize=(int)(kvbuf-kvbuf_start);\
    /* <string,fixed> */\
    }else if(kvtype==StringKFixedV){\
        char *kvbuf_start=kvbuf;\
        key = kvbuf;\
        keybytes = (int)strlen(key)+1;\
        kvbuf += keybytes;\
        valuebytes = data->vsize;\
        value = kvbuf;\
        kvbuf += valuebytes;\
        kvsize=(int)(kvbuf-kvbuf_start);\
    /* <fixed,string> */\
    }else if(kvtype==FixedKStringV){\
        char *kvbuf_start=kvbuf;\
        key = kvbuf;\
        keybytes = data->ksize;\
        kvbuf += keybytes;\
        value = kvbuf;\
        valuebytes = (int)strlen(value)+1;\
        kvbuf += valuebytes;\
        kvsize=(int)(kvbuf-kvbuf_start);\
    /* <general,string> */\
    }else if(kvtype==GeneralKStringV){\
        char *kvbuf_start=kvbuf;\
        keybytes = *(int*)(kvbuf);\
        kvbuf += oneintlen;\
        key = kvbuf;\
        kvbuf += keybytes;\
        value = kvbuf;\
        valuebytes = (int)strlen(value)+1;\
        kvbuf += valuebytes;\
        kvsize=(int)(kvbuf-kvbuf_start);\
    /* <general,fixed> */\
    }else if(kvtype==GeneralKFixedV){\
        char *kvbuf_start=kvbuf;\
        keybytes = *(int*)(kvbuf);\
        kvbuf += oneintlen;\
        key = kvbuf;\
        kvbuf += keybytes;\
        value = kvbuf;\
        valuebytes = data->vsize;\
        kvbuf += valuebytes;\
        kvsize=(int)(kvbuf-kvbuf_start);\
    }else if(kvtype==StringKGeneralV){\
        char *kvbuf_start=kvbuf;\
        valuebytes = *(int*)(kvbuf);\
        kvbuf += oneintlen;\
        key = kvbuf;\
        keybytes = (int)strlen(key)+1;\
        kvbuf += keybytes;\
        value = kvbuf;\
        kvbuf += valuebytes;\
        kvsize=(int)(kvbuf-kvbuf_start);\
    }else if(kvtype==FixedKGeneralV){\
        char *kvbuf_start=kvbuf;\
        valuebytes = *(int*)(kvbuf);\
        kvbuf += oneintlen;\
        key = kvbuf;\
        keybytes = data->ksize;\
        kvbuf += keybytes;\
        value = kvbuf;\
        kvbuf += valuebytes;\
        kvsize=(int)(kvbuf-kvbuf_start);\
    }\
}

#define GET_KV_SIZE(kvtype, keybytes, valuebytes, kvsize) \
{\
    char *buf=0;\
    char *buf_start=0;\
    if(kvtype==GeneralKV){\
        buf+=twointlen;\
        buf+=keybytes;\
        buf+=valuebytes;\
    }else if(kvtype==StringKV || kvtype==FixedKV || \
        kvtype == StringKFixedV || kvtype==FixedKStringV){\
        buf+=keybytes;\
        buf+=valuebytes;\
    }else if(kvtype==GeneralKStringV || kvtype==GeneralKFixedV || \
        kvtype==StringKGeneralV || kvtype==FixedKGeneralV){\
        buf+=oneintlen;\
        buf+=keybytes;\
        buf+=valuebytes;\
    }\
    kvsize=(int)(buf-buf_start);\
}

#define PUT_KV_VARS(kvtype,kvbuf,key,keybytes,value,valuebytes,kvsize) \
{\
    char *kvbuf_start=kvbuf;\
    if(kvtype==GeneralKV){\
        *(int*)kvbuf=keybytes;\
        kvbuf+=oneintlen;\
        *(int*)kvbuf=valuebytes;\
        kvbuf+=oneintlen;\
        memcpy(kvbuf, key, keybytes);\
        kvbuf += keybytes;\
        memcpy(kvbuf, value, valuebytes);\
        kvbuf += valuebytes;\
    }else if(kvtype==StringKV || kvtype==FixedKV ||\
        kvtype==StringKFixedV || kvtype==FixedKStringV){\
        memcpy(kvbuf, key, keybytes);\
        kvbuf += keybytes;\
        memcpy(kvbuf, value, valuebytes);\
        kvbuf += valuebytes;\
    }else if(kvtype==GeneralKStringV || kvtype==GeneralKFixedV){\
        *(int*)kvbuf=keybytes;\
        kvbuf+=oneintlen;\
        memcpy(kvbuf, key, keybytes);\
        kvbuf += keybytes;\
        memcpy(kvbuf, value, valuebytes);\
        kvbuf += valuebytes;\
    }else if(kvtype==StringKGeneralV || kvtype==FixedKGeneralV){\
        *(int*)kvbuf=valuebytes;\
        kvbuf+=oneintlen;\
        memcpy(kvbuf, key, keybytes);\
        kvbuf += keybytes;\
        memcpy(kvbuf, value, valuebytes);\
        kvbuf += valuebytes;\
    }\
    kvsize=(int)(kvbuf-kvbuf_start);\
}

#define GET_KMV_VARS(kmvtype,kvbuf,key,keybytes,nvalue,values,valuebytes,mvbytes,kmvsize,data) \
{\
    char *kvbuf_start=kvbuf;\
    if(kmvtype==GeneralKV || kmvtype==StringKGeneralV || kmvtype==FixedKGeneralV){\
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
        kmvtype==StringKFixedV || kmvtype==FixedKStringV \
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
