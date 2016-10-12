#ifndef CONSTANT_H
#define CONSTANT_H

//#define ALIGNKV 4
//#define ALIGNK  ALIGNKV
//#define ALIGNV  ALIGNKV
//#define ALIGNT  ALIGNKV

//#define INTMAX 0x7FFFFFFF

#define NIN(A,B) ((A) < (B)) ? (A) : (B)
#define MAX(A,B) ((A) > (B)) ? (A) : (B)

#define CACHELINE_SIZE   64
#define MEMPAGE_SIZE   4096

//#define ROUNDUP(A,B) (char *) (((uint64_t) A + B) & ~B);

// general type
#define GET_KV_VARS(kvtype, kvbuf,key,keybytes,value,valuebytes,kvsize,data) \
{\
  if(kvtype==0){\
    char *kvbuf_start=kvbuf;\
    keybytes = *(int*)(kvbuf);\
    valuebytes = *(int*)(kvbuf+oneintlen);\
    kvbuf += twointlen;\
    key = kvbuf;\
    kvbuf += keybytes;\
    value = kvbuf;\
    kvbuf += valuebytes;\
    kvsize=(int)(kvbuf-kvbuf_start);\
  }else if(kvtype==1){\
    char *kvbuf_start=kvbuf;\
    key = kvbuf; \
    keybytes = (int)strlen(key)+1;\
    kvbuf += keybytes;\
    value = kvbuf;\
    valuebytes = (int)strlen(value)+1;\
    kvbuf += valuebytes;\
    kvsize=(int)(kvbuf-kvbuf_start);\
  }else if(kvtype==2){\
    char *kvbuf_start=kvbuf;\
    keybytes = data->ksize;\
    valuebytes = data->vsize;\
    key = kvbuf;\
    kvbuf += keybytes;\
    value = kvbuf;\
    kvbuf += valuebytes;\
    kvsize=(int)(kvbuf-kvbuf_start);\
  }else if(kvtype==3){\
    char *kvbuf_start=kvbuf;\
    key = kvbuf;\
    keybytes = (int)strlen(key)+1;\
    kvbuf += keybytes;\
    valuebytes = data->vsize;\
    value = kvbuf;\
    kvbuf += valuebytes;\
    kvsize=(int)(kvbuf-kvbuf_start);\
  }else LOG_ERROR("Undefined type %d!", kvtype);\
}

#define GET_KV_SIZE(kvtype, keybytes, valuebytes, kvsize) \
{\
  char *buf=0;\
  char *buf_start=0;\
  if(kvtype==0){\
    buf+=twointlen;\
    buf+=keybytes;\
    buf+=valuebytes;\
  }else if(kvtype==1 || kvtype==2){\
    buf+=keybytes;\
    buf+=valuebytes;\
  }else if(kvtype==3){\
    buf+=keybytes;\
    buf+=valuebytes;\
  }\
  else LOG_ERROR("Undefined KV type %d!\n", kvtype);\
  kvsize=(int)(buf-buf_start);\
}

#define PUT_KV_VARS(kvtype,kvbuf,key,keybytes,value,valuebytes,kvsize) \
{\
  char *kvbuf_start=kvbuf;\
  if(kvtype==0){\
    *(int*)kvbuf=keybytes;\
    kvbuf+=oneintlen;\
    *(int*)kvbuf=valuebytes;\
    kvbuf+=oneintlen;\
    memcpy(kvbuf, key, keybytes);\
    kvbuf += keybytes;\
    memcpy(kvbuf, value, valuebytes);\
    kvbuf += valuebytes;\
  }else if(kvtype==1 || kvtype==2){\
    memcpy(kvbuf, key, keybytes);\
    kvbuf += keybytes;\
    memcpy(kvbuf, value, valuebytes);\
    kvbuf += valuebytes;\
  }else if(kvtype==3){\
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
  if(kmvtype==0){\
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
  }else if(kmvtype==1 || kmvtype==2 || kmvtype==3){\
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
  }else LOG_ERROR("Undefined KV type %d!\n", kmvtype);\
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
