#ifndef CONSTANT_H
#define CONSTANT_H

#define ALIGNKV 4
#define ALIGNK  ALIGNKV
#define ALIGNV  ALIGNKV
#define ALIGNT  ALIGNKV

#define NIN(A,B) ((A) < (B)) ? (A) : (B)
#define MAX(A,B) ((A) > (B)) ? (A) : (B)

#define ROUNDUP(A,B) (char *) (((uint64_t) A + B) & ~B);

// general type
#define GET_KV_VARS(kvtype, kvbuf,key,keybytes,value,valuebytes,kvsize,data) \
{\
  if(kvtype==0){\
    char *kvbuf_start=kvbuf;\
    keybytes = *(int*)(kvbuf);\
    valuebytes = *(int*)(kvbuf+oneintlen);\
    kvbuf += twointlen;\
    kvbuf = ROUNDUP(kvbuf, kalignm);\
    key = kvbuf;\
    kvbuf += keybytes;\
    kvbuf = ROUNDUP(kvbuf, valignm);\
    value = kvbuf;\
    kvbuf += valuebytes;\
    kvbuf = ROUNDUP(kvbuf, talignm);\
    kvsize=kvbuf-kvbuf_start;\
  }else if(kvtype==1){\
    char *kvbuf_start=kvbuf;\
    kvbuf = ROUNDUP(kvbuf, kalignm);\
    key = kvbuf; \
    keybytes = strlen(key)+1;\
    kvbuf += keybytes;\
    kvbuf = ROUNDUP(kvbuf, valignm);\
    value = kvbuf;\
    valuebytes = strlen(value)+1;\
    kvbuf += valuebytes;\
    kvbuf = ROUNDUP(kvbuf, talignm);\
    kvsize=kvbuf-kvbuf_start;\
  }else if(kvtype==2){\
    char *kvbuf_start=kvbuf;\
    keybytes = data->ksize;\
    valuebytes = data->vsize;\
    kvbuf = ROUNDUP(kvbuf, kalignm);\
    key = kvbuf;\
    kvbuf += keybytes;\
    kvbuf = ROUNDUP(kvbuf, valignm);\
    value = kvbuf;\
    kvbuf += valuebytes;\
    kvbuf = ROUNDUP(kvbuf, talignm);\
    kvsize=kvbuf-kvbuf_start;\
  }else if(kvtype==3){\
    char *kvbuf_start=kvbuf;\
    kvbuf = ROUNDUP(kvbuf, kalignm);\
    key = kvbuf;\
    keybytes = strlen(key)+1;\
    kvbuf += keybytes;\
    kvbuf = ROUNDUP(kvbuf, talignm);\
    valuebytes = 0;\
    value = NULL;\
    kvsize=kvbuf-kvbuf_start;\
  }else LOG_ERROR("Undefined type %d!", kvtype);\
}

#define GET_KV_SIZE(kvtype, keybytes, valuebytes, kvsize) \
{\
  char *buf=0;\
  char *buf_start=0;\
  if(kvtype==0){\
    buf+=twointlen;\
    buf=ROUNDUP(buf,kalignm);\
    buf+=keybytes;\
    buf=ROUNDUP(buf,valignm);\
    buf+=valuebytes;\
    buf=ROUNDUP(buf,talignm);\
  }else if(kvtype==1 || kvtype==2){\
    buf+=keybytes;\
    buf=ROUNDUP(buf,valignm);\
    buf+=valuebytes;\
    buf=ROUNDUP(buf, talignm);\
  }else if(kvtype==3){\
    buf+=keybytes;\
    buf=ROUNDUP(buf, talignm);\
  }\
  else LOG_ERROR("Undefined KV type %d!\n", kvtype);\
  kvsize=buf-buf_start;\
}

#define PUT_KV_VARS(kvtype,kvbuf,key,keybytes,value,valuebytes,kvsize) \
{\
  char *kvbuf_start=kvbuf;\
  if(kvtype==0){\
    *(int*)kvbuf=keybytes;\
    kvbuf+=oneintlen;\
    *(int*)kvbuf=valuebytes;\
    kvbuf+=oneintlen;\
    kvbuf=ROUNDUP(kvbuf, kalignm);\
    memcpy(kvbuf, key, keybytes);\
    kvbuf += keybytes;\
    kvbuf=ROUNDUP(kvbuf, valignm);\
    memcpy(kvbuf, value, valuebytes);\
    kvbuf += valuebytes;\
    kvbuf=ROUNDUP(kvbuf, talignm);\
  }else if(kvtype==1 || kvtype==2){\
    memcpy(kvbuf, key, keybytes);\
    kvbuf += keybytes;\
    kvbuf = ROUNDUP(kvbuf, valignm);\
    memcpy(kvbuf, value, valuebytes);\
    kvbuf += valuebytes;\
    kvbuf=ROUNDUP(kvbuf, talignm);\
  }else if(kvtype==3){\
    memcpy(kvbuf, key, keybytes);\
    kvbuf += keybytes;\
    kvbuf = ROUNDUP(kvbuf, talignm);\
  }\
  kvsize=kvbuf-kvbuf_start;\
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
  }else if(kmvtype==1 || kmvtype==2){\
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
  }else if(kmvtype==3){\
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
  }else LOG_ERROR("Undefined KV type %d!\n", kmvtype);\
  kmvsize=kvbuf-kvbuf_start;\
}

extern int oneintlen;
extern int twointlen;
extern int threeintlen;

extern int kalignm;
extern int valignm;
extern int talignm;

#endif
