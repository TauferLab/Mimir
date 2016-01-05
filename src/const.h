#ifndef CONSTANT_H
#define CONSTANT_H

#define DEFINE_KV_VARS  \
    char *key, *value;\
    int keybytes, valuebytes, kvsize;\
    char *kvbuf;

#define GET_KV_VARS_TYPE0 \
  {\
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
  }

    

extern int oneintlen;
extern int twointlen;
extern int threeintlen;

#define NIN(A,B) ((A) < (B)) ? (A) : (B)
#define MAX(A,B) ((A) > (B)) ? (A) : (B)

#define ROUNDUP(A,B) (char *) (((uint64_t) A + B) & ~B);

#define ALIGNKV 4
#define ALIGNK  ALIGNKV
#define ALIGNV  ALIGNKV
#define ALIGNT  ALIGNKV

#define ASIZE(A,B) ((A%B)==0?(A):(A+B-A%B))

#endif
