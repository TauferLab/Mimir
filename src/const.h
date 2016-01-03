#ifndef CONSTANT_H
#define CONSTANT_H

extern int oneintlen;
extern int twointlen;

#define ROUNDUP(A,B) (char *) (((uint64_t) A + B) & ~B);
#define ALIGNKV 1
#define ALIGNK  ALIGNKV
#define ALIGNV  ALIGNKV
#define ALIGNT  ALIGNKV

#define ASIZE(A,B) ((A%B)==0?(A):(A+B-A%B))

#endif
