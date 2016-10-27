### Mimir Overview ###

Mimir is a memory-efficient and scalable MapReduce implementation over MPI for supercomputing systems. The inefficient memory use of MapReduce-MPI library [link](http://mapreduce.sandia.gov/) drives the design of this new library.

### C++ Programming Interfaces ###
All interfaces are defined in the MIMIR\_NS namespace and provided byMapReduce class. A MapReduce Object contians a KV container (KVC) and defines a set of oprations to process the KVs in the container.

#### Construct, Destruct and Copy MapReduce Object ####
The MapReduce Object can be constructed from a MPI communicator or copy from another MapReduce object. The example code is shown as follows:
```c++
MapReduce mr1=MapReduce(comm);
MapReduce mr2=MapReduce(mr1);
delete mr2
delete mr1
```

#### map interfaces ####
```c++
uint64_t map_text_file(
    char *filepath, /* filename or file path of input files */
    int shared, /* the file in shared file system or not */
    int recurse, /* read file recursely or not */
    char *separator, /* separator of words */
    UserMapFile mymap, /*user-defined map function*/
    void *ptr, /* pointer passed to user-defined map function */ 
    int comm /* perform aggregate communication or not */
    ) /* return the number of KVs in all processes */
void (*UserMapFile)(
    MapReduce *mr, /* MapReduce object pointer */
    char *word,  /* word */
    void *ptr /* user-defined pointer */
    )
```

```c++
uint64_t map_key_value(
    MapReduce *mr, /* MapReduce object pointer */ 
    UserMapKV mymap,  /* user-defined map function */
    void *ptr,  /* pointer passed to user-defined map function */
    int comm /* perform aggregate communication or not */
    ) /* return the number of KVs in all processes */
void (*UserMapKV)(
    MapReduce *mr, /* MapReduce object pointer */
    char *key,  /* key */
    int keybytes,  /* key bytes */
    char *value,  /* value */
    int valuebytes, /* value bytes */
    void *ptr /* user-defined pointer */
    )
```

```c++
uint64_t init_key_value(
     UserInitKV myinit,  /* user-defined init function */
     void *ptr, /* pointer passed to user-defined init function */
     int comm /* perform aggregate communication or not */
     ) /* return the number of KVs in all processes */
void (*UserInitKV)(
    MapReduce *mr, /* MapReduce object pointer */ 
    void *ptr /* user-defined pointer */
    )
```

#### reduce interfaces ####

```c++
uint64_t reduce(
    UserReduce myreduce, /* user-defined reduce */ 
    void *ptr /* pointer passed to user-defined reduce function */
    ) /* return the number of KVs in all processes */
void (*UserReduce)(
    MapReduce *mr,  /*  MapReduce object pointer */
    char *key,  /*  key */
    int keybytes,  /* key bytes */
    void *ptr /* user-defined pointer */
    )
```

#### interfaces within map and reduce callbacks ####
```c++
void add_key_value(
    char *key, /* key */
    int keybytes, /* key bytes */
    char *value,  /* value */
    int valuebytes)/* value bytes */
```

```c++
void *get_first_value(
) /* return the first value */ 
```

```c++
void *get_next_value(
) /* return the next value or null if no extra values */
```

```c++
int get_value_bytes(
) /* get the bytes of current value */
```

```c++
int get_value_count(
) /* get number of values */
```

#### optional interfaces ####

```c++
void set_comiber(
    UserCombiner mycombiner, /* user-defined combiner function */
    void *ptr /* user-defined pointer */
    )
void (*UserCombiner)(
    MapReduce *mr, /* MapReduce Object pointer */
    char *key, /* key */
    int keybytes, /* bytes of key */
   char *value1,  /* first value */
   int value1bytes, /* bytes of first value */
   char *value2,  /* second value */
   int value2bytes, /* bytes of second value */
   void *ptr /* user-defined pointer */
   )
```

```c++
void set_key_length(
    int keybytes /* length of key */
    )
```

```c++
void set_value_length(
    int valuebytes /* length of value */
    )
```

```c++
void set_hash(
    int64_t (*myhash)(char *key, int keybytes))
```

#### other interfaces ####
```c++
void scan(
    UserScan myscan, 
    void *ptr=NULL)
```

```c++
void output(
    int keytype, 
    int valuetype, 
    int format, 
    FILE *fp) 
```

### Environment Variables ###
```
$ MIMIR_COMM_UNIT_SIZE=4K (default: 4K, set the unit size for communication buffer)
```

### Example Applications ###


### Gather Profile & Trace Data ###


### Debugging ###
```
$ export MIMIR_DBG_ALL=1 (default: 0, turn on general, communication, data and io debug information)
$ export MIMIR_DBG_GEN=1 (default: 0, turn on general debug information)
$ export MIMIR_DBG_DATA=1 (default:0, turn on data debug information )
$ export MIMIR_DBG_COMM=1 (default:0, turn on communication information)
$ export MIMIR_DBG_IO=1 (default:0, turn on io debug information )
```