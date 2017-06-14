AM_LDFLAGS = -L../src
AM_CPPFLAGS = -I../src -std=c++11 -fpermissive -DENABLE_PROFILER -DENABLE_TRACKER -DNDEBUG
AM_CXXFLAGS = -I../src -std=c++11 -fpermissive -DENABLE_PROFILER -DENABLE_TRACKER -DNDEBUG

bin_PROGRAMS = wc bfs oc join

wc_SOURCES = wordcount.cpp
wc_LDFLAGS = $(AM_LDFLAGS)
wc_LDADD = -lmimir

bfs_SOURCES = bfs.cpp
bfs_LDFLAGS = $(AM_LDFLAGS)
bfs_LDADD = -lmimir

oc_SOURCES = octree_clustering.cpp
oc_LDFLAGS = $(AM_LDFLAGS)
oc_LDADD = -lmimir

join_SOURCES = join.cpp
join_LDFLAGS = $(AM_LDFLAGS)
join_LDADD = -lmimir
