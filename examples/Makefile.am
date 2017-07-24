AM_LDFLAGS = -L../src
AM_CPPFLAGS = -I../src -g -Wno-write-strings -Wall -Wconversion     \
	      -fpermissive -DENABLE_PROFILER -DENABLE_TRACKER -DNDEBUG
AM_CXXFLAGS = -I../src -g -Wno-write-strings -Wall -Wconversion     \
	      -fpermissive -DENABLE_PROFILER -DENABLE_TRACKER -DNDEBUG

bin_PROGRAMS = wc wc_cb bfs oc oc_cb join

wc_SOURCES = wordcount.cpp
wc_LDFLAGS = $(AM_LDFLAGS)
wc_LDADD = -lmimir

wc_cb_SOURCES = wordcount.cpp
wc_cb_CPPFLAGS = $(AM_CPPFLAGS) -DCOMBINER
wc_cb_CXXFLAGS = $(AM_CXXFLAGS) -DCOMBINER
wc_cb_LDFLAGS = $(AM_LDFLAGS)
wc_cb_LDADD = -lmimir

bfs_SOURCES = bfs.cpp
bfs_LDFLAGS = $(AM_LDFLAGS)
bfs_LDADD = -lmimir

oc_SOURCES = octree_clustering.cpp
oc_LDFLAGS = $(AM_LDFLAGS)
oc_LDADD = -lmimir

oc_cb_SOURCES = octree_clustering.cpp 
oc_cb_CPPFLAGS = $(AM_CPPFLAGS) -DCOMBINER
oc_cb_CXXFLAGS = $(AM_CXXFLAGS) -DCOMBINER
oc_cb_LDFLAGS = $(AM_LDFLAGS)
oc_cb_LDADD = -lmimir

join_SOURCES = join.cpp
join_LDFLAGS = $(AM_LDFLAGS)
join_LDADD = -lmimir
