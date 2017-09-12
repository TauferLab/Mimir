AM_LDFLAGS = -L../src
AM_CPPFLAGS = -I../src -g -Wno-write-strings -Wall -Wconversion     \
	      -fpermissive -DENABLE_PROFILER -DENABLE_TRACKER -DNDEBUG
AM_CXXFLAGS = -I../src -g -Wno-write-strings -Wall -Wconversion     \
	      -fpermissive -DENABLE_PROFILER -DENABLE_TRACKER -DNDEBUG

bin_PROGRAMS = wc wc_cb bfs bfs_join oc oc_cb join join_split

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

bfs_join_SOURCES = bfs_join.cpp
bfs_join_LDFLAGS = $(AM_LDFLAGS)
bfs_join_LDADD = -lmimir

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

join_split_SOURCES = join.cpp
join_split_CPPFLAGS = $(AM_CPPFLAGS) -DSPLIT_HINT
join_split_CXXFLAGS = $(AM_CXXFLAGS) -DSPLIT_HINT
join_split_LDFLAGS = $(AM_LDFLAGS)
join_split_LDADD = -lmimir
