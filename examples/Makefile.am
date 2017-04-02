MIMIR_DIR=/home/taogao/install/mimir/
AM_LDFLAGS = -L$(MIMIR_DIR)/lib -lmimir
#AM_CPPFLAGS = -I../src -std=c++11 -fpermissive -DENABLE_PROFILER -DENABLE_TRACKER -DNDEBUG
AM_CXXFLAGS = -I$(MIMIR_DIR)/src -std=c++11 -fpermissive -DENABLE_PROFILER -DENABLE_TRACKER -DNDEBUG

bin_PROGRAMS = wc

wc_SOURCES = wordcount.cpp common.cpp
wc_LDFLAGS = $(AM_LDFLAGS) $(STATIC_FLAGS)

#bfs_SOURCES = bfs.cpp common.cpp
#bfs_LDFLAGS = $(AM_LDFLAGS)

#octree_SOURCES = octree_move_lg.cpp common.cpp
#octree_LDFLAGS = $(AM_LDFLAGS)
