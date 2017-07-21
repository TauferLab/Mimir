AM_LDFLAGS = -L../src
AM_CPPFLAGS = -I../src -std=c++11 -fpermissive -DENABLE_PROFILER -DENABLE_TRACKER -DNDEBUG
AM_CXXFLAGS = -I../src -std=c++11 -fpermissive -DENABLE_PROFILER -DENABLE_TRACKER -DNDEBUG

bin_PROGRAMS = gen_keys gen_words gen_3d_points

gen_keys_SOURCES = gen_keys.cpp common.cpp
gen_keys_LDFLAGS = $(AM_LDFLAGS)
gen_keys_LDADD = -lmimir

gen_words_SOURCES = gen_words.cpp common.cpp
gen_words_LDFLAGS = $(AM_LDFLAGS)
gen_words_LDADD = -lmimir

gen_3d_points_SOURCES = gen_3d_points.cpp
gen_3d_points_LDFLAGS = $(AM_LDFLAGS)
gen_3d_points_LDADD = -lmimir
