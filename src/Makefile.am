lib_LIBRARIES = libmimir.a
libmimir_a_CXXFLAGS = -std=c++11 -fpermissive -DENABLE_PROFILER -DENABLE_TRACKER -DNDEBUG
libmimir_adir = $(includedir)
libmimir_a_HEADERS = mimir.h interface.h recordformat.h globals.h hashbucket.h \
		     hash.h log.h baserecordformat.h inputsplit.h filesplitter.h \
		     filereader.h stat.h baseshuffler.h filewriter.h kvcontainer.h \
		     container.h containeriter.h mimircontext.h tools.h memory.h \
		     combinekvcontainer.h kmvcontainer.h collectiveshuffler.h  \
		     nbcollectiveshuffler.h combinecollectiveshuffler.h config.h \
		     nbcombinecollectiveshuffler.h chunkmanager.h
libmimir_a_SOURCES = mimircontext.h                           		       \
		     container.cpp container.h containeriter.h	       	       \
		     kvcontainer.cpp kvcontainer.h		      	       \
		     combinekvcontainer.cpp  combinekvcontainer.h	       \
		     kmvcontainer.cpp kmvcontainer.h 		               \
		     baseshuffler.h collectiveshuffler.cpp collectiveshuffler.h\
		     combinecollectiveshuffler.cpp combinecollectiveshuffler.h \
		     nbcollectiveshuffler.cpp nbcollectiveshuffler.h	       \
		     nbcombinecollectiveshuffler.cpp nbcombinecollectiveshuffler.h \
		     inputsplit.cpp inputsplit.h filesplitter.cpp filesplitter.h \
		     filereader.h filewriter.cpp filewriter.h		       \
		     hashbucket.cpp hashbucket.h hash.cpp hash.h	       \
		     baserecordformat.h recordformat.h recordformat.cpp        \
		     config.cpp config.h stat.cpp stat.h globals.cpp	       \
		     globals.h log.h interface.h			       \
		     mimir.cpp mimir.h tools.h memory.cpp memory.h
