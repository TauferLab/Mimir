lib_LIBRARIES = libmimir.a
libmimir_a_CXXFLAGS = -g -Wno-write-strings -Wall -Wconversion      		\
		      -fpermissive -DENABLE_PROFILER -DENABLE_TRACKER -DNDEBUG
libmimir_adir = $(includedir)
libmimir_a_HEADERS = mimir.h interface.h globals.h hashbucket.h 	       \
		     hash.h log.h fileparser.h inputsplit.h filesplitter.h     \
		     filereader.h stat.h baseshuffler.h filewriter.h kvcontainer.h \
		     container.h containeriter.h mimircontext.h tools.h memory.h \
		     combinekvcontainer.h kmvcontainer.h collectiveshuffler.h  \
		     combinebincontainer.h bincontainer.h serializer.h	       \
		     nbcollectiveshuffler.h combinecollectiveshuffler.h config.h \
		     ac_config.h nbcombinecollectiveshuffler.h chunkmanager.h  \
		     uniteddataset.h getrss.h powerlimit.h
libmimir_a_SOURCES = mimircontext.h                           		       \
		     container.cpp container.h containeriter.h		       \
		     kvcontainer.h combinekvcontainer.h kmvcontainer.h 	       \
		     combinebincontainer.h bincontainer.h		       \
		     baseshuffler.h collectiveshuffler.h combinecollectiveshuffler.h \
		     nbcollectiveshuffler.h nbcombinecollectiveshuffler.h      \
		     inputsplit.cpp inputsplit.h filesplitter.cpp filesplitter.h \
		     filereader.h filewriter.h hashbucket.h hash.cpp hash.h    \
		     config.cpp config.h stat.cpp stat.h globals.cpp	       \
		     globals.h log.h interface.h			       \
		     mimir.cpp mimir.h tools.h memory.cpp memory.h	       \
		     uniteddataset.h powerlimit.cpp powerlimit.h
