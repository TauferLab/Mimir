/*
 * (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego 
 *     Supercomputer Center, National University of Defense Technology, 
 *     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
 *
 *     See COPYRIGHT in top-level directory.
 */
// Hash function hashlittle()
// from lookup3.c, by Bob Jenkins, May 2006, Public Domain
// bob_jenkins@burtleburtle.net

#include <stdint.h>

uint32_t hashlittle(const void *key, size_t length, uint32_t);
