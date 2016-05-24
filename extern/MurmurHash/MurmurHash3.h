//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.
// http://code.google.com/p/smhasher/wiki/MurmurHash3

/*
 * Modification notice:
 * The file is modified from its original version, to move fmix() from the CPP file to the header file.
 * The versions taking signed integers are added.
 * Paradigm4 Inc. June 2012
 */

#ifndef _MURMURHASH3_H_
#define _MURMURHASH3_H_

#include <stdint.h>

//-----------------------------------------------------------------------------

void MurmurHash3_x86_32  ( const void * key, int len, uint32_t seed, void * out );

void MurmurHash3_x86_128 ( const void * key, int len, uint32_t seed, void * out );

void MurmurHash3_x64_128 ( const void * key, int len, uint32_t seed, void * out );


//-----------------------------------------------------------------------------
// Finalization mix - force all bits of a hash block to avalanche

#define BIG_CONSTANT(x) (x##LLU)

inline uint32_t fmix ( uint32_t h )
{
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;

  return h;
}

//----------

inline uint64_t fmix ( uint64_t k )
{
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xff51afd7ed558ccd);
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
  k ^= k >> 33;

  return k;
}

inline uint64_t fmix(int64_t k) {
    return fmix(static_cast<uint64_t>(k));
}

inline uint32_t fmix(int32_t k) {
    return fmix(static_cast<uint32_t>(k));
}

//-----------------------------------------------------------------------------

#endif // _MURMURHASH3_H_
