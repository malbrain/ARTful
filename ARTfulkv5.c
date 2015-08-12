//	ARTful5: Adaptive Radix Trie key-value store

//	Author: Karl Malbrain, malbrain@cal.berkeley.edu
//	Date:   13 JAN 15

/*
This work, including the source code, documentation
and related data, is placed into the public domain.

The orginal author is Karl Malbrain.

THIS SOFTWARE IS PROVIDED AS-IS WITHOUT WARRANTY
OF ANY KIND, NOT EVEN THE IMPLIED WARRANTY OF
MERCHANTABILITY. THE AUTHOR OF THIS SOFTWARE,
ASSUMES _NO_ RESPONSIBILITY FOR ANY CONSEQUENCE
RESULTING FROM THE USE, MODIFICATION, OR
REDISTRIBUTION OF THIS SOFTWARE.
*/

#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE
#define _GNU_SOURCE

#include <unistd.h>
#include <stdlib.h>
#include <memory.h>
#include <sys/mman.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>

#include <xmmintrin.h>

typedef unsigned long int ulong;
typedef unsigned char uchar;
typedef unsigned int uint;

enum NodeType {
	UnusedNode = 0, // node is not yet in use
	LeafSlot,		// node slot contains leaf offset
	ValueSlot,		// node slot contains ARTval offset
	SpanNode,		// node contains up to 8 key bytes and leaf element
	Array4,			// node contains 4 radix slots & leaf element
	Array16,		// node contains 16 radix slots & leaf element
	Array48,		// node contains 48 radix slots & leaf element
	Array256		// node contains 256 radix slots & leaf element
};

typedef union {
  struct {
	ulong off:45;		// offset to node sub-contents
	uchar type:3;		// type of radix node
	uchar mutex[1];		// update/write synchronization
	uchar nslot:7;		// number of slots of node in use
	uchar dead:1;		// node is no longer in the tree
  };
  ulong bits;
} ARTslot;

//  a node is broken down into two parts:
//  the node proper and its pointer slot.

//  the first few fields are generic to all nodes:

typedef struct {
	ARTslot value[1];	// slot to a leaf value that ended before this node.
} ARTgeneric;

//	radix node with four slots and their key bytes:

typedef struct {
	ARTslot value[1];	// slot to a leaf value that ended before this node.
	uchar keys[4];
	ARTslot radix[4];
} ARTnode4;

//	radix node with sixteen slots and their key bytes:

typedef struct {
	ARTslot value[1];	// slot to a leaf value that ended before this node.
	uchar keys[16];
	ARTslot radix[16];
} ARTnode16;

//	radix node with sixty-four slots and a 256 key byte array:

typedef struct {
	ARTslot value[1];	// slot to a leaf value that ended before this node.
	uchar keys[256];
	ARTslot radix[48];
} ARTnode48;

//	radix node all two hundred fifty six slots

typedef struct {
	ARTslot value[1];	// slot to a leaf value that ended before this node.
	ARTslot radix[256];
} ARTnode256;

//	Span node containing up to 16 consecutive key bytes

typedef struct {
	ARTslot value[1];	// slot to a leaf value that ended before this node.
	uchar bytes[8];
	ARTslot next[1];	// next node under span
} ARTspan;

//	the ARTful trie containing the root node slot
//	and the heap storage management.

typedef struct {
	ARTslot root[1];
	ulong arena_size;	// size of Arena File
	ulong arena_next;	// next available offset
	uchar arena_mutex[1];
} ARTtrie;

//	the ARTful trie value string in the heap

typedef struct {
	uchar len;			// this can be changed to a ushort or uint
	uchar value[0];
} ARTval;

typedef union {
	ARTspan *span;
	ARTnode4 *radix4;
	ARTnode16 *radix16;
	ARTnode48 *radix48;
	ARTnode256 *radix256;
	ARTgeneric *generic;
} ARTfan;

//	the cursor stack element

typedef struct {
	ARTslot *slot;		// current slot
	uint off;			// offset within key
	int idx;			// current index within slot
} ARTstack;

//	the cursor control

typedef struct {
	uint maxdepth;		// maximum depth of ARTful trie
	uint depth;			// current depth of cursor
	ARTval *value;		// current leaf node
	ARTstack stack[0];	// cursor stack
} ARTcursor;

//	Each thread gets one of these structures

typedef struct {
	ulong base;			// base of arena chunk assigned to thread
	ulong offset;		// next offset of this chunk to allocate
	ARTtrie *trie;		// ARTful trie
	ARTcursor *cursor;	// thread cursor
} ARTthread;

//	one byte mutex spin lock

#define relax() asm volatile("pause\n": : : "memory")

void mutexlock(uchar *volatile latch)
{
  while( __sync_lock_test_and_set (latch, 1) )
	while( latch[0] )
		relax();
}

void mutexrelease(uchar *latch)
{
//	__sync_synchronize();
	*latch = 0;
}

//  Maximum Arena size, e.g. virtual memory size

ulong ArenaVM = 1024UL * 1024UL*1024UL *12;

//	Initial/Incremental Arena file size

ulong ArenaInit = 1024UL*1024UL *100;

uchar *Arena;		// pointer to base of heap
int ArenaFd;		// arena file descriptor

//	incremental amount to allocate to threads
//	must be a power of two.

#define ARENA_chunk (1024 * 1024)

//	release unused value heap area

uint Census[8], Free[8];

void art_free (ARTtrie *trie, uchar type, void *what)
{
	mutexlock (trie->arena_mutex);
	Free[type]++;
	mutexrelease (trie->arena_mutex);
}

//	allocate space in the Arena heap

ulong art_space (ARTthread *thread, uint size)
{
ulong offset;
uint xtra;

	if( xtra = size & 0x7 )
		size += 8 - xtra;

	if( xtra = thread->offset & 0x7 )
		thread->offset += 8 - xtra;

	if( !thread->offset || thread->offset + size > ARENA_chunk ) {
	  mutexlock (thread->trie->arena_mutex);
	  if( thread->trie->arena_next + ARENA_chunk > thread->trie->arena_size ) {
		thread->trie->arena_next = thread->trie->arena_size;
		thread->trie->arena_size += ArenaInit;
#ifdef PERSIST
		ftruncate (ArenaFd, thread->trie->arena_size);
#endif
	  }
	  thread->offset = 0;
	  thread->base = thread->trie->arena_next;
	  thread->trie->arena_next += ARENA_chunk;
	  mutexrelease (thread->trie->arena_mutex);
	}

	offset = thread->offset + thread->base;
//	memset (Arena + offset, 0, size);
	thread->offset += size;
	return offset;
}

//	allocate a new trie node in the Arena heap

ulong art_node (ARTthread *thread, uchar type)
{
uint size;

	mutexlock (thread->trie->arena_mutex);
	Census[type]++;
	mutexrelease (thread->trie->arena_mutex);

	switch( type ) {
	case SpanNode:
	  size = sizeof(ARTspan);
	  break;
	case Array4:
	  size = sizeof(ARTnode4);
	  break;
	case Array16:
	  size = sizeof(ARTnode16);
	  break;
	case Array48:
	  size = sizeof(ARTnode48);
	  break;
	case Array256:
	  size = sizeof(ARTnode256);
	  break;
	default:
	  abort();
	}

	return art_space (thread, size);
}

//	allocate a new thread cursor object

ARTthread *ARTnewthread (ARTtrie *trie, uint depth)
{
ARTcursor *cursor = calloc (1, sizeof(ARTcursor) + depth * sizeof(ARTstack));
ARTthread *thread = calloc (1, sizeof(ARTthread));

	cursor->maxdepth = depth;
	thread->cursor = cursor;
	thread->trie = trie;
	return thread;
}

//	create/open an ARTful trie

ARTtrie *ARTnew (int fd)
{
int flag = PROT_READ | PROT_WRITE;
ARTnode256 *radix256, *root256;
ARTtrie *trie;
ulong offset;
uint i, j;

#ifdef PERSIST
	if( !(offset = lseek64 (fd, 0L, 2)) )
		ftruncate64 (fd, offset = ArenaInit);

	Arena = mmap (0, ArenaVM, flag, MAP_SHARED, fd, 0);
#else
	offset = ArenaVM;
	Arena = mmap(NULL, offset, flag, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
#endif

	ArenaFd = fd;

	trie = (ARTtrie *)Arena;
	trie->arena_size = offset;

	//	is this a new file?
	//	if so, fill out the first two levels
	//	of the trie with radix256 nodes.

	if( !trie->arena_next ) {
	  trie->arena_next = sizeof(ARTtrie);
	  root256 = (ARTnode256 *)(Arena + trie->arena_next);
	  trie->root->off = trie->arena_next >> 3;
	  trie->root->type = Array256;
	  trie->arena_next += sizeof(ARTnode256);

	  for( i = 0; i < 256; i++ ) {
		radix256 = (ARTnode256 *)(Arena + trie->arena_next);
		root256->radix[i].off = trie->arena_next >> 3;
		root256->radix[i].type = Array256;
		trie->arena_next += sizeof(ARTnode256);
//		for( j = 0; j < 256; j++ ) { // fill in 3rd level
//			radix256[i].radix[j].off = trie->arena_next >> 3;
//			radix256[i].radix[j].type = Array256;
//	  		trie->arena_next += sizeof(ARTnode256);
//		}
	  }

	  // round up to complete the first chunks

	  trie->arena_next |= ARENA_chunk - 1;
	  trie->arena_next++;
	}

	return trie;
}

void ARTclosethread (ARTthread *thread)
{
	free (thread->cursor);
	free (thread);
}

void ARTclose (ARTtrie *trie)
{
}

//	position cursor at largest key

void ARTlastkey (ARTthread *thread, uchar *key, uint keylen)
{
}

//	position cursor before requested key

void ARTstartkey (ARTthread *thread, uchar *key, uint keylen)
{
}

//	retrieve next key from cursor

uint ARTnextkey (ARTthread *thread, uchar *key, uint keymax)
{
}

//	retrieve previous key from cursor

uint ARTprevkey (ARTthread *thread, uchar *key, uint keymax)
{
}

//  find key in ARTful trie, returning its value slot address or zero

ARTslot *ARTfindkey (ARTthread *thread, uchar *key, uint keylen)
{
uint len, idx, off;
ARTfan node[1];
ARTslot *slot;
uchar *chr;

restart:
	slot = thread->trie->root;
	off = 0;

	//	loop through all the key bytes

	while( off < keylen ) {
	  node->generic = (ARTgeneric *)(Arena + slot->off * 8);
	  len = slot->nslot;
	  
	  if( slot->dead )
		goto restart;

	  switch( slot->type ) {
	  case ValueSlot:
		return NULL;

	  case LeafSlot:
		return NULL;

	  case SpanNode:
		if( keylen - off < len || memcmp (key + off, node->span->bytes, len) )
		  return NULL;

		slot = node->span->next;
		off += len;
		continue;

	  case Array4:
		for( idx = 0; idx < len; idx++ )
		  if( key[off] == node->radix4->keys[idx] )
			break;

		if( idx == len )
		  return NULL;

		slot = node->radix4->radix + idx;
		off++;
		continue;

	  case Array16:
		// is key byte in radix node?

		if( chr = memchr (node->radix16->keys, key[off++], len) ) {
		  idx = chr - node->radix16->keys;
		  slot = node->radix16->radix + idx;
		  continue;
		}

		return NULL;

	  case Array48:
		idx = node->radix48->keys[key[off++]];

		// is the key byte assigned to a radix node?

		if( idx == 0xff )
		  return NULL;

		slot = node->radix48->radix + idx;
		continue;

	  case Array256:
		slot = node->radix256->radix + key[off++];
		continue;

	  case UnusedNode:
		return NULL;
	  }
	}

	if( slot->type > ValueSlot ) {
	  node->generic = (ARTgeneric *)(Arena + slot->off * 8);
	  if( node->generic->value->type )
		return node->generic->value;
	  else
		return NULL;
	}

	if( slot->type )
	  return slot;

	return NULL;
}

//	insert key/value into ARTful trie, returning pointer to value slot.

ARTslot *ARTinsert (ARTthread *thread, uchar *key, uint keylen)
{
ARTfan node[1], node2[1], node3[1], node4[1];
ARTslot *prev, newslot[1], *slot;
ARTslot *oldvalue, *retvalue;
uint len, idx, max, off;
uchar slot48, *update48;
uchar *chr, type;
uint prevoff;

restart:
	slot = thread->trie->root;
	oldvalue = NULL;
	off = 0;

	while( off < keylen ) {
	  newslot->bits = slot->bits;
	  type = newslot->type;

	  node->generic = (ARTgeneric*)(Arena + newslot->off * 8);
	  update48 = NULL;
	  prev = slot;

	  if( newslot->dead )
		goto restart;

	  switch( type ) {
	  case SpanNode:
		max = len = newslot->nslot;

		if( len > keylen - off )
			len = keylen - off;

		for( idx = 0; idx < len; idx++ )
		  if( key[off + idx] != node->span->bytes[idx] )
			break;

		// did we use the entire span node?

		if( idx == max ) {
		  slot = node->span->next;
		  off += idx;
		  continue;
		}

		// obtain write lock on the node

		mutexlock (prev->mutex);
		*newslot->mutex = 1;

		//  see if slot changed values
		//	and restart if so.

		if( newslot->bits != prev->bits ) {
		  mutexrelease (prev->mutex);
		  continue;
		}

		prev->dead = 1;
		off += idx;

		// copy matching prefix bytes to a new span node

		if( idx ) {
		  node2->span = (ARTspan *)(Arena + art_node(thread, SpanNode));
		  memcpy (node2->span->bytes, node->span->bytes, idx);
		  newslot->off = (uchar *)node2->span - Arena >> 3;
		  mutexlock (node->span->value->mutex);
		  node2->span->value->bits = node->span->value->bits;
		  node->span->value->dead = 1;
		  *node2->span->value->mutex = 0;
		  mutexrelease (node->span->value->mutex);
		  newslot->type = SpanNode;
		  slot = node2->span->next;
		  newslot->nslot = idx;
		}

		// else cut the span node from the tree by transforming
		// the original node into a radix4 or span node

		else
		  slot = newslot;

		// place a radix node after span1 and before span2
		// if needed for additional key byte(s)

		if( off < keylen ) {
		  node3->radix4 = (ARTnode4 *)(Arena + art_node(thread, Array4));

		  // are we the first node?

		  if( !idx ) {
			mutexlock (node->span->value->mutex);
			node3->radix4->value->bits = node->span->value->bits;
			node->span->value->dead = 1;
			*node3->radix4->value->mutex = 0;
			mutexrelease (node->span->value->mutex);
		  }

		  slot->off = (uchar *)node3->radix4 - Arena >> 3;
		  slot->type = Array4;
		  slot->nslot = 2;

		  // fill in first radix element

		  node3->radix4->keys[0] = node->span->bytes[idx++];
		  slot = node3->radix4->radix + 0;
		}

		// are there any original span bytes remaining?
		// if so, place them in a second span node

		if( max - idx ) {
		  node4->span = (ARTspan *)(Arena + art_node(thread, SpanNode));
		  memcpy (node4->span->bytes, node->span->bytes + idx, max - idx);
		  mutexlock (node->span->next->mutex);
		  node4->span->next->bits = node->span->next->bits;
		  node->span->next->dead = 1;
		  *node4->span->next->mutex = 0;
		  mutexrelease (node->span->next->mutex);

		  slot->off = (uchar *)node4->span - Arena >> 3;
		  slot->nslot = max - idx;
		  slot->type = SpanNode;

		  slot = node4->span->value;
		} else {
		  mutexlock (node->span->next->mutex);
		  slot->bits = node->span->next->bits;
		  node->span->next->dead = 1;
		  *slot->mutex = 0;
		  mutexrelease (node->span->next->mutex);
		  slot = node3->radix4->value;
		}

		//  does key stop at the beginning of radix/span node?

		if( off == keylen )
		  break; 

		//  if not, fill in the second radix element
		//	and the rest of the key in span nodes below

		node3->radix4->keys[1] = key[off++];
		slot = node3->radix4->radix + 1;
		break;

	  case Array4:
		max = newslot->nslot;

		for( idx = 0; idx < max; idx++ )
		  if( key[off] == node->radix4->keys[idx] )
			break;

		if( idx < max ) {
		  slot = node->radix4->radix + idx;
		  off++;
		  continue;
		}

		// obtain write lock on the node

		mutexlock (prev->mutex);
		*newslot->mutex = 1;

		//  see if slot changed values
		//	and restart if so.

		if( newslot->bits != prev->bits ) {
		  mutexrelease (prev->mutex);
		  continue;
		}

		// add to radix4 node if room

		if( max < 4 ) {
		  node->radix4->keys[newslot->nslot] = key[off++];
		  slot = node->radix4->radix + newslot->nslot++;
		  break;
		}

		// the radix node is full, promote to
		// the next larger size.

		node2->radix16 = (ARTnode16 *)(Arena + art_node(thread, Array16));
		prev->dead = 1;

		for( idx = 0; idx < max; idx++ ) {
		  slot = node->radix4->radix + idx;
		  mutexlock (slot->mutex);
		  node2->radix16->radix[idx].bits = slot->bits;
		  node2->radix16->keys[idx] = node->radix4->keys[idx];
		  slot->dead = 1;
		  *node2->radix16->radix[idx].mutex = 0;
		  mutexrelease (slot->mutex);
		}

		node2->radix16->keys[max] = key[off++];
		mutexlock (node->radix4->value->mutex);
		node2->radix16->value->bits = node->radix4->value->bits;
		node->radix4->value->dead = 1;
		*node2->radix16->value->mutex = 0;
		mutexrelease (node->radix4->value->mutex);

		newslot->off = (uchar *)node2->radix16 - Arena >> 3;
		newslot->type = Array16;

		//	fill in rest of the key in span nodes below

		slot = node2->radix16->radix + newslot->nslot++;
		break;

	  case Array16:
		max = newslot->nslot;

		// is key byte in this radix node?

		if( chr = memchr (node->radix16->keys, key[off], max) ) {
		  idx = chr - node->radix16->keys;
		  slot = node->radix16->radix + idx;
		  off++;
		  continue;
		}

		// obtain write lock on the node

		mutexlock (prev->mutex);
		*newslot->mutex = 1;

		//  see if slot changed values
		//	and restart if so.

		if( newslot->bits != prev->bits ) {
		  mutexrelease (prev->mutex);
		  continue;
		}

		// add to radix node if room

		if( max < 16 ) {
		  node->radix16->keys[max] = key[off++];
		  slot = node->radix16->radix + newslot->nslot++;
		  break;
		}

		// the radix node is full, promote to
		// the next larger size. mark all the
		// keys as currently unused.

		node2->radix48 = (ARTnode48 *)(Arena + art_node(thread, Array48));
		prev->dead = 1;
		memset (node2->radix48->keys, 0xff, sizeof(node2->radix48->keys));

		for( idx = 0; idx < max; idx++ ) {
		  slot = node->radix16->radix + idx;
		  mutexlock (slot->mutex);
		  node2->radix48->radix[idx].bits = slot->bits;
		  node2->radix48->keys[node->radix16->keys[idx]] = idx;
		  slot->dead = 1;
		  *node2->radix48->radix[idx].mutex = 0;
		  mutexrelease (slot->mutex);
		}

		newslot->off = (uchar *)node2->radix48 - Arena >> 3;
		newslot->type = Array48;

		node2->radix48->keys[key[off++]] = max;
		mutexlock (node->radix16->value->mutex);
		node2->radix48->value->bits = node->radix16->value->bits;
		node->radix16->value->dead = 1;
		*node2->radix48->value->mutex = 0;
		mutexrelease (node->radix16->value->mutex);

		//	fill in rest of the key bytes into
		//	span nodes below.

		slot = node2->radix48->radix + newslot->nslot++;
		break;

	  case Array48:
		idx = node->radix48->keys[key[off]];

		if( idx < 0xff ) {
		  slot = node->radix48->radix + idx;
		  off++;
		  continue;
		}

		// obtain write lock on the node

		mutexlock (prev->mutex);
		*newslot->mutex = 1;

		//  see if slot changed values
		//	and restart if so.

		if( newslot->bits != prev->bits ) {
		  mutexrelease (prev->mutex);
		  continue;
		}

		// add to radix node

		if( newslot->nslot < 48 ) {
		  update48 = node->radix48->keys + key[off++];
		  slot48 = newslot->nslot++;
		  slot = node->radix48->radix + slot48;
		  break;
		}

		// the radix node is full, promote to
		// the next larger size.

		node2->radix256 = (ARTnode256 *)(Arena + art_node(thread, Array256));
		prev->dead = 1;

		for( idx = 0; idx < 256; idx++ )
		 if( node->radix48->keys[idx] < 0xff ) {
		  slot = node->radix48->radix + node->radix48->keys[idx];
		  mutexlock (slot->mutex);
		  node2->radix256->radix[idx].bits = slot->bits;
		  slot->dead = 1;
		  *node2->radix256->radix[idx].mutex = 0;
		  mutexrelease (slot->mutex);
		 }

		newslot->type = Array256;
		newslot->off = (uchar *)node2->radix256 - Arena >> 3;
		mutexlock (node->radix48->value->mutex);
		node2->radix256->value->bits = node->radix48->value->bits;
		node->radix48->value->dead = 1;
		*node2->radix256->value->mutex = 0;
		mutexrelease (node->radix48->value->mutex);

		//	fill in the rest of the key bytes
		//	into Span nodes below

		slot = node2->radix256->radix + key[off++];
		break;

	  case Array256:
		slot = node->radix256->radix + key[off++];
		continue;

	  case UnusedNode:
		mutexlock (prev->mutex);
		*newslot->mutex = 1;

		//  see if slot changed values
		//	and restart if so.

		if( newslot->bits != prev->bits ) {
		  mutexrelease (prev->mutex);
		  continue;
		}

		slot = newslot;
		break;

	  case ValueSlot:
	  case LeafSlot:
		oldvalue = newslot;
		slot = newslot;
		break;
	  }

	  // did we drop down from Array/Span node w/empty slot?
	  // else we dropped down from last three types.

	  if( type > ValueSlot )
		retvalue = slot;
	  else
		retvalue = prev;

	  // fill in an empty slot with remaining key bytes
	  // i.e. copy remaining key bytes to span nodes

	  while( len = keylen - off ) {
		node2->span = (ARTspan *)(Arena + art_node(thread, SpanNode));

		if( oldvalue ) {
			mutexlock (oldvalue->mutex);
			node2->span->value->bits = oldvalue->bits;
			mutexrelease (node2->span->value->mutex);
			mutexrelease (oldvalue->mutex);
		}

		if( len > sizeof(node2->span->bytes) )
		  len = sizeof(node2->span->bytes);

		memcpy (node2->span->bytes, key + off, len);
		slot->off = (uchar *)node2->span - Arena >> 3;
		slot->type = SpanNode;
		slot->nslot = len;
		oldvalue = NULL;

		retvalue = slot = node2->span->next;
		off += len;
	  }

	  // lock the slot for caller
	  // or leave lock in newslot

	  if( prev != retvalue ) {
	  	mutexlock (retvalue->mutex);
	    *newslot->mutex = 0;
	  }

	  prev->bits = newslot->bits;

	  if( update48 )
		*update48 = slot48;

	  return retvalue;
	}

	// return the leaf node slot

	if( slot->type > ValueSlot ) {
		node->generic = (ARTgeneric *)(Arena + slot->off * 8);
		retvalue = node->generic->value;
	} else
		retvalue = slot;

	mutexlock (retvalue->mutex);
	return retvalue;
}

//  scan the keys stored in the ARTtrie

ulong ARTscan (uchar *key, uint off, uint max, ARTslot *slot)
{
ulong children = 0;
ARTfan node[1];
ARTval *val;
uint i, j;
uint nxt;
uint idx;
int last;

	switch( slot->type ) {
	case SpanNode:
		node->span = (ARTspan *)(Arena + slot->off * 8);

		if( node->span->value->type > 0 ) {
		  fwrite (key, off, 1, stdout);
		  if( node->span->value->type == ValueSlot ) {
		  	val = (ARTval *)(Arena + node->span->value->off * 8);
			fwrite (val->value, val->len, 1, stdout);
		  } else for( idx = 1; idx < node->span->value->off; idx++ )
		    children++, fputc ('\n', stdout), fwrite (key, off, 1, stdout);

		  fputc ('\n', stdout);
		  children++;
		}

		memcpy (key + off, node->span->bytes, slot->nslot);
		off += slot->nslot;

		children += ARTscan (key, off, max, node->span->next);
		return children;

	case LeafSlot:	
	case ValueSlot:	
		fwrite (key, off, 1, stdout);
		if( slot->type == ValueSlot ) {
		  val = (ARTval *)(Arena + slot->off * 8);
		  fwrite (val->value, val->len, 1, stdout);
		} else for( idx = 1; idx < slot->off; idx++ )
		  children++, fputc ('\n', stdout), fwrite (key, off, 1, stdout);
		fputc ('\n', stdout);
		children++;
		return children;

	case Array4:
		node->radix4 = (ARTnode4 *)(Arena + slot->off * 8);

		if( node->radix4->value->type > 0 ) {
		  fwrite (key, off, 1, stdout);
		  if( node->radix4->value->type == ValueSlot ) {
			val = (ARTval *)(Arena + node->radix4->value->off * 8);
			fwrite (val->value, val->len, 1, stdout);
		  } else for( idx = 1; idx < node->span->value->off; idx++ )
		    children++, fputc ('\n', stdout), fwrite (key, off, 1, stdout);
		  fputc ('\n', stdout);
		  children++;
		}

		nxt = 0x100;
		last = -1;

		for( idx = 0; idx < slot->nslot; idx++ ) {
		  for( i = 0; i < slot->nslot; i++ )
			if( node->radix4->keys[i] > last )
			  if( node->radix4->keys[i] < nxt )
				nxt = node->radix4->keys[i], j = i;

		  key[off] = nxt;
		  children += ARTscan (key, off + 1, max, node->radix4->radix + j);
		  last = nxt;
		  nxt = 0x100;
		}
		
		return children;

	case Array16:
		node->radix16 = (ARTnode16 *)(Arena + slot->off * 8);

		if( node->radix16->value->type > 0 ) {
		  fwrite (key, off, 1, stdout);
		  if( node->radix16->value->type == ValueSlot ) {
			val = (ARTval *)(Arena + node->radix16->value->off * 8);
			fwrite (val->value, val->len, 1, stdout);
		  } else for( idx = 1; idx < node->radix16->value->off; idx++ )
		    children++, fputc ('\n', stdout), fwrite (key, off, 1, stdout);
		  fputc ('\n', stdout);
		  children++;
		}

		nxt = 0x100;
		last = -1;

		for( idx = 0; idx < slot->nslot; idx++ ) {
		  for( i = 0; i < slot->nslot; i++ )
			if( node->radix16->keys[i] > last )
			  if( node->radix16->keys[i] < nxt )
				nxt = node->radix16->keys[i], j = i;

		  key[off] = nxt;
		  children += ARTscan (key, off + 1, max, node->radix16->radix + j);
		  last = nxt;
		  nxt = 0x100;
		}
		
		return children;

	case Array48:
		node->radix48 = (ARTnode48 *)(Arena + slot->off * 8);

		if( node->radix48->value->type > 0 ) {
		  fwrite (key, off, 1, stdout);
		  if( node->radix48->value->type == ValueSlot ) {
			val = (ARTval *)(Arena + node->radix48->value->off * 8);
			fwrite (val->value, val->len, 1, stdout);
		  } else for( idx = 1; idx < node->radix48->value->off; idx++ )
		    children++, fputc ('\n', stdout), fwrite (key, off, 1, stdout);
		  fputc ('\n', stdout);
		  children++;
		}

		for( idx = 0; idx < 256; idx++ ) {
		  j = node->radix48->keys[idx];
		  if( j < 0xff ) {
			key[off] = idx;
			children += ARTscan (key, off + 1, max, node->radix48->radix + j);
		  }
		}
		
		return children;

	case Array256:
		node->radix256 = (ARTnode256 *)(Arena + slot->off * 8);

		if( node->radix256->value->type > 0 ) {
		  fwrite (key, off, 1, stdout);
		  if( node->radix256->value->type == ValueSlot ) {
			val = (ARTval *)(Arena + node->radix256->value->off * 8);
			fwrite (val->value, val->len, 1, stdout);
		  } else for( idx = 1; idx < node->radix256->value->off; idx++ )
		    children++, fputc ('\n', stdout), fwrite (key, off, 1, stdout);
		  fputc ('\n', stdout);
		  children++;
		}

		for( idx = 0; idx < 256; idx++ ) {
		  key[off] = idx;
		  children += ARTscan (key, off + 1, max, node->radix256->radix + idx);
		}

		return children;
	}

	return 0;
}

//  count the number of keys stored in the ARTtrie

ulong ARTcount (ARTslot *slot)
{
ulong children;
ARTfan node[1];
uint idx;

	switch( slot->type ) {
	case SpanNode:
		node->span = (ARTspan *)(Arena + slot->off * 8);
		children = ARTcount (node->span->next);

		if( node->span->value->type > 0 )
		  if( node->span->value->type == LeafSlot )
			children += node->span->value->off;
		  else
			children++;

		return children;

	case ValueSlot:	
		return 1;

	case LeafSlot:	
		return slot->off;

	case Array4:
		node->radix4 = (ARTnode4 *)(Arena + slot->off * 8);
		children = 0;

		for( idx = 0; idx < slot->nslot; idx++ )
			children += ARTcount (node->radix4->radix + idx);
		
		if( node->radix4->value->type > 0 )
		  if( node->span->value->type == LeafSlot )
			children += node->span->value->off;
		  else
			children++;

		return children;

	case Array16:
		node->radix16 = (ARTnode16 *)(Arena + slot->off * 8);
		children = 0;

		for( idx = 0; idx < slot->nslot; idx++ )
			children += ARTcount (node->radix16->radix + idx);
		
		if( node->radix16->value->type > 0 )
		  if( node->span->value->type == LeafSlot )
			children += node->span->value->off;
		  else
			children++;

		return children;

	case Array48:
		node->radix48 = (ARTnode48 *)(Arena + slot->off * 8);
		children = 0;

		for( idx = 0; idx < slot->nslot; idx++ )
			children += ARTcount (node->radix48->radix + idx);
		
		if( node->radix48->value->type > 0 )
		  if( node->span->value->type == LeafSlot )
			children += node->span->value->off;
		  else
			children++;

		return children;

	case Array256:
		node->radix256 = (ARTnode256 *)(Arena + slot->off * 8);
		children = 0;

		for( idx = 0; idx < 256; idx++ )
			children += ARTcount (node->radix256->radix + idx);
		
		if( node->radix256->value->type > 0 )
		  if( node->span->value->type == LeafSlot )
			children += node->span->value->off;
		  else
			children++;

		return children;
	}

	return 0;
}

#ifdef STANDALONE
#include <time.h>
#include <sys/resource.h>

double getCpuTime(int type)
{
struct rusage used[1];
struct timeval tv[1];

	switch( type ) {
	case 0:
		gettimeofday(tv, NULL);
		return (double)tv->tv_sec + (double)tv->tv_usec / 1000000;

	case 1:
		getrusage(RUSAGE_SELF, used);
		return (double)used->ru_utime.tv_sec + (double)used->ru_utime.tv_usec / 1000000;

	case 2:
		getrusage(RUSAGE_SELF, used);
		return (double)used->ru_stime.tv_sec + (double)used->ru_stime.tv_usec / 1000000;
	}

	return 0;
}

typedef struct {
	char idx;
	char type;
	char *infile;
	ARTtrie *trie;
} ThreadArg;

#define ARTmaxkey 4096
#define ARTdepth 4096

//  standalone program to index file of keys
//  then list them onto std-out

void *index_file (void *arg)
{
int line = 0, cnt = 0, cachecnt, idx;
unsigned char key[ARTmaxkey];
struct random_data buf[1];
ulong offset, found = 0;
int len = 0, type = 0;
ThreadArg *args = arg;
ARTthread *thread;
uchar state[64];
int vallen, ch;
ARTslot *slot;
uint next[1];
ARTval *val;
uint size;
FILE *in;

	thread = ARTnewthread(args->trie, ARTdepth);

	switch(args->type | 0x20)
	{
	case 'c':	// count keys
		if( args->idx )
			break;
		fprintf(stderr, "started counting\n");
		found = ARTcount (args->trie->root);
		fprintf(stderr, "finished counting, found %ld keys\n", found);
		break;

	case '4':	// 4 byte random keys
		size = atoi(args->infile);
		memset (buf, 0, sizeof(buf));
		initstate_r(args->idx * 100 + 100, state, 64, buf);

		for( line = 0; line < size; line++ ) {
#ifdef SPARSE
			random_r(buf, next);
#else
			*next = line + size * args->idx;
#endif
			key[3] = next[0];
			next[0] >>= 8;
			key[2] = next[0];
			next[0] >>= 8;
			key[1] = next[0];
			next[0] >>= 8;
			key[0] = next[0];

			slot = ARTinsert (thread, key, 4);
			slot->type = LeafSlot;

			if( slot->off )
				found++;

			slot->off++;
			mutexrelease (slot->mutex);
		}

		fprintf(stderr, "finished inserting %d keys, duplicates %ld\n", line, found);
		break;

	case '8':	// 8 byte random keys of random length
		size = atoi(args->infile);
		memset (buf, 0, sizeof(buf));
		initstate_r(args->idx * 100 + 100, state, 64, buf);

		for( line = 0; line < size; line++ ) {
			random_r(buf, next);

			key[0] = next[0];
			next[0] >>= 8;
			key[1] = next[0];
			next[0] >>= 8;
			key[2] = next[0];
			next[0] >>= 8;
			key[3] = next[0];

			random_r(buf, next);

			key[4] = next[0];
			next[0] >>= 8;
			key[5] = next[0];
			next[0] >>= 8;
			key[6] = next[0];
			next[0] >>= 8;
			key[7] = next[0];

			slot = ARTinsert (thread, key, (line % 8) + 1);
			slot->type = LeafSlot;

			if( slot->off )
				found++;

			slot->off++;
			mutexrelease (slot->mutex);
		}

		fprintf(stderr, "finished inserting %d keys, duplicates %ld\n", line, found);
		break;

	case 'y':	// 8 byte random keys of random length
		size = atoi(args->infile);
		memset (buf, 0, sizeof(buf));
		initstate_r(args->idx * 100 + 100, state, 64, buf);

		for( line = 0; line < size; line++ ) {
			random_r(buf, next);

			key[0] = next[0];
			next[0] >>= 8;
			key[1] = next[0];
			next[0] >>= 8;
			key[2] = next[0];
			next[0] >>= 8;
			key[3] = next[0];

			random_r(buf, next);

			key[4] = next[0];
			next[0] >>= 8;
			key[5] = next[0];
			next[0] >>= 8;
			key[6] = next[0];
			next[0] >>= 8;
			key[7] = next[0];

			if( slot = ARTfindkey (thread, key, line % 8 + 1) )
				found++;
		}

		fprintf(stderr, "finished searching %d keys, found %ld\n", line, found);
		break;

	case 'x':	// find 4 byte random keys
		size = atoi(args->infile);
		memset (buf, 0, sizeof(buf));
		initstate_r(args->idx * 100 + 100, state, 64, buf);

		for( line = 0; line < size; line++ ) {
#ifdef SPARSE
			random_r(buf, next);
#else
			*next = line + size * args->idx;
#endif
			key[3] = next[0];
			next[0] >>= 8;
			key[2] = next[0];
			next[0] >>= 8;
			key[1] = next[0];
			next[0] >>= 8;
			key[0] = next[0];

			if( slot = ARTfindkey (thread, key, 4) )
				found++;
		}

		fprintf(stderr, "finished searching %d keys, found %ld\n", line, found);
		break;

	case 'd':
//		type = Delete;

	case 'p':
//		if( !type )
//			type = Unique;

//		 if( type == Delete )
//		  fprintf(stderr, "started pennysort delete for %s\n", args->infile);
//		 else
		  fprintf(stderr, "started pennysort insert for %s\n", args->infile);

		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;

			  offset = art_space (thread, len - 10 + sizeof(ARTval));
			  val = (ARTval *)(Arena + offset);
			  memcpy (val->value, key + 10, len - 10);
			  val->len = len - 10;

			  slot = ARTinsert (thread, key, 10);

			  if( slot->type == ValueSlot )
				  fprintf(stderr, "Duplicate key source: %d\n", line), exit(0);

			  slot->type = ValueSlot;
			  slot->off = offset >> 3;
			  mutexrelease (slot->mutex);

			  len = 0;
			  continue;
			}

		    else if( len < ARTmaxkey )
			  key[len++] = ch;

		fprintf(stderr, "finished %s for %d keys\n", args->infile, line);
		break;

	case 'w':
		fprintf(stderr, "started indexing for %s\n", args->infile);
		if( in = fopen (args->infile, "r") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;

			  slot = ARTinsert (thread, key, len);
			  slot->type = LeafSlot;
			  slot->off++;
			  mutexrelease (slot->mutex);
			  len = 0;
			}
			else if( len < ARTmaxkey )
				key[len++] = ch;

		fprintf(stderr, "finished %s for %d keys\n", args->infile, line);
		break;

	case 'f':
		fprintf(stderr, "started finding keys for %s\n", args->infile);
		if( in = fopen (args->infile, "rb") )
		  while( ch = getc(in), ch != EOF )
			if( ch == '\n' )
			{
			  line++;

			  if( slot = ARTfindkey (thread, key, len) )
				found++;

			  len = 0;
			}
			else if( len < ARTmaxkey )
				key[len++] = ch;

		fprintf(stderr, "finished %s for %d keys, found %ld\n", args->infile, line, found);
		break;

	case 's':
		if( args->idx )
			break;

		fprintf(stderr, "started forward scan\n");
		cnt = ARTscan (key, 0, sizeof(key), thread->trie->root);

		fprintf(stderr, " Total keys scanned %d\n", cnt);
		break;

	case 'r':
		fprintf(stderr, "started reverse scan\n");
		ARTlastkey (thread, NULL, 0);

		while( len = ARTprevkey (thread, key, ARTmaxkey) ) {
		  fwrite (key, len, 1, stdout);
		  val = thread->cursor->value;

		  if( val->len )
			fwrite (val->value, val->len, 1, stdout);

		  fputc ('\n', stdout);
		  cnt++;
	    }

		fprintf(stderr, " Total keys read %d\n", cnt);
		break;
	}

	ARTclosethread (thread);
	return NULL;
}

typedef struct timeval timer;

int main (int argc, char **argv)
{
pthread_t *threads;
int idx, cnt, err;
ThreadArg *args;
double start[3];
float elapsed;
ARTtrie *trie;
int fd, run;

	if( argc < 3 ) {
		fprintf (stderr, "Usage: %s idx_file cmds src_file1 src_file2 ... ]\n", argv[0]);
		fprintf (stderr, "  where idx_file is the name of the ARTful tree file\n");
		fprintf (stderr, "  cmds is a string of (r)ev scan/(w)rite/(s)can/(d)elete/(f)ind/(p)ennysort/(c)ount/(4)bit random keys, with the commands executed in sequence across the input files\n");
		fprintf (stderr, "  src_file1 thru src_filen are files of keys or pennysort records separated by newline\n");
		exit(0);
	}

	if( argc > 3 )
		cnt = argc - 3;
	else
		cnt = 0;

	threads = malloc (cnt * sizeof(pthread_t));
	args = malloc ((cnt + 1) * sizeof(ThreadArg));
#ifdef PERSIST
	fd = open ((char*)argv[1], O_RDWR | O_CREAT, 0666);

	if( fd == -1 ) {
		fprintf (stderr, "Unable to create/open ARTful file %s\n", argv[1]);
		exit (1);
	}
#else
	fd = -1;
#endif
	trie = ARTnew(fd);

	//	fire off threads for each command

	for( run = 0; run < strlen(argv[2]); run++ ) {
	 start[0] = getCpuTime(0);
	 start[1] = getCpuTime(1);
	 start[2] = getCpuTime(2);

	 if( cnt > 1 )
	  for( idx = 0; idx < cnt; idx++ ) {
		args[idx].infile = argv[idx + 3];
		args[idx].type = argv[2][run];
		args[idx].trie = trie;
		args[idx].idx = idx;

		if( err = pthread_create (threads + idx, NULL, index_file, args + idx) )
			fprintf(stderr, "Error creating thread %d\n", err);
	  }
	 else {
		args[0].infile = argv[3];
		args[0].type = argv[2][run];
		args[0].trie = trie;
		args[0].idx = 0;
		index_file (args);
	}

	// 	wait for termination

	 if( cnt > 1 )
	  for( idx = 0; idx < cnt; idx++ )
		pthread_join (threads[idx], NULL);

	 elapsed = getCpuTime(0) - start[0];
	 fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	 elapsed = getCpuTime(1) - start[1];
	 fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	 elapsed = getCpuTime(2) - start[2];
	 fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	}

	fprintf(stderr, " Total memory used %d MB\n", trie->arena_next/1000000);
	ARTclose (trie);

}
#endif	//STANDALONE
