//	ARTful key-value store

//	Author: Karl Malbrain, malbrain@cal.berkeley.edu
//	Date:   31 DEC 14

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
	SpanNode,		// node contains key bytes (up to 16) and leaf element
	Array4,			// node contains 4 radix slots & leaf element
	Array16,		// node contains 16 radix slots & leaf element
	Array64,		// node contains 64 radix slots & leaf element
	Array256		// node contains 256 radix slots & leaf element
};

typedef union {
  struct {
	ulong off:48;		// offset to node sub-contents
	uchar mutex[1];		// update/write synchronization
	uchar nslot:7;		// number of slots of node in use
	uchar leaf:1;		// this slot is a leaf node pointer
  };
  ulong bits;
} ARTslot;

typedef struct {
	ulong value:48;		// offset of leaf value that ended before this node
	uchar type;			// type of ARTful node
	uchar fill;			// filler
} ARTgeneric;

typedef struct {
	ulong value:48;		// offset of leaf value that ended before this node
	uchar type;			// type of ARTful node
	uchar fill;			// filler
	ARTslot radix[4];
	uchar keys[4];
} ARTnode4;

typedef struct {
	ulong value:48;		// offset of leaf value that ended before this node
	uchar type;			// type of ARTful node
	uchar fill;			// filler
	ARTslot radix[16];
	uchar keys[16];
} ARTnode16;

typedef struct {
	ulong value:48;		// offset of leaf value that ended before this node
	uchar type;			// type of ARTful node
	uchar fill;			// filler
	ARTslot radix[64];
	uchar keys[256];
} ARTnode64;

typedef struct {
	ulong value:48;		// offset of leaf value that ended before this node
	uchar type;			// type of ARTful node
	uchar fill;			// filler
	ARTslot radix[256];
} ARTnode256;

typedef struct {
	ulong value:48;		// offset of leaf value that ended before this node
	uchar type;			// type of ARTful node
	uchar fill;			// filler
	ARTslot next[1];	// next node under span
	uchar bytes[8];
} ARTspan;

typedef struct {
	ulong value:48;		// offset of leaf value that ended before this node
	uchar type;			// type of ARTful node
	uchar fill;			// filler
} ARTleaf;

typedef struct {
	ARTslot root[1];
	ulong arena_size;	// size of Arena File
	ulong arena_next;	// next available offset
	uchar arena_mutex[1];
} ARTtrie;

typedef struct {
	uchar len;			// this can be changed to a ushort or uint
	uchar value[0];
} ARTval;

typedef struct {
	ARTslot *slot;		// current slot
	uint off;			// offset within key
	int idx;			// current index within slot
} ARTstack;

typedef struct {
	uint maxdepth;		// maximum depth of ARTful trie
	uint depth;			// current depth of cursor
	ARTval *value;		// current leaf node
	ARTstack stack[0];	// cursor stack
} ARTcursor;

typedef struct {
	ulong base;			// base of arena assigned to thread
	ulong offset;		// next offset of chunk to allocate
	ARTtrie *trie;		// ARTful trie
	ARTcursor *cursor;	// thread cursor
} ARTthread;

#define relax() asm volatile("pause\n": : : "memory")

void mutexlock(uchar *volatile latch)
{
  while( __sync_lock_test_and_set (latch, 1) )
	while( latch[0] )
		relax();
}

void mutexrelease(uchar *latch)
{
	__sync_synchronize();
	*latch = 0;
}

//  Maximum Arena size, e.g. virtual memory size

ulong ArenaVM = 1024UL * 1024UL*1024UL *12;

//	Initial/Incremental Arena file size

ulong ArenaInit = 1024UL*1024UL *100;

uchar *Arena;
int ArenaFd;		// arena file descriptor

//	incremental amount to allocate to threads

#define ARENA_chunk (1024 * 1024)

void art_free (ARTtrie *trie, uchar type, void *what)
{
}

ulong art_space (ARTthread *thread, uint size)
{
ulong offset;

	if( !thread->offset || thread->offset + size > ARENA_chunk ) {
	  mutexlock (thread->trie->arena_mutex);
	  if( thread->trie->arena_next + ARENA_chunk > thread->trie->arena_size ) {
		thread->trie->arena_next = thread->trie->arena_size;
		thread->trie->arena_size += ArenaInit;
		ftruncate (ArenaFd, thread->trie->arena_size);
	  }
	  thread->base = thread->trie->arena_next;
	  thread->offset = 0;
	  thread->trie->arena_next += ARENA_chunk;
	  mutexrelease (thread->trie->arena_mutex);
	}

	offset = thread->offset + thread->base;
	memset (Arena + offset, 0, size);
	thread->offset += size;
	return offset;
}

ulong art_node (ARTthread *thread, uchar type)
{
uint size, xtra;

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
	case Array64:
	  size = sizeof(ARTnode64);
	  break;
	case Array256:
	  size = sizeof(ARTnode256);
	  break;
	}

	if( xtra = size & 0x7 )
		size += 8 - xtra;

	if( xtra = thread->offset & 0x7 )
		thread->offset += 8 - xtra;

	return art_space (thread, size);
}

ARTthread *ARTnewthread (ARTtrie *trie, uint depth)
{
ARTcursor *cursor = calloc (1, sizeof(ARTcursor) + depth * sizeof(ARTstack));
ARTthread *thread = calloc (1, sizeof(ARTthread));

	cursor->maxdepth = depth;
	thread->cursor = cursor;
	thread->trie = trie;
	return thread;
}

ARTtrie *ARTnew (int fd)
{
int flag = PROT_READ | PROT_WRITE;
ARTnode256 *radix256, *root256;
ARTtrie *trie;
ulong offset;
uint i;

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

	if( !trie->arena_next ) {
	  trie->arena_next = sizeof(ARTtrie);
	  root256 = (ARTnode256 *)(Arena + trie->arena_next);
	  trie->root->off = trie->arena_next;
	  trie->arena_next += sizeof(ARTnode256);
	  root256->type = Array256;

	  for( i = 0; i < 256; i++ ) {
		radix256 = (ARTnode256 *)(Arena + trie->arena_next);
		root256->radix[i].off = trie->arena_next;
		trie->arena_next += sizeof(ARTnode256);
		radix256->type = Array256;
	  }

	  trie->arena_next = ARENA_chunk;
	}

	return trie;
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

//  find key in ARTful trie, returning current value or NULL

ulong ARTfindkey (ARTthread *thread, uchar *key, uint keylen)
{
uint len, idx, off;
ARTnode4 *radix4;
ARTnode16 *radix16;
ARTnode64 *radix64;
ARTnode256 *radix256;
ARTgeneric *generic;
ulong oldvalue;
ARTslot *slot;
ARTspan *span;

	off = 0;
	slot = thread->trie->root;

	while( off < keylen ) {
	  if( slot->off )
		generic = (ARTgeneric *)(Arena + slot->off);
	  else
		return 0;

	  if( slot->leaf || !generic->type )
		return 0;

	  switch( generic->type ) {
	  case SpanNode:
		span = (ARTspan*)(Arena + slot->off);
		len = keylen - off;

		if( len < slot->nslot )
			return 0;

		if( memcmp (key + off, span->bytes, slot->nslot) )
			return 0;

		off += slot->nslot;
		slot = span->next;
		continue;

	  case Array4:
		radix4 = (ARTnode4 *)(Arena + slot->off);
		len = slot->nslot;

		for( idx = 0; idx < len; idx++ )
		  if( key[off] == radix4->keys[idx] )
			break;

		if( idx == len )
		  return 0;

		slot = radix4->radix + idx;
		off++;
		continue;

	  case Array16:
		radix16 = (ARTnode16 *)(Arena + slot->off);
		len = slot->nslot;

		for( idx = 0; idx < len; idx++ )
		  if( key[off] == radix16->keys[idx] )
			break;

		if( idx == len )
		  return 0;

		slot = radix16->radix + idx;
		off++;
		continue;

	  case Array64:
		radix64 = (ARTnode64 *)(Arena + slot->off);

		if( radix64->keys[key[off]] == 0xff )
		  return 0;

		slot = radix64->radix + radix64->keys[key[off++]];
		continue;

	  case Array256:
		radix256 = (ARTnode256 *)(Arena + slot->off);
		slot = radix256->radix + key[off++];
		continue;
	  }
	}

	if( slot->leaf )
	  return slot->off;

	generic = (ARTgeneric *)(Arena + slot->off);
	return generic->value;
}

//	insert key/value into ARTful trie, returning old value offset

ulong ARTinsert (ARTthread *thread, uchar *key, uint keylen, ulong valueoffset)
{
ARTslot *prev, node[1], *lock, *slot;
ARTspan *span, *span1, *span2;
uint len, idx, max, off;
uchar slot64, *update64;
ARTnode4 *radix4;
ARTnode16 *radix16;
ARTnode64 *radix64;
ARTnode256 *radix256;
ARTgeneric *generic;
ulong oldvalue;

	slot = thread->trie->root;
	lock = NULL;
	prev = NULL;
	off = 0;

	while( off < keylen ) {
	  node->bits = slot->bits;
	  generic = (ARTgeneric *)(Arena + node->off);
	  update64 = NULL;
	  prev = slot;

	  if( !node->leaf )
	   if( !lock && generic->type < Array256 ) {
		mutexlock (prev->mutex);
		*node->mutex = 1;

		if( node->bits != prev->bits ) {
		  mutexrelease (prev->mutex);
		  continue;
		}

		lock = prev;
	   }

	  if( !node->leaf )
	   switch( generic->type ) {
	   case SpanNode:
		span = (ARTspan*)(Arena + node->off);
		max = len = node->nslot;

		if( len > keylen - off )
			len = keylen - off;

		for( idx = 0; idx < len; idx++ )
		  if( key[off] != span->bytes[idx] )
			break;
		  else
			off++;

		// did we use the entire span node?

		if( idx == max ) {
		  slot = span->next;
		  continue;
		}

		// copy prefix bytes to a new span node

		if( idx ) {
		  span1 = (ARTspan *)(Arena + art_node(thread, SpanNode));
		  memcpy (span1->bytes, span->bytes, idx);
		  node->off = (uchar *)span1 - Arena;
		  span1->type = SpanNode;
		  slot = span1->next;
		  node->nslot = idx;
		}

		// else cut span from the tree by transforming
		// the original node into the radix4 node

		else
		  slot = node;

		// does our key terminate after the span node?

		if( off == keylen )
		  continue;

		// break span node into two parts
		// with radix node in between

		radix4 = (ARTnode4 *)(Arena + art_node(thread, Array4));
		radix4->keys[0] = span->bytes[idx];
		radix4->type = Array4;

		slot->off = (uchar *)radix4 - Arena;
		slot->nslot = 1 + (off < keylen);

		slot = radix4->radix + 0;	// fill in first radix element

		// are there any span bytes remaining?
		// place them under first radix branch
		// in a new span node

		if( idx + 1 < max ) {
		  span2 = (ARTspan *)(Arena + art_node(thread, SpanNode));
		  memcpy (span2->bytes, span->bytes + idx + 1, max - idx - 1);
		  span2->type = SpanNode;
		  slot->nslot = max - idx - 1;
		  slot->off = (uchar *)span2 - Arena;
		  *span2->next = *span->next;
		} else
		  *slot = *span->next;

		// does our key terminate at the radix node?

		if( off == keylen )
		  continue;

		// otherwise there are two radix elements

		slot = radix4->radix + 1;	// second radix element
		radix4->keys[1] = key[off++];
		break;

	   case Array4:
		radix4 = (ARTnode4*)(Arena + node->off);

		for( idx = 0; idx < node->nslot; idx++ )
		  if( key[off] == radix4->keys[idx] )
			break;

		if( idx < node->nslot ) {
		  slot = radix4->radix + idx;
		  off++;
		  continue;
		}

		// add to radix node if room

		if( node->nslot < 4 ) {
		  radix4->keys[node->nslot] = key[off++];
		  slot = radix4->radix + node->nslot++;
		  break;
		}

		// the radix node is full, promote to
		// the next larger size.

		radix16 = (ARTnode16 *)(Arena + art_node(thread, Array16));

		for( idx = 0; idx < node->nslot; idx++ ) {
		  radix16->radix[idx] = radix4->radix[idx];
		  radix16->keys[idx] = radix4->keys[idx];
		}

		radix16->keys[node->nslot] = key[off++];
		radix16->value = radix4->value;
		radix16->type = Array16;

		node->off = (uchar *)radix16 - Arena;
		slot = radix16->radix + node->nslot++;
		break;

	   case Array16:
		radix16 = (ARTnode16*)(Arena + node->off);

		for( idx = 0; idx < node->nslot; idx++ )
		  if( key[off] == radix16->keys[idx] )
			break;

		// is key byte in radix node

		if( idx < node->nslot ) {
		  slot = radix16->radix + idx;
		  off++;
		  continue;
		}

		// add to radix node if room

		if( node->nslot < 16 ) {
		  radix16->keys[node->nslot] = key[off++];
		  slot = radix16->radix + node->nslot++;
		  break;
		}

		// the radix node is full, promote to
		// the next larger size.

		radix64 = (ARTnode64 *)(Arena + art_node(thread, Array64));
		memset (radix64->keys, 0xff, sizeof(radix64->keys));

		for( idx = 0; idx < node->nslot; idx++ ) {
		  slot = radix16->radix + idx;
		  radix64->radix[idx] = *slot;
		  radix64->keys[radix16->keys[idx]] = idx;
		}

		node->off = (uchar *)radix64 - Arena;
		radix64->keys[key[off++]] = node->nslot;
		radix64->value = radix16->value;
		radix64->type = Array64;

		slot = radix64->radix + node->nslot++;
		break;

	   case Array64:
		radix64 = (ARTnode64*)(Arena + node->off);

		// is key already in radix node?

		if( radix64->keys[key[off]] < 0xff ) {
		  slot = radix64->radix + radix64->keys[key[off++]];
		  continue;
		}

		// add to radix node

		if( node->nslot < 64 ) {
		  update64 = radix64->keys + key[off++];
		  slot64 = node->nslot++;
		  slot = radix64->radix + slot64;
		  break;
		}

		// the radix node is full, promote to
		// the next larger size.

		radix256 = (ARTnode256 *)(Arena + art_node(thread, Array256));

		for( idx = 0; idx < 256; idx++ )
		 if( radix64->keys[idx] < 0xff ) {
		  slot = radix64->radix + radix64->keys[idx];
		  radix256->radix[idx] = *slot;
		 }

		node->off = (uchar *)radix256 - Arena;
		radix256->value = radix64->value;
		radix256->type = Array256;

		slot = radix256->radix + key[off++];
		break;

	   case Array256:
		radix256 = (ARTnode256*)(Arena + node->off);
		slot = radix256->radix + key[off++];
		continue;

	 	// execution from case Array256 above
		// will continue here on an empty slot

	   case UnusedNode:
		slot = node;
		break;
	   }

	  // fill in empty/leaf slot with remaining key bytes

	  if( node->leaf )
		oldvalue = node->off;
	  else
		oldvalue = 0;

	  // copy key bytes to span nodes

	  while( len = keylen - off ) {
		span = (ARTspan *)(Arena + art_node(thread, SpanNode));
		span->value = oldvalue;
		span->type = SpanNode;
		oldvalue = 0;

		if( len > sizeof(span->bytes) )
		  len = sizeof(span->bytes);

		memcpy (span->bytes, key + off, len);
		slot->off = (uchar *)span - Arena;
		slot->nslot = len;
		slot = span->next;
		off += len;
	  }

	  slot->off = valueoffset;
	  slot->leaf = 1;

	  *node->mutex = 0;
	  prev->bits = node->bits;

	  if( update64 )
		*update64 = slot64;

	  if( lock && lock != prev )
		mutexrelease (lock->mutex);

	  return oldvalue;
	}

	// set the leaf offset in the node

	if( !slot->leaf ) {
		generic = (ARTgeneric *)(Arena + slot->off);
		oldvalue = generic->value;
		generic->value = valueoffset;
	} else {
		oldvalue = slot->off;
		slot->off = valueoffset;
	}

	if( lock )
	  mutexrelease (lock->mutex);

	return oldvalue;
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
	char *type;
	char *infile;
	ARTtrie *trie;
} ThreadArg;

#define ARTmaxkey 256
#define ARTdepth 256

//  standalone program to index file of keys
//  then list them onto std-out

void *index_file (void *arg)
{
int line = 0, found = 0, cnt = 0, cachecnt, idx;
int ch, len = 0, slot, type = 0;
unsigned char key[ARTmaxkey];
struct random_data buf[1];
ThreadArg *args = arg;
ARTthread *thread;
uint counts[8][2];
uchar state[64];
uint next[1];
ulong offset;
int vallen;
ARTval *val;
uint size;
FILE *in;

	if( args->idx < strlen (args->type) )
		ch = args->type[args->idx];
	else
		ch = args->type[strlen(args->type) - 1];

	thread = ARTnewthread(args->trie, ARTdepth);

	switch(ch | 0x20)
	{
	case '4':	// 4 byte random keys
		memset (buf, 0, sizeof(buf));
		initstate_r(args->idx * 100 + 100, state, 64, buf);
		for( line = 0; line < 16000000; line++ ) {
		random_r(buf, next);

			key[0] = next[0];
			next[0] >>= 8;
			key[1] = next[0];
			next[0] >>= 8;
			key[2] = next[0];
			next[0] >>= 8;
			key[3] = next[0];
			ARTinsert (thread, key, 4, 0);
		}
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

			  if( ARTinsert (thread, key, 10, offset) )
				  fprintf(stderr, "Duplicate key source: %d\n", line), exit(0);
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

			  ARTinsert (thread, key, len, 1);
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
			  if( ARTfindkey (thread, key, len) )
				found++;
			  len = 0;
			}
			else if( len < ARTmaxkey )
				key[len++] = ch;
		fprintf(stderr, "finished %s for %d keys, found %d\n", args->infile, line, found);
		break;

	case 's':
		fprintf(stderr, "started forward scan\n");
		ARTstartkey (thread, NULL, 0);

		while( len = ARTnextkey (thread, key, ARTmaxkey) ) {
		  fwrite (key, len, 1, stdout);
		  val = thread->cursor->value;
		  if( val->len )
			fwrite (val->value, val->len, 1, stdout);
		  fputc ('\n', stdout);
		  cnt++;
	    }

		fprintf(stderr, " Total keys read %d\n", cnt);
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

	return NULL;
}

typedef struct timeval timer;

int main (int argc, char **argv)
{
double start, stop;
pthread_t *threads;
int idx, cnt, err;
ThreadArg *args;
float elapsed;
void *trie;
int fd;

	if( argc < 3 ) {
		fprintf (stderr, "Usage: %s idx_file cmds src_file1 src_file2 ... ]\n", argv[0]);
		fprintf (stderr, "  where idx_file is the name of the ARTful tree file\n");
		fprintf (stderr, "  cmds is a string of (r)ev scan/(w)rite/(s)can/(d)elete/(f)ind/(p)ennysort/(c)ount/(m)ainflush/(a)udit, with a one character command for each input src_file. A command can also be given with no input file\n");
		fprintf (stderr, "  src_file1 thru src_filen are files of keys or pennysort records separated by newline\n");
		exit(0);
	}

	start = getCpuTime(0);

	if( argc > 3 )
		cnt = argc - 3;
	else
		cnt = 0;

	threads = malloc (cnt * sizeof(pthread_t));
	args = malloc ((cnt + 1) * sizeof(ThreadArg));

	fd = open ((char*)argv[1], O_RDWR | O_CREAT, 0666);

	if( fd == -1 ) {
		fprintf (stderr, "Unable to create/open ARTful file %s\n", argv[1]);
		exit (1);
	}

	trie = ARTnew(fd);

	//	fire off threads

	if( cnt > 1 )
	  for( idx = 0; idx < cnt; idx++ ) {
		args[idx].infile = argv[idx + 3];
		args[idx].type = argv[2];
		args[idx].trie = trie;
		args[idx].idx = idx;

		if( err = pthread_create (threads + idx, NULL, index_file, args + idx) )
			fprintf(stderr, "Error creating thread %d\n", err);
	  }
	else {
		args[0].infile = argv[3];
		args[0].type = argv[2];
		args[0].trie = trie;
		args[0].idx = 0;
		index_file (args);
	}

	// 	wait for termination

	if( cnt > 1 )
	  for( idx = 0; idx < cnt; idx++ )
		pthread_join (threads[idx], NULL);

	ARTclose (trie);

	elapsed = getCpuTime(0) - start;
	fprintf(stderr, " real %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(1);
	fprintf(stderr, " user %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
	elapsed = getCpuTime(2);
	fprintf(stderr, " sys  %dm%.3fs\n", (int)(elapsed/60), elapsed - (int)(elapsed/60)*60);
}
#endif	//STANDALONE
