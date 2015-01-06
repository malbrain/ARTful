ARTful
======

ARTful radix tree preliminary versions with initial support for inserts only.

ARTfulkv.c:	initial version with a latch for every slot in the radix trie.

ARTfulkv2.c: advanced multi-threaded version that only latches one slot per insert
ARTfulkv3.c: clean-up, move slot type to slot entry.

To compile the source code:
cc -c ARTfulkv3.c -o ARTfulkv3 -O2 -lpthread

To time 16M inserts of a 4 byte random key:
./ARTfulkv3 xyz 4 16000000

To time 16M inserts of a 4 byte random key with 4 threads:
./ARTfulkv3 xyz 4 4000000 4000000 4000000 4000000
