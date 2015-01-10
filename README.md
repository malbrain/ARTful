ARTful
======

ARTful radix tree preliminary version with initial support for inserts and finds only.

ARTfulkv.c:	initial version with a latch for every slot in the radix trie.

ARTfulkv2.c: advanced multi-threaded version that only latches one slot per insert
ARTfulkv3.c: clean-up, move slot type to slot entry. Revamp cmd interface.

To compile the source code:
cc -c ARTfulkv3.c -o ARTfulkv3 -O2 -lpthread

To time 16M inserts and finds of 4 byte random keys by a single thread:
./ARTfulkv3 xyz 4x 16000000

To time 16M inserts and finds of 4 byte random keys with 4 threads:
./ARTfulkv3 xyz 4x 4000000 4000000 4000000 4000000

To time sorting and subsequent listing of 10 byte key pennysort file segments:
./ARTfulkv3 xyz ps pennysort*

To time sorting and subsequent listing of an arbitrary file:
./ARTfulkv3 xyz ws filex
