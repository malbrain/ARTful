ARTful
======

ARTful radix tree preliminary versions with initial support for inserts only.

ARTfulkv.c:	initial version with a latch for every slot in the radix trie.

ARTfulkv2.c: advanced multi-threaded version that only latches one slot per insert

To compile the source code:
cc -c ARTfulkv2.c -o ARTfulkv2 -lpthread

To compare 16M inserts of a 4 byte random key:
./ARTfulkv2 xyz 4

To compare 16M inserts of a 4 byte random key with 4 threads:
./ARTfulkv2 xyz 4 a b c d
