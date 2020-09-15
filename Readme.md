Necessary requirements:
- Boost, GTest and XXHash

PHJ_USE_MIMALLOC - CMAKE option that specifies whether to use Mimalloc or not
PHJ_MIMALLOC_DLL_DIR - if PHJ_USE_MIMALLOC is set to true and OS is Windows, then this must be set to the directory containing all DLLs required by Mimalloc

TODO (15.9.):
- cpu cache misses measuring + hash functions?
- add support for GetAll (i.e. non unique keys)
- create custom memory allocator for hash algorithms (memory can be reused)
- write unit tests (e.g. ThreadPool)
- investigate perf of SeparateChaining vs LinearProbing
- make good description
- clang format

https://stackoverflow.com/questions/21917529/is-it-possible-to-initialize-stdvector-over-already-allocated-memory
https://stackoverflow.com/questions/9755316/does-it-make-sense-to-use-stduninitialized-fill-with-any-allocator/9757971#9757971
https://upcoder.com/6/custom-vector-allocation