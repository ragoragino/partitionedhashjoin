Necessary requirements:
- Boost, GoogleTest, Google Benchmark, XXHash and Mimalloc 

Work in progress

TODO (11.7.):
- cpu cache misses measuring + hash functions?
- add support for GetAll (i.e. non unique keys)
- create custom memory allocator for hash algorithms (memory can be reused)
- write unit tests (e.g. ThreadPool)
- add some benchmarks
- investigate perf of SeparateChaining vs LinearProbing


https://stackoverflow.com/questions/21917529/is-it-possible-to-initialize-stdvector-over-already-allocated-memory
https://stackoverflow.com/questions/9755316/does-it-make-sense-to-use-stduninitialized-fill-with-any-allocator/9757971#9757971
https://upcoder.com/6/custom-vector-allocation