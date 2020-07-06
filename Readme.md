Necessary requirements:
- Boost, GoogleTest, Google Benchmark, XXHash and Mimalloc 

Work in progress

TODO (6.7.):
- make HashJoin parametrized on hash algoritms
- create custom memory allocator for hash algorithms (memory can be reused)
- write unit tests (e.g. ThreadPool)
- add some benchmarks
- investigate perf of SeparateChaining vs LinearProbing
