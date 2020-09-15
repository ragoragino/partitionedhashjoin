# partitionedhashjoin

This project aims to be a base for replication of selected results from:
"Blanas, Spyros, Yinan Li, and Jignesh M. Patel. "Design and evaluation of main memory hash join algorithms for multi-core CPUs." Proceedings of the 2011 ACM SIGMOD International Conference on Management of data. 2011."

## Getting Started

This project provides comparison of hash join algorithms on artificially generated OLAP-like datasets (with only key and value fields). 
The algorithms currently implemented are no partitioning hash join and radix partitioning join (as described by the study). 
Each program run is a separate execution of the selected algorithm. See phjoin --help for some more information about provided configuration options.

Project defines a variable that specifies whether to use mimalloc library for overriding malloc calls:
___PHJ_USE_MIMALLOC___ - CMAKE option that specifies whether to use Mimalloc or not.\
___PHJ_MIMALLOC_DLL_DIR___ - if PHJ_USE_MIMALLOC is ON and OS is Windows, then this CMake variable must be set to the directory containing all DLLs required by Mimalloc.

### Prerequisites

- Boost 1.7.2+
- GTest 1.11.0+
- XXHash 0.7+
- CMake 3.8+

### Installing

```bash
mkdir build
cd build
cmake ..
cmake --build .
```

## Running the tests

Small Google Test suite is provided.

## Built and tested with

- MSVC++ 14.25
- Boost 1.7.2
- GTest 1.11.0
- XXHash 0.7
- CMake 3.8

## License

This project is licensed under the MIT License.

## Results

TODO

## Improvements
- add support for Linux builds
- add support for cpu cache misses measurements
- add support for GetAll (i.e. non unique keys)