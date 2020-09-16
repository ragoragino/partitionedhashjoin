# partitionedhashjoin

This project aims to be a base for replication of selected results from:
"Blanas, Spyros, Yinan Li, and Jignesh M. Patel. "Design and evaluation of main memory hash join algorithms for multi-core CPUs." Proceedings of the 2011 ACM SIGMOD International Conference on Management of data. 2011."

## Getting Started

This project provides a comparison of hash join algorithms on artificially generated OLAP-like datasets (with only key and value fields). 
The algorithms currently implemented are no-partitioning hash join and radix-partitioning hash join (as described by the study). 
Each program run provides a separate execution of the selected algorithm. Current implementation does not materialize join results (only counts matched keys).
See phjoin --help for some more information about provided configuration options.

Project defines a variable that specifies whether to use mimalloc library for overriding malloc calls:\
___PHJ_USE_MIMALLOC___ - CMAKE option that specifies whether to use Mimalloc or not.\
___PHJ_MIMALLOC_DLL_DIR___ - if PHJ_USE_MIMALLOC is ON and OS is Windows, then this CMake variable must be set to the directory containing all DLLs required by Mimalloc.

Also one can build GTest suite by setting the option:\
___PHJ_BUILD_TESTS___

### Prerequisites

- Boost 1.7.2+
- GTest 1.11.0+
- XXHash 0.7.4+
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

Compilers:
- MSVC 14.25
- GCC 7.4.0

Libraries:
- Boost 1.7.2
- GTest 1.11.0
- XXHash 0.7.4
- CMake 3.8

## License

This project is licensed under the MIT License.

## Results

See directory results for details (results were obtained with default relation size values, i.e. 10 million tuples for the primary and 200 million tuples for the secondary relation).

![alt text](https://github.com/ragoragino/partitionedhashjoin/blob/master/results/1.05/figure.png?raw=true)
![alt text](https://github.com/ragoragino/partitionedhashjoin/blob/master/results/1.25/figure.png?raw=true)

## Improvements
- add support for measuring CPU cache misses