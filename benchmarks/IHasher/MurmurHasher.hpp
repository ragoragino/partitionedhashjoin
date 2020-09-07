#pragma once

#include <benchmark/benchmark.h>

#include <iostream>
#include <memory>
#include <utility>

#include "Common/MurmurHasher.hpp"

static void Benchmark_MurmurHasher(benchmark::State& state) {
    size_t counter = state.range(0);
    for (auto _ : state) {
        Common::MurmurHasher hasher{};
        for (size_t i = 0; i != counter; i++) {
            benchmark::DoNotOptimize(hasher.Hash(i, counter));
        }
    };
}

BENCHMARK(Benchmark_MurmurHasher)->Arg(10000000)->Unit(benchmark::kMillisecond);