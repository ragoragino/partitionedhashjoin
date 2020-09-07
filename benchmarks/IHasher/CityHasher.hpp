#pragma once

#include <benchmark/benchmark.h>

#include <iostream>
#include <memory>
#include <utility>

#include "Common/CityHasher.hpp"

static void Benchmark_CityHasher(benchmark::State& state) {
    size_t counter = state.range(0);
    for (auto _ : state) {
        Common::CityHasher hasher{};
        for (size_t i = 0; i != counter; i++) {
            benchmark::DoNotOptimize(hasher.Hash(i, counter));
        }
    };
}

BENCHMARK(Benchmark_CityHasher)->Arg(10000000)->Unit(benchmark::kMillisecond);