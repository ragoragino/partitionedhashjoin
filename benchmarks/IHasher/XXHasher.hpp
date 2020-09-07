#pragma once

#include <benchmark/benchmark.h>

#include <iostream>
#include <memory>
#include <utility>

#include "Common/XXHasher.hpp"

static void Benchmark_XXHasher(benchmark::State& state) {
    size_t counter = state.range(0);
    for (auto _ : state) {
        Common::XXHasher hasher{};
        for (size_t i = 0; i != counter; i++) {
            benchmark::DoNotOptimize(hasher.Hash(i, counter));
        }
    };
}

BENCHMARK(Benchmark_XXHasher)->Arg(10000000)->Unit(benchmark::kMillisecond);