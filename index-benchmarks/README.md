# OptiQL Index Benchmarks

## Build
```
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -jN
```

## Run with [PiBench](https://github.com/sfu-dis/pibench.git)
```
./_deps/pibench-build/src/PiBench ./wrappers/libbtreeomcs_leaf_offset_wrapper.so --mode=time --pcm=False --threads=40 --records=100000000 --seconds=10 --read_ratio=0.0 --update_ratio=1.0 --distribution=SELFSIMILAR --skew=0.2 --skip_verify=True --apply_hash=False
```

We use jemalloc for memory management. See [this](https://github.com/jemalloc/jemalloc/issues/1237) if you encounter similar issues.

## Run all benchmarks
```
../benchmarks/run.py
```
You might need to install several Python dependencies, including `numpy`, `pandas`, `matplotlib`, and `seaborn`.

## Wrappers
|      Wrapper name      |                              Content                              |
|:-----------------------|:------------------------------------------------------------------|
|    btreeolc_upgrade    | B+-tree with centralized optimistic locks                         |
|  btreeomcs_leaf_offset | B+-tree with OptiQL-NOR                                           |
| btreeomcs_leaf_op_read | B+-tree with OptiQL                                               |
|     artolc_upgrade     | ART with centralized optimistic locks                             |
|     artomcs_offset     | ART with OptiQL-NOR                                               |
|     artomcs_op_read    | ART with OptiQL                                                   |