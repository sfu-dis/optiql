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
