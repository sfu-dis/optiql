# Latch Microbenchmarks

## Build

```bash
mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && make -j<number-of-threads> && cd ..
```

## Run

```bash
./scripts/run.py
./scripts/run-cs-length.py
```
