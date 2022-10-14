# OptiQL Index Benchmark Plots

## Generate all plots
You need to firstly run all benchmarks (except for latency sampling ones).
```
../../index-benchmarks/benchmarks/run.py
```
Then
```
./collapse.py
./plot-skewed-scalability.py
./plot-uniform-scalability.py
./plot-skewed-scalability-sparse.py
```
Finally, you can run all latency sampling benchmarks and then generate the latency plot:
```
./plot-latency.py
```