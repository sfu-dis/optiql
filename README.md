## OptiQL: Optimistic Queuing Lock

OptiQL is an optimistic lock that offers both high read performance and robustness under contention by extending the classic queue-based MCS locks with optimistic reads.  

See details in our [SIGMOD 2024 paper](https://www.cs.sfu.ca/~tzwang/optiql.pdf) below. If you use our work, please cite:

```
OptiQL: Robust Optimistic Locking for Memory-Optimized Indexes.
Ge Shi, Ziyi Yan and Tianzheng Wang.
Proceedings of the ACM on Management of Data, Vol 1, No. 3 (SIGMOD 2024)
```

See you in [Santiago](https://2024.sigmod.org/)!

### Included in this repository:
This repository implements OptiQL and several other baseline locks, along with index use cases (B+-tree and ART) that use OptiQL in optimistic lock coupling. See sub-directories for:
* Index benchmarks:  under `index-benchmarks` directory 
* Microbenchmarks: under `latch-microbenchmarks` directory
* Plotting scripts: under `plots` directory
