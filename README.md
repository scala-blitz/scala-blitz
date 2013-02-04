

# Workstealing tree prototype

This project contains a prototype implementation of the workstealing tree data-structure and the workstealing tree scheduler for data-parallel operations.

It also contains experiments for the corresponding tech report and paper. This document summarizes the instructions for preparing and running the experiments. In general, different experiments are distributed across branches `experiment/XXX` -- to repeat the experiment cited in the paper, simply checkout the corresponding branch and run a corresponding python script as described below.


## Prerequisites

* JDK 1.7.0 - update 4 or later
* SBT 0.12.0 or newer
* Python 2.7.1 (tested with this version, but some other version may work as well)


## Experiments

This section contains instructions on running different experiments.
We assume that the command `sbt` runs SBT.


### Fixed size chunking vs. workstealing tree

To evaluate the effect of the `STEP` value on the speedup for the baseline kernel, run this experiment.

1. Checkout the branch `experiment/fixedsize`.
2. Run: `sbt clean`
3. Run: `scripts/measure-step-sizes.py sbt`

The script outputs a TikZ representation of the running time diagrams, which you can embed in a LaTeX
document or interpret the results directly.


### Fixed size chunking vs. workstealing tree

This experiment shows the effect of false sharing on the workstealing tree.
Here, the nodes of the workstealing tree are not padded to the cache line size.

1. Checkout the branch `experiment/fixedsize-nopadding`.
2. Run: `sbt clean`
3. Run: `scripts/measure-step-sizes.py sbt`


### Evaluation of workstealing tree traversal strategies

In this experiment we compare the effect of different strategies on the workstealing tree size.

1. Checkout the branch `experiment/strategies`.
2. Run: `sbt clean`
3. Run: `scripts/measure-strategies.py sbt`

The script outputs a TikZ representation of the throughput and tree size diagrams.


### Evaluation of the different workload kernels

In this experiment we compare the effect of different workload distributions on different schedulers.
The exact granularity and characteristics of each kernel are described in the paper.
You may also inspect the source code to learn more about them (see the file `Workloads.scala`).

1. Checkout the branch `experiment/coarsegrained`.
2. Run: `sbt clean`
3. Run: `scripts/measure-kernels.py sbt`

The script outputs the throughput diagrams for each kernel.


### Evaluation of the invocation overhead

In this experiment we study the effect of the invocation overhead on the overall running time for different range sizes.
We use the baseline workload, and output the throughput across different range sizes, as well as parallelism levels `P`.

1. Checkout the branch `experiment/overhead`.
2. Run: `sbt clean`
3. Run: `scripts/measure-invocation.py sbt`
















