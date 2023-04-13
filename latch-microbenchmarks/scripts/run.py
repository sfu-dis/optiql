#!/usr/bin/env python3

from math import prod
import os
import subprocess
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pygal

pd.options.display.max_columns = None
pd.options.display.max_rows = None

NUM_CORES = 20
NUM_SOCKETS = 2


def relative_stddev(x):
    return np.std(x, ddof=1) / np.mean(x) * 100


class LatchExperiment:
    NUM_REPLICATES = 10

    def __init__(self, name, latch, bench_bin, **kwargs):
        self.bench_bin = bench_bin
        self.name = name
        self.latch = latch
        self.threads = kwargs['threads']
        self.args = []
        self.results = []
        self.raw_outputs = []
        for k, w in kwargs.items():
            self.args.append('--{}={}'.format(k, w))

        def get_numactl_command(threads):
            upto = (threads + NUM_CORES - 1) // NUM_CORES
            upto = min(upto, NUM_SOCKETS)
            sockets = ','.join(map(str, range(upto)))
            return ['numactl', f'--membind={sockets}']

        self.numactl = get_numactl_command(kwargs['threads'])

    def run(self, replica, idx, total):
        commands = [*self.numactl, self.bench_bin, *self.args]

        print(f'Executing ({idx}/{total}):', ' '.join(commands))
        result_text = None
        fname = "{}-{}-{}-{}-stdout.txt".format(
            self.name, self.latch, self.threads, replica)
        if os.path.exists(fname):
            print('Skipping')
            with open(fname) as f:
                result_text = f.read()
        else:
            result = subprocess.run(
                commands, capture_output=True, text=True
            )
            with open(fname, "w") as f:
                f.write(result.stdout)
            result_text = result.stdout

        def parse_output(text):
            pattern = r'(.+),(.+),(.+),(.+),(.+)'
            ops = []
            suc = []
            rs = []
            rsucs = []
            for line in text.split('\n'):
                m = re.match(pattern, line)
                if m:
                    if m.group(1) == "All":
                        operations = float(m.group(2))
                        successes = float(m.group(3))
                        reads = float(m.group(4))
                        read_successes = float(m.group(5))
                        return (operations,
                                successes,
                                reads,
                                read_successes,
                                np.std(ops, ddof=1), np.mean(
                                    ops), min(ops), max(ops),
                                np.std(suc, ddof=1), np.mean(
                                    suc), min(suc), max(suc),
                                np.std(rs, ddof=1), np.mean(
                                    rs), min(rs), max(rs),
                                np.std(rsucs, ddof=1), np.mean(
                                    rsucs), min(rsucs), max(rsucs),
                                )
                    elif m.group(1) != "Thread":
                        ops.append(float(m.group(2)))
                        suc.append(float(m.group(3)))
                        rs.append(float(m.group(4)))
                        rsucs.append(float(m.group(5)))
            print('Warning: results not found')
            return (np.nan, np.nan, np.nan, np.nan)

        self.results.append(parse_output(result_text))


def run_all_experiments(latches, name, threads, *args, **kwargs):
    # Don't move this file
    repo_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

    experiments = []

    labels = latches

    estimated_sec = prod(
        [len(latches), len(threads), LatchExperiment.NUM_REPLICATES, kwargs['seconds']])
    print('Estimated time:', estimated_sec // 60, 'minutes')

    for latch in latches:
        bench_bin = os.path.join(
            repo_dir, 'build/benchmarks/{}_bench'.format(latch))

        if not os.path.isfile(bench_bin):
            print('binary not found')
            return

        if not os.access(bench_bin, os.R_OK | os.W_OK | os.X_OK):
            print('binary permission denied')
            return

        for t in threads:
            experiments.append(LatchExperiment(
                name, latch, bench_bin, threads=t, **kwargs))

    for i in range(LatchExperiment.NUM_REPLICATES):
        for j, exp in enumerate(experiments):
            exp.run(i + 1, i * len(experiments) + j + 1, len(experiments)
                    * LatchExperiment.NUM_REPLICATES)

    df_columns = ['latch', 'thread', 'replicate',
                  'operations',
                  'successes',
                  "reads",
                  "read_successes",
                  "stddev_operations", "mean_operations", "min_operations", "max_operations",
                  "stddev_successes", "mean_successes", "min_successes", "max_successes",
                  "stddev_reads", "mean_reads", "min_reads", "max_reads",
                  "stddev_read_successes", "mean_read_successes", "min_read_successes", "max_read_successes",
                  ]
    objs = []

    for exp, (latch, t) in zip(experiments, [(latch, t) for latch in latches for t in threads]):
        for rid, results in enumerate(exp.results):
            row = pd.DataFrame(
                [[latch, t, rid + 1, *results]], columns=df_columns)
            objs.append(row)

    df = pd.concat(objs, ignore_index=True)
    df.to_csv(f'{name}.csv')

    fig, ax = plt.subplots()
    fig.tight_layout()
    fig.set_size_inches(16, 6, forward=True)
    for latch, label in zip(latches, labels):
        plot_df = df[df['latch'] == latch]
        g = sns.lineplot(x='thread', y='successes',
                         data=plot_df, label=label, ax=ax,
                         err_style='bars', marker='.', ci='sd', markersize=10)

    df_digest = df.groupby(['latch', 'thread']).agg(
        ['mean', 'min', 'max', relative_stddev])['successes']
    print(df_digest)
    df_digest.to_csv(f'{name}-digest.csv')

    g.set_xticks(threads)
    g.set(xlabel='Number of threads')
    g.set(ylabel='Throughput')
    ax.grid(axis='y', alpha=0.4)
    ax.axvspan(20, 40, facecolor='0.2', alpha=0.05)
    ax.axvspan(40, 80, facecolor='0.2', alpha=0.10)
    ax.axvspan(80, 160, facecolor='0.2', alpha=0.15)

    plt.savefig(f'{name}.pdf', format='pdf', bbox_inches='tight')

    g.set(xscale='log')
    g.set_xticks(threads, labels=threads)
    plt.savefig(f'{name}-logscale.pdf', format='pdf', bbox_inches='tight')

    fig = pygal.Line()
    fig.title = f'{name}'
    fig.x_labels = [None] + threads

    for latch, label in zip(latches, labels):
        data = [None]
        for thread in threads:
            mean = df_digest['mean'][latch][thread]
            min = df_digest['min'][latch][thread]
            max = df_digest['max'][latch][thread]
            if df_digest['relative_stddev'][latch][thread] < 1.0:
                # Ignore error bar if too small
                data.append({'value': mean})
            else:
                data.append({'value': mean, 'ci': {'low': min, 'high': max}})

        fig.add(label, data)

    fig.render_to_file(f'{name}.svg')


rw_latches = ['optlock_st', 'omcs_offset', 'omcs_offset_op_read_numa_qnode',
              'stdrw', 'mcsrw_offset']
wo_latches = ['tatas_st', 'mcs']

if __name__ == '__main__':
    SECONDS = 10
    cs_cycles = 50
    ps_cycles = 0

    for (r, w) in [(0, 100)]:
        latches = rw_latches + wo_latches
        threads = [1, 2, 5, 10, 20, 30, 40, 60, 80]

        run_all_experiments(latches, 'Latch-FIXED-R{}-W{}'.format(r, w), threads, array_size=256, seconds=SECONDS,
                            ver_read_pct=r, acq_rel_pct=w, dist='fixed', cs_cycles=cs_cycles, ps_cycles=ps_cycles)

        run_all_experiments(latches, 'Latch-1-Max-R{}-W{}'.format(r, w), threads, array_size=1, seconds=SECONDS,
                            ver_read_pct=r, acq_rel_pct=w, dist='uniform', cs_cycles=cs_cycles, ps_cycles=ps_cycles)
        run_all_experiments(latches, 'Latch-Low-1M-R{}-W{}'.format(r, w), threads, array_size=1048576, seconds=SECONDS,
                            ver_read_pct=r, acq_rel_pct=w, dist='uniform', cs_cycles=cs_cycles, ps_cycles=ps_cycles)
        MEDIUM_SIZE = 30000
        run_all_experiments(latches, 'Latch-Medium-{}-R{}-W{}'.format(MEDIUM_SIZE, r, w), threads, array_size=MEDIUM_SIZE, seconds=SECONDS,
                            ver_read_pct=r, acq_rel_pct=w, dist='uniform', cs_cycles=cs_cycles, ps_cycles=ps_cycles)
        run_all_experiments(latches, 'Latch-High-5-R{}-W{}'.format(r, w), threads, array_size=5, seconds=SECONDS,
                            ver_read_pct=r, acq_rel_pct=w, dist='uniform', cs_cycles=cs_cycles, ps_cycles=ps_cycles)

    for (r, w) in [(20, 80), (50, 50), (80, 20), (90, 10)]:  # [(95, 5), (100, 0)]
        latches = rw_latches
        threads = [80]

        run_all_experiments(latches, 'Latch-1-Max-R{}-W{}'.format(r, w), threads, array_size=1, seconds=SECONDS,
                            ver_read_pct=r, acq_rel_pct=w, dist='uniform', cs_cycles=cs_cycles, ps_cycles=ps_cycles)
        run_all_experiments(latches, 'Latch-Low-1M-R{}-W{}'.format(r, w), threads, array_size=1048576, seconds=SECONDS,
                            ver_read_pct=r, acq_rel_pct=w, dist='uniform', cs_cycles=cs_cycles, ps_cycles=ps_cycles)
        MEDIUM_SIZE = 30000
        run_all_experiments(latches, 'Latch-Medium-{}-R{}-W{}'.format(MEDIUM_SIZE, r, w), threads, array_size=MEDIUM_SIZE, seconds=SECONDS,
                            ver_read_pct=r, acq_rel_pct=w, dist='uniform', cs_cycles=cs_cycles, ps_cycles=ps_cycles)
        run_all_experiments(latches, 'Latch-High-5-R{}-W{}'.format(r, w), threads, array_size=5, seconds=SECONDS,
                            ver_read_pct=r, acq_rel_pct=w, dist='uniform', cs_cycles=cs_cycles, ps_cycles=ps_cycles)
