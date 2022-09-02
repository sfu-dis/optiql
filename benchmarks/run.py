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

NUM_SOCKETS = 2
NUM_CORES = 20


class PiBenchExperiment:
    NUM_REPLICATES = 3
    if os.getenv('TEST'):
        NUM_REPLICATES = 1
    opTypes = ['Insert', 'Read', 'Update', 'Remove', 'Scan']
    finishTypes = ['completed', 'succeeded']

    def __init__(self, exp_name, index, wrapper_bin, dense, **kwargs):
        self.name = exp_name
        self.index = index
        self.wrapper_bin = wrapper_bin
        self.pibench_args = []
        self.results = []
        kwargs['skip_verify'] = True
        kwargs['apply_hash'] = not dense

        if os.getenv('TEST'):
            #kwargs['skip_verify'] = False
            kwargs['seconds'] = 3
        self.kwargs = kwargs
        for k, w in kwargs.items():
            self.pibench_args.append('--{}={}'.format(k, w))

        def get_numactl_command(threads):
            upto = (threads + NUM_CORES - 1) // NUM_CORES
            upto = min(upto, NUM_SOCKETS)
            sockets = ','.join(map(str, range(upto)))
            return ['numactl', f'--membind={sockets}']

        self.numactl = get_numactl_command(kwargs['threads'])

    def run(self, ith, idx, total):
        commands = [*self.numactl, PiBenchExperiment.pibench_bin,
                    self.wrapper_bin, *self.pibench_args]
        print(f'Executing ({idx}/{total}):', ' '.join(commands))
        result_text = None
        skipped = False
        if os.path.exists(os.path.join(f'{self.name}.raw', f'{self.index}-{self.kwargs["threads"]}-{ith}.out')):
            skipped = True
            print('Skipping')

        result = None
        if not skipped:
            done = False
            timeout = 10
            if self.kwargs['threads'] < 5:
                timeout = 60
            elif 5 <= self.kwargs['threads'] < 10:
                timeout = 30
            while not done:
                try:
                    result = subprocess.run(
                        commands, capture_output=True, text=True, timeout=timeout
                    )
                    done = True
                except subprocess.TimeoutExpired:
                    print(f'Timeout after {timeout} seconds, retrying...')
            result_text = result.stdout
        else:
            with open(os.path.join(f'{self.name}.raw', f'{self.index}-{self.kwargs["threads"]}-{ith}.out'), 'r') as f:
                result_text = f.read()

        def parse_output(text):
            results = pd.DataFrame(
                index=PiBenchExperiment.opTypes, columns=PiBenchExperiment.finishTypes)
            for opType in PiBenchExperiment.opTypes:
                for finishType in PiBenchExperiment.finishTypes:
                    pattern = r'\s+\-\s{}\s{}\:\s(.+)\sops'.format(
                        opType, finishType)
                    for line in text.split('\n'):
                        m = re.match(pattern, line)
                        if m:
                            throughput = float(m.group(1))
                            print('{} {} throughput: {}'.format(
                                opType, finishType, throughput))
                            results.loc[opType, finishType] = throughput
                            break
                    else:
                        print(
                            'Warning - {} {} throughput not found'.format(opType, finishType))
                        results.loc[opType, finishType] = float('NaN')

            return results

        self.results.append(parse_output(result_text))
        assert(ith == len(self.results))
        if not skipped:
            with open(os.path.join(f'{self.name}.raw', f'{self.index}-{self.kwargs["threads"]}-{ith}.out'), 'w') as f:
                f.writelines(result.stdout)


def run_all_experiments(name, dense=True, *args, **kwargs):
    # Don't move this file
    if not dense:
        name = f'{name}-sparse'
    else:
        name = f'{name}-dense'
    repo_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

    pibench_bin = os.path.join(
        repo_dir, 'build/_deps/pibench-build/src/PiBench')
    print('PiBench binary at:', pibench_bin)

    if not os.path.isfile(pibench_bin):
        print('PiBench binary not found')
        return

    if not os.access(pibench_bin, os.R_OK | os.W_OK | os.X_OK):
        print('PiBench binary permission denied')
        return

    PiBenchExperiment.pibench_bin = pibench_bin

    experiments = []

    indexes = ['btreeolc_upgrade', 'btreeolc', 'btreeolc_exp_backoff',
               'btreeomcs_leaf', 'btreeomcs_leaf_offset', 'btreeomcs_leaf_op_read',
               'artolc_upgrade', 'artolc', 'artolc_exp_backoff',
               'artomcs', 'artomcs_offset', 'artomcs_op_read']
    labels = ['B+Tree OptLock-NB', 'B+Tree OptLock-BL', 'B+Tree OptLock-BL-BO',
              'B+Tree OMCS-VMA', 'B+Tree OMCS', 'B+Tree OMCS+OpRead',
              'ART OptLock-NB', 'ART OptLock-BL', 'ART OptLock-BL BO',
              'ART OMCS-VMA', 'ART OMCS', 'ART OMCS+OpRead']
    btree_indexes = [index for index in indexes if 'btree' in index]
    art_indexes = [index for index in indexes if 'art' in index]
    btree_labels = [label for label in labels if 'B+Tree' in label]
    art_labels = [label for label in labels if 'ART' in label]

    assert(len(indexes) == len(labels))

    #threads = [1, 2, 5, 10, 15, 18, 20, 22, 25, 30, 32, 35, 40, 50, 60, 70, 80]
    threads = [1, 2, 5, 10, 16, 20, 30, 40, 50, 60, 70, 80]

    estimated_sec = prod(
        [len(indexes), len(threads), PiBenchExperiment.NUM_REPLICATES, kwargs['seconds']])
    print('Estimated time:', estimated_sec // 60, 'minutes')

    try:
        os.mkdir(f'{name}.raw')
    except:
        print(f'Warning: directory "{name}.raw" already exists')

    for index in indexes:
        wrapper_bin = os.path.join(
            repo_dir, 'build/wrappers/lib{}_wrapper.so'.format(index))
        for t in threads:
            experiments.append(PiBenchExperiment(
                name, index, wrapper_bin, dense, mode='time', pcm=False,
                threads=t, **kwargs))

    for i in range(PiBenchExperiment.NUM_REPLICATES):
        for j, exp in enumerate(experiments):
            exp.run(i + 1, i * len(experiments) + j + 1, len(experiments)
                    * PiBenchExperiment.NUM_REPLICATES)

    df_columns = ['index', 'thread', 'replicate'] + PiBenchExperiment.finishTypes + [
        f'{opType}-{finishType}' for opType in PiBenchExperiment.opTypes for finishType in PiBenchExperiment.finishTypes]
    objs = []

    for exp, (index, t) in zip(experiments, [(index, t) for index in indexes for t in threads]):
        for rid, results in enumerate(exp.results):
            exp_result_digest = results.sum(axis=0).to_numpy().tolist()
            exp_result_raw = results.to_numpy().flatten().tolist()
            row = pd.DataFrame(
                [[index, t, rid+1, *exp_result_digest, *exp_result_raw]], columns=df_columns)
            objs.append(row)

    df = pd.concat(objs, ignore_index=True)
    df.to_csv(f'{name}.csv')

    plotFinishType = 'succeeded'

    fig, ax = plt.subplots()
    fig.tight_layout()
    fig.set_size_inches(6, 4, forward=True)
    for index, label in zip(indexes, labels):
        plot_df = df[df['index'] == index]
        g = sns.lineplot(x='thread', y=plotFinishType,
                         data=plot_df, label=label, ax=ax,
                         err_style='bars', marker='.', ci='sd', markersize=10)

    def rstddev(x):
        return np.std(x, ddof=1) / np.mean(x) * 100
    df_digest = df.groupby(['index', 'thread']).agg(
        ['mean', 'min', 'max', rstddev])[plotFinishType]
    print(df_digest)
    df_digest.to_csv(f'{name}-digest.csv')

    g.set_xticks(threads)
    g.set(xlabel='Number of threads')
    g.set(ylabel='Throughput')
    ax.grid(axis='y', alpha=0.4)
    ax.axvspan(20, 40, facecolor='0.2', alpha=0.10)
    ax.axvspan(40, 80, facecolor='0.2', alpha=0.20)
    plt.savefig(f'{name}.pdf', format='pdf', bbox_inches='tight')

    fig = pygal.Line()
    fig.title = f'{name}'
    fig.x_labels = [None] + threads

    for latch, label in zip(indexes, labels):
        data = [None]
        for thread in threads:
            mean = df_digest['mean'][latch][thread]
            min = df_digest['min'][latch][thread]
            max = df_digest['max'][latch][thread]
            if df_digest['rstddev'][latch][thread] < 1.0:
                # Ignore error bar if too small
                data.append({'value': mean})
            else:
                data.append({'value': mean, 'ci': {'low': min, 'high': max}})

        fig.add(label, data)

    fig.render_to_file(f'{name}.svg')


if __name__ == '__main__':
    NUM_RECORDS = 100_000_000
    SECONDS = 10

    run_all_experiments('Update-only-selfsimilar', records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.0, update_ratio=1.0, distribution='SELFSIMILAR', skew=0.2)
    run_all_experiments('Update-only-uniform', records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.0, update_ratio=1.0, distribution='UNIFORM')

    run_all_experiments('Read-only-uniform', records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=1.0, distribution='UNIFORM')
    run_all_experiments('Read-only-selfsimilar', records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=1.0, distribution='SELFSIMILAR', skew=0.2)

    run_all_experiments('Write-heavy-uniform', records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.2, update_ratio=0.8, distribution='UNIFORM')
    run_all_experiments('Write-heavy-selfsimilar', records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.2, update_ratio=0.8, distribution='SELFSIMILAR', skew=0.2)

    run_all_experiments('Read-heavy-uniform', records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.8, update_ratio=0.2, distribution='UNIFORM')
    run_all_experiments('Read-heavy-selfsimilar', records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.8, update_ratio=0.2, distribution='SELFSIMILAR', skew=0.2)

    run_all_experiments('Balanced-uniform', records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.5, update_ratio=0.5, distribution='UNIFORM')
    run_all_experiments('Balanced-selfsimilar', records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.5, update_ratio=0.5, distribution='SELFSIMILAR', skew=0.2)
