#!/usr/bin/env python3

from math import prod
import os
import subprocess
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import sys

pd.options.display.max_columns = None
pd.options.display.max_rows = None

base_repo_dir = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))
sys.path.append(base_repo_dir)
from common.numa import NUM_SOCKETS, NUM_CORES

data_dir = os.path.join(base_repo_dir, 'plots', 'index', 'data')


class PiBenchExperiment:
    NUM_REPLICATES = 20
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
        if 'stdrw' in self.index or 'mcsrw' in self.index:
            commands.append('--bulk_load')
        print(f'Executing ({idx}/{total}):', ' '.join(commands))
        result_text = None
        skipped = False
        if os.path.exists(os.path.join(data_dir, f'{self.name}.raw', f'{self.index}-{self.kwargs["threads"]}-{ith}.out')):
            skipped = True
            print('Skipping')

        result = None
        if not skipped:
            result = subprocess.run(
                commands, capture_output=True, text=True,
            )
            result_text = result.stdout
        else:
            with open(os.path.join(data_dir, f'{self.name}.raw', f'{self.index}-{self.kwargs["threads"]}-{ith}.out'), 'r') as f:
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
            with open(os.path.join(data_dir, f'{self.name}.raw', f'{self.index}-{self.kwargs["threads"]}-{ith}.out'), 'w') as f:
                f.writelines(result.stdout)


gl_df_columns = ['exp', 'name'] + [f'{opType}-ratio' for opType in PiBenchExperiment.opTypes] + ['key-type', 'distribution', 'skew-factor'] + ['index', 'thread', 'replicate'] + PiBenchExperiment.finishTypes + [
    f'{opType}-{finishType}' for opType in PiBenchExperiment.opTypes for finishType in PiBenchExperiment.finishTypes]

dataframe = pd.DataFrame(columns=gl_df_columns)


def run_all_experiments(expname, name, indexes, labels, threads, dense=True, *args, **kwargs):
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

    btree_indexes = [index for index in indexes if 'btree' in index]
    art_indexes = [index for index in indexes if 'art' in index]
    btree_labels = [label for label in labels if 'B+Tree' in label]
    art_labels = [label for label in labels if 'ART' in label]

    assert(len(indexes) == len(labels))

    estimated_sec = prod(
        [len(indexes), len(threads), PiBenchExperiment.NUM_REPLICATES, kwargs['seconds']])
    print('Estimated time:', estimated_sec //
          60, 'minutes (excluding load time)')

    try:
        os.mkdir(os.path.join(data_dir, f'{name}.raw'))
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
    df.to_csv(os.path.join(data_dir, f'{expname}-{name}.csv'))

    plotFinishType = 'succeeded'

    # fig, ax = plt.subplots()
    # fig.tight_layout()
    # fig.set_size_inches(6, 4, forward=True)
    # for index, label in zip(indexes, labels):
    #     plot_df = df[df['index'] == index]
    #     g = sns.lineplot(x='thread', y=plotFinishType,
    #                      data=plot_df, label=label, ax=ax,
    #                      err_style='bars', marker='.', ci='sd', markersize=10)

    def rstddev(x):
        return np.std(x, ddof=1) / np.mean(x) * 100
    df_digest = df.groupby(['index', 'thread']).agg(
        ['mean', 'min', 'max', rstddev])[plotFinishType]
    print(df_digest)
    df_digest.to_csv(os.path.join(data_dir, f'{expname}-{name}-digest.csv'))

    # g.set_xticks(threads)
    # g.set(xlabel='Number of threads')
    # g.set(ylabel='Throughput')
    # ax.grid(axis='y', alpha=0.4)
    # ax.axvspan(20, 40, facecolor='0.2', alpha=0.10)
    # ax.axvspan(40, 80, facecolor='0.2', alpha=0.20)
    # plt.savefig(os.path.join(data_dir, f'{name}.pdf'), format='pdf', bbox_inches='tight')

    # fig = pygal.Line()
    # fig.title = f'{name}'
    # fig.x_labels = [None] + threads

    # for latch, label in zip(indexes, labels):
    #     data = [None]
    #     for thread in threads:
    #         mean = df_digest['mean'][latch][thread]
    #         min = df_digest['min'][latch][thread]
    #         max = df_digest['max'][latch][thread]
    #         if df_digest['rstddev'][latch][thread] < 1.0:
    #             # Ignore error bar if too small
    #             data.append({'value': mean})
    #         else:
    #             data.append({'value': mean, 'ci': {'low': min, 'high': max}})

    #     fig.add(label, data)

    # fig.render_to_file(os.path.join(data_dir, f'{name}.svg'))

    insert_ratio = 0.0
    read_ratio = 1.0
    update_ratio = 0.0
    remove_ratio = 0.0
    scan_ratio = 0.0
    if 'insert_ratio' in kwargs:
        insert_ratio = kwargs['insert_ratio']
    if 'read_ratio' in kwargs:
        read_ratio = kwargs['read_ratio']
    if 'update_ratio' in kwargs:
        update_ratio = kwargs['update_ratio']
    if 'remove_ratio' in kwargs:
        remove_ratio = kwargs['remove_ratio']
    if 'scan_ratio' in kwargs:
        scan_ratio = kwargs['scan_ratio']

    df['exp'] = expname
    df['name'] = name
    df['Insert-ratio'] = insert_ratio
    df['Read-ratio'] = read_ratio
    df['Update-ratio'] = update_ratio
    df['Remove-ratio'] = remove_ratio
    df['Scan-ratio'] = scan_ratio

    if kwargs['distribution'] == 'UNIFORM':
        df['distribution'] = 'uniform'
        df['skew-factor'] = 0
    elif kwargs['distribution'] == 'SELFSIMILAR':
        df['distribution'] = 'selfsimilar'
        df['skew-factor'] = kwargs['skew']
    else:
        raise ValueError
    if dense:
        df['key-type'] = 'dense-int'
    else:
        df['key-type'] = 'sparse-int'

    df.reindex(columns=gl_df_columns)
    global dataframe
    dataframe = dataframe.append(df, ignore_index=True)


if __name__ == '__main__':
    NUM_RECORDS = 100_000_000
    SECONDS = 10

    try:
        os.makedirs(data_dir)
    except:
        print(f'Raw data directory already exists')

    # collapse
    indexes = [
        'btreeolc_upgrade',
        'btreeomcs_leaf_op_read',
    ]
    labels = [
        'B+-tree OptLock',
        'B+-tree OptiQL',
    ]
    #threads = [1, 2, 5, 10, 15, 18, 20, 22, 25, 30, 32, 35, 40, 50, 60, 70, 80]
    threads = [1, 2, 5, 10, 16, 20, 30, 40, 50, 60, 70, 80]
    # dense
    run_all_experiments('scalability', 'Update-only-uniform', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.0, update_ratio=1.0, distribution='UNIFORM')
    run_all_experiments('scalability', 'Update-only-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.0, update_ratio=1.0, distribution='SELFSIMILAR', skew=0.2)

    # scalability, read-only or read-write
    indexes = [
        'btreeolc_upgrade',
        'btreeomcs_leaf_offset',
        'btreeomcs_leaf_op_read',
        'btreelc_stdrw',
        'btreelc_mcsrw',
        'artolc_upgrade',
        'artomcs_offset',
        'artomcs_op_read',
        'artlc_stdrw',
        'artlc_mcsrw',
        'bwtree',
    ]
    labels = [
        'B+-tree OptLock',
        'B+-tree OptiQL-NOR',
        'B+-tree OptiQL',
        'B+-tree STDRW',
        'B+-tree MCSRW',
        'ART OptLock',
        'ART OptiQL-NOR',
        'ART OptiQL',
        'ART STDRW',
        'ART MCSRW',
        'Bw-Tree',
    ]
    # dense
    # run_all_experiments('scalability', 'Read-only-uniform', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
    #                     read_ratio=1.0, distribution='UNIFORM')
    run_all_experiments('scalability', 'Read-only-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=1.0, distribution='SELFSIMILAR', skew=0.2)

    # run_all_experiments('scalability', 'Write-heavy-uniform', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
    #                     read_ratio=0.2, update_ratio=0.8, distribution='UNIFORM')
    run_all_experiments('scalability', 'Write-heavy-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.2, update_ratio=0.8, distribution='SELFSIMILAR', skew=0.2)

    # run_all_experiments('scalability', 'Read-heavy-uniform', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
    #                     read_ratio=0.8, update_ratio=0.2, distribution='UNIFORM')
    run_all_experiments('scalability', 'Read-heavy-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.8, update_ratio=0.2, distribution='SELFSIMILAR', skew=0.2)

    run_all_experiments('scalability', 'Balanced-uniform', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.5, update_ratio=0.5, distribution='UNIFORM')
    run_all_experiments('scalability', 'Balanced-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.5, update_ratio=0.5, distribution='SELFSIMILAR', skew=0.2)

    # scalability, update-only (remainder from collapse)
    indexes = [
        'btreeomcs_leaf_offset',
        'btreelc_stdrw',
        'btreelc_mcsrw',
        'artolc_upgrade',
        'artomcs_offset',
        'artomcs_op_read',
        'artlc_stdrw',
        'artlc_mcsrw',
        'bwtree',
    ]
    labels = [
        'B+-tree OptiQL-NOR',
        'B+-tree STDRW',
        'B+-tree MCSRW',
        'ART OptLock',
        'ART OptiQL-NOR',
        'ART OptiQL',
        'ART STDRW',
        'ART MCSRW',
        'Bw-Tree',
    ]
    # dense
    # run_all_experiments('scalability', 'Update-only-uniform', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
    #                     read_ratio=0.0, update_ratio=1.0, distribution='UNIFORM')
    run_all_experiments('scalability', 'Update-only-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.0, update_ratio=1.0, distribution='SELFSIMILAR', skew=0.2)

    # sparse, skewed
    indexes = [
        'artolc_upgrade',
        'artomcs_offset',
        'artomcs_op_read',
        'artlc_stdrw',
        'artlc_mcsrw',
    ]
    labels = [
        'ART OptLock',
        'ART OptiQL-NOR',
        'ART OptiQL',
        'ART STDRW',
        'ART MCSRW',
    ]
    # run_all_experiments('scalability', 'Update-only-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS, dense=False,
    #                     read_ratio=0.0, update_ratio=1.0, distribution='SELFSIMILAR', skew=0.2)
    # run_all_experiments('scalability', 'Read-only-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS, dense=False,
    #                     read_ratio=1.0, distribution='SELFSIMILAR', skew=0.2)
    run_all_experiments('scalability', 'Write-heavy-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS, dense=False,
                        read_ratio=0.2, update_ratio=0.8, distribution='SELFSIMILAR', skew=0.2)
    run_all_experiments('scalability', 'Read-heavy-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS, dense=False,
                        read_ratio=0.8, update_ratio=0.2, distribution='SELFSIMILAR', skew=0.2)
    # run_all_experiments('scalability', 'Balanced-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS, dense=False,
    #                     read_ratio=0.5, update_ratio=0.5, distribution='SELFSIMILAR', skew=0.2)

    # page size, dense, skewed
    index_bases = [
        'btreeolc_upgrade',
        'btreeomcs_leaf_offset',
        'btreeomcs_leaf_op_read',
        'btreeomcs_leaf_op_read_new_api',
    ]
    page_size_suffices = ['', '_512', '_1K', '_2K', '_4K', '_8K', '_16K']
    indexes = [
        index + suffix for index in index_bases for suffix in page_size_suffices]
    label_bases = [
        'B+-Tree OptLock',
        'B+-Tree OptiQL-NOR',
        'B+-Tree OptiQL',
        'B+-Tree OptiQL New API',
    ]
    labels = [
        label + suffix for label in label_bases for suffix in page_size_suffices]
    threads = [40]

    run_all_experiments('page-size', 'Write-heavy-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.2, update_ratio=0.8, distribution='SELFSIMILAR', skew=0.2)
    run_all_experiments('page-size', 'Read-heavy-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.8, update_ratio=0.2, distribution='SELFSIMILAR', skew=0.2)
    run_all_experiments('page-size', 'Balanced-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.5, update_ratio=0.5, distribution='SELFSIMILAR', skew=0.2)

    # qnode pool
    indexes = [
        'btreeomcs_leaf_op_read_gnp',
        'artomcs_op_read_gnp',
    ]
    labels = [
        'B+-tree OptiQL-GNP',
        'ART OptiQL-GNP',
    ]
    threads = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80]
    # dense
    run_all_experiments('scalability', 'Update-only-selfsimilar', indexes, labels, threads, records=NUM_RECORDS, seconds=SECONDS,
                        read_ratio=0.0, update_ratio=1.0, distribution='SELFSIMILAR', skew=0.2)

    dataframe.to_csv(os.path.join(data_dir, 'All.csv'))
