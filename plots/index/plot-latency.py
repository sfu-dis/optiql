#!/usr/bin/env python3

from calendar import day_abbr
from math import prod
import os
import subprocess
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import MaxNLocator
import matplotlib.patches as mpatches
import seaborn as sns
import numpy as np
from IPython import embed

pd.options.display.max_columns = None
pd.options.display.max_rows = None

NUM_SOCKETS = 2
NUM_CORES = 20


base_repo_dir = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))
data_dir = os.path.join(base_repo_dir, 'plots', 'index', 'data')
try:
    os.makedirs(data_dir)
except:
    print(f'Raw data directory already exists')


indexes = ['btreeolc_upgrade', 'btreeolc_exp_backoff',
           'btreeomcs_leaf_offset', 'btreeomcs_leaf_op_read',
           'artolc_upgrade', 'artolc_exp_backoff',
           'artomcs_offset', 'artomcs_op_read']
labels = ['B+Tree OptLock-NB', 'B+Tree OptLock-BL-BO',
          'B+Tree OMCS', 'B+Tree OMCS+OpRead',
          'ART OptLock-NB', 'ART OptLock-BL BO',
          'ART OMCS', 'ART OMCS+OpRead']
btree_indexes = [index for index in indexes if 'btree' in index]
art_indexes = [index for index in indexes if 'art' in index]
btree_labels = [label for label in labels if 'B+Tree' in label]
art_labels = [label for label in labels if 'ART' in label]
latch_labels = ['OptLock', 'OptLock-BO', 'OptiQL--', 'OptiQL']

# threads = [1, 2, 5, 10, 15, 18, 20, 22, 25, 30, 32, 35, 40, 50, 60, 70, 80]
threads = [1, 2, 5, 10, 16, 20, 30, 40, 50, 60, 70, 80]

x_labels = [1, 20, 40, 60, 80]

latency_quantiles = ['min', '50%', '90%', '99%', '99.9%', '99.99%', '99.999%']


class PiBenchExperiment:
    NUM_REPLICATES = 1
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
            # kwargs['skip_verify'] = False
            kwargs['seconds'] = 5
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
        if not os.getenv('PLOT'):
            print(f'Executing ({idx}/{total}):', ' '.join(commands))
        result_text = None
        skipped = False
        if os.path.exists(os.path.join(data_dir, f'{self.name}.raw', f'{self.index}-{self.kwargs["threads"]}-{ith}.out')):
            skipped = True
            if not os.getenv('PLOT'):
                print('Skipping')

        result = None
        if not skipped:
            done = False
            timeout = 30
            if self.kwargs['threads'] < 5:
                timeout = 60
            elif 5 <= self.kwargs['threads'] <= 10:
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
            with open(os.path.join(data_dir, f'{self.name}.raw', f'{self.index}-{self.kwargs["threads"]}-{ith}.out'), 'r') as f:
                result_text = f.read()

        def parse_latency_output(text):
            results = pd.DataFrame(columns=latency_quantiles)
            for quantile in latency_quantiles:
                pattern = r'\s+{}\:\s(.+)'.format(quantile)
                for line in text.split('\n'):
                    m = re.match(pattern, line)
                    if m:
                        latency = int(m.group(1))
                        if not os.getenv('PLOT'):
                            print('{} latency: {}'.format(
                                quantile, latency))
                        results.loc[0, quantile] = latency
                        break
                else:
                    print(
                        'Warning - {} latency not found'.format(quantile))
                    results.loc[quantile] = float('NaN')

            return results

        self.results.append(parse_latency_output(result_text))
        ith = len(self.results)
        if not skipped:
            with open(os.path.join(data_dir, f'{self.name}.raw', f'{self.index}-{self.kwargs["threads"]}-{ith}.out'), 'w') as f:
                f.writelines(result.stdout)


gl_df_columns = ['name'] + [f'{opType}-ratio' for opType in PiBenchExperiment.opTypes] + [
    'key-type', 'distribution', 'skew-factor'] + ['index', 'thread', 'replicate'] + latency_quantiles

dataframe_dense = pd.DataFrame(columns=gl_df_columns)
dataframe_sparse = pd.DataFrame(columns=gl_df_columns)


def run_all_experiments(name, dense=True, *args, **kwargs):
    # Don't move this file
    if not dense:
        name = f'{name}-sparse'
    else:
        name = f'{name}-dense'
    repo_dir = os.path.join(base_repo_dir, 'index-benchmarks')

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

    assert(len(indexes) == len(labels))

    estimated_sec = prod(
        [len(indexes), len(threads), PiBenchExperiment.NUM_REPLICATES, kwargs['seconds']])
    print('Estimated time:', estimated_sec // 60, 'minutes')

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

    df_columns = ['index', 'thread', 'replicate'] + latency_quantiles
    objs = []

    for exp, (index, t) in zip(experiments, [(index, t) for index in indexes for t in threads]):
        for rid, results in enumerate(exp.results):
            results['index'] = index
            results['thread'] = t
            results['replicate'] = rid + 1
            results.reindex(columns=df_columns)
            objs.append(results)

    df = pd.concat(objs, ignore_index=True)
    df.to_csv(os.path.join(data_dir, f'{name}-latency.csv'))

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
    global dataframe_dense
    dataframe_dense = dataframe_dense.append(df, ignore_index=True)


if __name__ == '__main__':
    NUM_RECORDS = 100_000_000
    SECONDS = 10

    df_fname = os.path.join(data_dir, 'All-latency.csv')
    # df_fname_sparse = os.path.join(data_dir, 'All-latency-sparse.csv')
    if not os.path.exists(df_fname):
        # TODO: sparse
        run_all_experiments('Update-only-selfsimilar-latency', records=NUM_RECORDS, seconds=SECONDS,
                            read_ratio=0.0, update_ratio=1.0, distribution='SELFSIMILAR', skew=0.2, latency_sampling=0.1)
        run_all_experiments('Update-only-uniform-latency', records=NUM_RECORDS, seconds=SECONDS,
                            read_ratio=0.0, update_ratio=1.0, distribution='UNIFORM', latency_sampling=0.1)
        run_all_experiments('Read-only-selfsimilar-latency', records=NUM_RECORDS, seconds=SECONDS,
                            read_ratio=1.0, distribution='SELFSIMILAR', skew=0.2, latency_sampling=0.1)
        run_all_experiments('Read-only-uniform-latency', records=NUM_RECORDS, seconds=SECONDS,
                            read_ratio=1.0, distribution='UNIFORM', latency_sampling=0.1)
        run_all_experiments('Balanced-selfsimilar-latency', records=NUM_RECORDS, seconds=SECONDS,
                            read_ratio=0.5, update_ratio=0.5, distribution='SELFSIMILAR', skew=0.2, latency_sampling=0.1)
        run_all_experiments('Balanced-uniform-latency', records=NUM_RECORDS, seconds=SECONDS,
                            read_ratio=0.5, update_ratio=0.5, distribution='UNIFORM', latency_sampling=0.1)
        dataframe_dense.to_csv(df_fname)
    else:
        dataframe_dense = pd.read_csv(df_fname).iloc[:, 1:]
        # dataframe_sparse = pd.read_csv(df_fname_sparse).iloc[:, 1:]

    # start plotting
    def plot_latency(key_type='dense-int'):
        nrows = 2
        ncols = 6

        dataframe = None
        if key_type == 'dense-int':
            dataframe = dataframe_dense
        else:
            dataframe = dataframe_sparse

        markers = ['v', '^', 's', 'o', '*', 'd', '>', 'P', 'd', 'h']

        indexes = [btree_indexes, art_indexes]
        labels = [btree_labels, art_labels]
        distributions = ['selfsimilar', 'selfsimilar']
        skew_factors = [0.2, 0.2]
        ylabels = ['B+-Tree', 'ART']

        titles = ['Read-only\n20 threads', 'Read-only\n40 threads',
                  'Balanced\n20 threads', 'Balanced\n40 threads',
                  'Update-only\n20 threads', 'Update-only\n40 threads']
        read_ratios = [1.0, 1.0, 0.5, 0.5, 0.0, 0.0]
        update_ratios = [0.0, 0.0, 0.5, 0.5, 1.0, 1.0]
        threads = [20, 40, 20, 40, 20, 40]
        ylims = [15000, 15000, 45000, 75000, 75000, 75000]

        fig, axs = plt.subplots(nrows, ncols)
        fig.set_size_inches(9, 1.8, forward=True)
        fig.subplots_adjust(wspace=0.25)  # space between subfigs

        for r in range(nrows):
            for c in range(ncols):
                ax = axs[r, c]

                df = dataframe[(dataframe['key-type'] == key_type)
                               & (dataframe['index'].isin(indexes[r]))
                               & (dataframe['distribution'] == distributions[r])
                               & (dataframe['skew-factor'] == skew_factors[r])
                               & (dataframe['Read-ratio'] == read_ratios[c])
                               & (dataframe['Update-ratio'] == update_ratios[c])
                               & (dataframe['thread'] == threads[c])]
                df1 = df[['thread', 'index', *latency_quantiles]]
                for i, (index, label) in enumerate(zip(indexes[r], labels[r])):
                    data = df1[df1['index'] ==
                               index][latency_quantiles]
                    data.columns = ["min", "50\%", "90\%",
                                    "99\%", "99.9\%", "99.99\%", "99.999\%"]
                    data = data.transpose()
                    data = data.reset_index(level=0)
                    data = data.set_axis(['quantile', 'latency'], axis=1)

                    sns.lineplot(data=data, x='quantile', y='latency',
                                 marker=markers[i], markersize=6,
                                 linewidth=1, markeredgecolor='black', markeredgewidth=0.3,
                                 ax=ax, label=latch_labels[i], legend=False)

                ax.yaxis.set_major_locator(MaxNLocator(3))
                ax.grid(axis='y', alpha=0.4)

                if r == 0:
                    ax.set_title(titles[c], fontsize=8)
                ax.set_ylim([0, ylims[c]])
                ax.set_xlabel("")
                ax.set_ylabel("")
                ax.set_xlim([None, "99.999\%"])
                if c == 0:
                    ax.set_ylabel(ylabels[r])

                for tick in ax.get_xticklabels():
                    tick.set_rotation(45)
                    tick.set_fontsize(7)

                ticks_y = ticker.FuncFormatter(
                    lambda x, pos: '{0:g}'.format(x/1e3))
                ax.yaxis.set_major_formatter(ticks_y)

        lines, _ = axs[0, 0].get_legend_handles_labels()
        fig.legend(lines, latch_labels, loc='upper right', bbox_to_anchor=(
            0.75, 1.25), ncol=len(latch_labels), frameon=False)

        fig.text(0.01, 0.5, "Latency (µs)", va='center', rotation='vertical')

        fig.subplots_adjust(left=0.08, right=0.98, bottom=0.05,
                            top=0.9, hspace=0.62, wspace=0.25)
        filename = 'Latency.pdf' if key_type == 'dense-int' else 'Latency-sparse.pdf'
        plt.savefig(filename, format='pdf',
                    bbox_inches='tight', pad_inches=0)

    # plt.rcParams.update({'font.size': 8})
    plt.rcParams['text.usetex'] = True
    plt.rcParams['text.latex.preamble'] = '\\usepackage{libertine}\n\\usepackage{libertinust1math}\n\\usepackage[T1]{fontenc}'
    plt.rcParams["font.family"] = "serif"

    plot_latency(key_type='dense-int')
    # plot_latency(key_type='sparse-int')
