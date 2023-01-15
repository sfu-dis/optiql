#!/usr/bin/env python3

from cgitb import text
from math import prod
import os
import subprocess
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import MaxNLocator
import seaborn as sns
import numpy as np

pd.options.display.max_columns = None
pd.options.display.max_rows = None

NUM_SOCKETS = 2
NUM_CORES = 20

indexes = ['btreeolc_upgrade', 'btreeomcs_leaf_op_read']
labels = ['Centralized optimistic lock', 'OptiQL (this work)']
distributions = ['uniform', 'selfsimilar']
threads = [1, 2, 5, 10, 16, 20, 30, 40, 50, 60, 70, 80]
x_labels = [1, 20, 40, 60, 80]

plotFinishType = 'completed'


class PiBenchExperiment:
    NUM_REPLICATES = 5
    if os.getenv('TEST'):
        NUM_REPLICATES = 1
    opTypes = ['Insert', 'Read', 'Update', 'Remove', 'Scan']
    finishTypes = ['completed', 'succeeded']

    def __init__(self, index, wrapper_bin, **kwargs):
        self.index = index
        self.wrapper_bin = wrapper_bin
        self.pibench_args = []
        self.results = []
        kwargs['skip_verify'] = True
        kwargs['apply_hash'] = False
        if os.getenv('TEST'):
            kwargs['seconds'] = 1
        for k, w in kwargs.items():
            self.pibench_args.append('--{}={}'.format(k, w))

        def get_numactl_command(threads):
            upto = (threads + NUM_CORES - 1) // NUM_CORES
            upto = min(upto, NUM_SOCKETS)
            sockets = ','.join(map(str, range(upto)))
            return ['numactl', f'--membind={sockets}']

        self.numactl = get_numactl_command(kwargs['threads'])

    def run(self, idx, total):
        commands = [*self.numactl, PiBenchExperiment.pibench_bin,
                    self.wrapper_bin, *self.pibench_args]
        print(f'Executing ({idx}/{total}):', ' '.join(commands))
        result = subprocess.run(
            commands, capture_output=True, text=True
        )

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

        self.results.append(parse_output(result.stdout))


def run_all_experiments(name, *args, **kwargs):
    # Don't move this file
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

    estimated_sec = prod(
        [len(indexes), len(threads), PiBenchExperiment.NUM_REPLICATES, kwargs['seconds']])
    print('Estimated time:', estimated_sec // 60, 'minutes')

    for index in indexes:
        wrapper_bin = os.path.join(
            repo_dir, 'build/wrappers/lib{}_wrapper.so'.format(index))
        for t in threads:
            experiments.append(PiBenchExperiment(
                index, wrapper_bin, mode='time', pcm=False,
                threads=t, **kwargs))

    for i in range(PiBenchExperiment.NUM_REPLICATES):
        for j, exp in enumerate(experiments):
            exp.run(i * len(experiments) + j + 1, len(experiments)
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

    def std(x):
        return np.std(x, ddof=1) / np.mean(x) * 100
    df_digest = df.groupby(['index', 'thread']).agg(
        ['mean', 'min', 'max', std]).rename(columns={'std': 'std (%)'})[plotFinishType]
    print(df_digest)
    df_digest.to_csv(f'{name}-digest.csv')

    return df


def draw(fig, ax, name, df, legend=False):
    # fig, ax = plt.subplots()
    markers = ['v', 'o']
    palette = sns.color_palette()[0:3:2]
    for i, (index, label) in enumerate(zip(indexes, labels)):
        plot_df = df[df['index'] == index]
        # g = sns.lineplot(x='thread', y=plotFinishType, color=palette[i],
        #                  data=plot_df, label=label if legend else "", ax=ax,
        #                  marker=markers[i], markersize=5)
        g = sns.lineplot(data=plot_df, x='thread', y='succeeded',
                         color=palette[i], marker=markers[i], markersize=6,
                         linewidth=1, markeredgecolor='black', markeredgewidth=0.3,
                         ax=ax, label=label, legend=legend)

    g.set_xticks(x_labels)
    g.set(xlabel='Threads')
    ax.grid(axis='y', alpha=0.4)
    ax.axvspan(20, 40, facecolor='0.2', alpha=0.10)
    ax.axvspan(40, 80, facecolor='0.2', alpha=0.20)
    ticks_y = ticker.FuncFormatter(lambda x, pos: '{0:g}'.format(x/1e6))
    ax.yaxis.set_major_formatter(ticks_y)

    if legend:
        plt.legend(loc='upper right', bbox_to_anchor=(
            1.08, 1.5), ncol=2, frameon=False)


if __name__ == '__main__':
    dataframe = pd.read_csv(os.path.join('data', 'All.csv')).iloc[:, 1:]

    df = [None, None]
    for i in range(2):
        df[i] = dataframe[(dataframe['key-type'] == 'dense-int')
                        & (dataframe['index'].isin(indexes))
                        & (dataframe['distribution'] == distributions[i])
                        & (dataframe['Update-ratio'] == 1.0)]
        df[i] = df[i][['thread', 'index', 'replicate', 'succeeded']]

    # plt.rcParams.update({'font.size': 8})
    plt.rcParams['text.usetex'] = True
    plt.rcParams['text.latex.preamble'] = '\\usepackage{libertine}\n\\usepackage{libertinust1math}\n\\usepackage[T1]{fontenc}'
    plt.rcParams["font.family"] = "serif"
    fig, (ax0, ax1) = plt.subplots(1, 2)

    fig.tight_layout()
    fig.set_size_inches(3.7, 0.8, forward=True)

    fig.subplots_adjust(wspace=0.25)  # space between subfigs

    ax0.set_xlim([0, 80])
    ax0.set_ylim([0, 85000000])
    ax0.yaxis.set_major_locator(MaxNLocator(5))
    draw(fig, ax0, 'Update-only-uniform', df[0])

    ax1.set_xlim([0, 80])
    ax1.set_ylim([0, 43000000])
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    draw(fig, ax1, 'Update-only-selfsimilar', df[1], legend=True)

    ax0.set(ylabel='Million ops/s')
    ax1.set(ylabel='')

    ax0.set(xlabel='Threads\n(a) Low contention')
    ax1.set(xlabel='Threads\n(b) High contention')

    plt.savefig(f'collapse.pdf', format='pdf', bbox_inches='tight')
