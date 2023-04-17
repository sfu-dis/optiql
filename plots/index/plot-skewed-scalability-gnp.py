#!/usr/bin/env python3

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
from utils import savefig

pd.options.display.max_columns = None
pd.options.display.max_rows = None

NUM_SOCKETS = 2
NUM_CORES = 20

indexes = [
    'btreeomcs_leaf_offset',
    'btreeomcs_leaf_op_read',
    'btreeomcs_leaf_offset_gnp',
    'btreeomcs_leaf_op_read_gnp',
    'artomcs_offset',
    'artomcs_op_read',
    'artomcs_offset_gnp',
    'artomcs_op_read_gnp',
]
labels = [
    'B+-tree OptiQL-NOR',
    'B+-tree OptiQL',
    'B+-tree OptiQL-NOR-GNP',
    'B+-tree OptiQL-GNP',
    'ART OptiQL-NOR',
    'ART OptiQL',
    'ART OptiQL-NOR-GNP',
    'ART OptiQL-GNP',
]

btree_indexes = [index for index in indexes if 'btree' in index]
# btree_indexes += ['bwtree']
art_indexes = [index for index in indexes if 'art' in index]
btree_labels = [label for label in labels if 'B+Tree' in label]
art_labels = [label for label in labels if 'ART' in label]
# latch_labels = ['OptiQL-NOR', 'OptiQL', 'OptiQL-NOR-GNP', 'OptiQL-GNP']
# latch_labels = ['Interleaved (OptiQL-NOR)', 'Node 0 (OptiQL-NOR)',
#                 'Interleaved (OptiQL)', 'Node 0 (OptiQL)']
latch_labels = ['Interleaved', 'Node 0']
# latch_labels += ['BwTree']

# threads = [1, 2, 5, 10, 15, 18, 20, 22, 25, 30, 32, 35, 40, 50, 60, 70, 80]
# threads = [1, 2, 5, 10, 16, 20, 30, 40, 50, 60, 70, 80]
threads = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80]


x_labels = [1, 20, 40, 60, 80]


class PiBenchExperiment:
    NUM_REPLICATES = 20


if __name__ == '__main__':
    NUM_RECORDS = 100_000_000
    SECONDS = 10

    # start plotting
    def plot_scalability(key_type='dense-int'):
        nrows = 1
        ncols = 2

        dataframe = pd.read_csv(os.path.join(
            'data', 'All.csv')).iloc[:, 1:]

        markers = ['v', '^', 'o', '*', 'd', '>', 'P', 'd', 'h']

        indexes = [
            ['btreeomcs_leaf_op_read', 'btreeomcs_leaf_op_read_gnp'],
            ['artomcs_op_read', 'artomcs_op_read_gnp'],
        ]
        # labels = [btree_labels, art_labels]
        distributions = ['selfsimilar', 'selfsimilar']
        skew_factors = [0.2, 0.2]
        ylabels = ['Update-only\nMillion ops/s']

        read_ratios = [0.0]
        update_ratios = [1.0]
        xlabels = ['Threads\n(a) B+-tree', 'Threads\n(b) ART']

        fig, axs = plt.subplots(nrows, ncols)
        fig.set_size_inches(3.3, 0.75, forward=True)
        fig.subplots_adjust(wspace=1)  # space between subfigs

        for r in range(nrows):
            for c in range(ncols):
                ax = axs[c]

                df = dataframe[(dataframe['exp'] == 'scalability')
                               & (dataframe['key-type'] == key_type)
                               & (dataframe['index'].isin(indexes[c]))
                               & (dataframe['distribution'] == distributions[c])
                               & (dataframe['skew-factor'] == skew_factors[c])
                               & (dataframe['Read-ratio'] == read_ratios[r])
                               & (dataframe['Update-ratio'] == update_ratios[r])]
                df1 = df[['thread', 'index', 'replicate', 'succeeded']]
                assert(df1.shape[0] >= len(
                    threads) * len(indexes[c]) * PiBenchExperiment.NUM_REPLICATES)

                ax.yaxis.set_major_locator(MaxNLocator(5))

                for i, index in enumerate(indexes[c]):
                    line = df1[df1['index'] == index]
                    g = sns.lineplot(data=line, x='thread', y='succeeded',
                                     marker=markers[i], markersize=6,
                                     linewidth=1, markeredgecolor='black', markeredgewidth=0.3,
                                     ax=ax, label=latch_labels[i], legend=False)
                #g = sns.lineplot(data=df1, ax=ax, legend=False)
                g.set_xticks(x_labels)

                ax.grid(axis='y', alpha=0.4)
                ax.axvspan(20, 40, facecolor='0.2', alpha=0.10)
                ax.axvspan(40, 80, facecolor='0.2', alpha=0.20)
                ax.set_xlabel("")
                ax.set_ylabel("")
                ax.set_xlim([0, 80])
                # if r == 0:
                #     ax.set_title(titles[c], fontsize=10)
                if c == 0:
                    ax.set_ylabel(ylabels[r])
                # if r == 1:
                #     ax.set_xlabel("Threads")
                ax.set_xlabel(xlabels[c])

                ticks_y = ticker.FuncFormatter(
                    lambda x, pos: '{0:g}'.format(x/1e6))
                ax.yaxis.set_major_formatter(ticks_y)
                # ax.set_ylim(bottom=0)

        if key_type == 'dense-int':
            axs[0].set_ylim([0, 40_000_000])
            axs[1].set_ylim([0, 32_000_000])

        lines, _ = axs[0].get_legend_handles_labels()
        fig.legend(lines, latch_labels, loc='center', bbox_to_anchor=(
            0.45, 1.04), ncol=len(latch_labels), frameon=False)

        # fig.text(-0.01, 0.5, "Million ops/s", va='center', rotation='vertical')

        fig.subplots_adjust(left=0.0, right=0.98, bottom=0.05,
                            top=0.9, hspace=0.4, wspace=0.35)
        savefig(plt, 'Scalability-skewed-gnp')

    plt.rcParams.update({'font.size': 10})
    plt.rcParams['text.usetex'] = True
    plt.rcParams['text.latex.preamble'] = '\\usepackage{libertine}\n\\usepackage{libertinust1math}\n\\usepackage[T1]{fontenc}'
    plt.rcParams["font.family"] = "serif"

    plot_scalability(key_type='dense-int')
