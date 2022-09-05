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

pd.options.display.max_columns = None
pd.options.display.max_rows = None

NUM_SOCKETS = 2
NUM_CORES = 20

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


class PiBenchExperiment:
    NUM_REPLICATES = 3


if __name__ == '__main__':
    NUM_RECORDS = 100_000_000
    SECONDS = 10

    dataframe = pd.read_csv(os.path.join('data', 'All.csv')).iloc[:, 1:]

    # start plotting
    def plot_scalability(key_type='sparse-int'):
        nrows = 1
        ncols = 2

        markers = ['v', '^', 's', 'o', '*', 'd', '>', 'P', 'd', 'h']

        ylabels = ['Million ops/s']

        distributions = ['selfsimilar', 'selfsimilar']
        skew_factors = [0.2, 0.2]
        titles = ['ART', 'ART']
        indexes = [art_indexes, art_indexes]
        labels = [btree_labels, art_labels]
        xlabels = ['Threads\n(a) Read-heavy', 'Threads\n(b) Write-heavy']
        read_ratios = [0.8, 0.2]
        update_ratios = [0.2, 0.8]

        fig, axs = plt.subplots(nrows, ncols)
        fig.set_size_inches(3.3, 0.75, forward=True)
        fig.subplots_adjust(wspace=1)  # space between subfigs

        for r in range(nrows):
            for c in range(ncols):
                ax = axs[c]

                df = dataframe[(dataframe['key-type'] == key_type)
                               & (dataframe['index'].isin(indexes[c]))
                               & (dataframe['distribution'] == distributions[c])
                               & (dataframe['skew-factor'] == skew_factors[c])
                               & (dataframe['Read-ratio'] == read_ratios[c])
                               & (dataframe['Update-ratio'] == update_ratios[c])]
                df1 = df[['thread', 'index', 'replicate', 'succeeded']]
                assert(df1.shape[0] == len(
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
                if c == 0:
                    ax.set_ylabel(ylabels[r])
                ax.set_xlabel(xlabels[c])

                ticks_y = ticker.FuncFormatter(
                    lambda x, pos: '{0:g}'.format(x/1e6))
                ax.yaxis.set_major_formatter(ticks_y)

        # axs[0, 0].set_ylim([0, 125_000_000])
        # axs[0, 1].set_ylim([0, 80_000_000])
        # axs[0, 2].set_ylim([0, 50_000_000])
        # axs[1, 0].set_ylim([0, 200_000_000])
        # axs[1, 1].set_ylim([0, 80_000_000])
        # axs[1, 2].set_ylim([0, 50_000_000])

        # axs[0].set_ylim([0, 80_000_000])
        # axs[1].set_ylim([0, 160_000_000])

        lines, _ = axs[0].get_legend_handles_labels()
        fig.legend(lines, latch_labels, loc='upper right', bbox_to_anchor=(
            1.03, 1.35), ncol=len(latch_labels), frameon=False, handletextpad=0.5, columnspacing=0.7)

        # fig.text(0.01, 0.5, "Million ops/s", va='center', rotation='vertical')

        fig.subplots_adjust(left=0.0, right=0.98, bottom=0.05,
                            top=0.9, hspace=0.4, wspace=0.35)
        plt.savefig(f'Scalability-skewed-sparse.pdf',
                    format='pdf', bbox_inches='tight', pad_inches=0)

    plt.rcParams.update({'font.size': 10})
    plt.rcParams['text.usetex'] = True
    plt.rcParams['text.latex.preamble'] = '\\usepackage{libertine}\n\\usepackage{libertinust1math}\n\\usepackage[T1]{fontenc}'
    plt.rcParams["font.family"] = "serif"
    plot_scalability(key_type='sparse-int')
