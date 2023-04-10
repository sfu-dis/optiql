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
    'btreeolc_upgrade',
    'btreeomcs_leaf_op_read',
    'artolc_upgrade',
    'artomcs_op_read',
    'bwtree',
]
labels = [
    'B+-tree (OptLock)',
    'B+-tree (OptiQL)',
    'ART (OptLock)',
    'ART (OptiQL)',
    'OpenBw-Tree',
]

latch_labels = labels

# threads = [1, 2, 5, 10, 15, 18, 20, 22, 25, 30, 32, 35, 40, 50, 60, 70, 80]
threads = [1, 2, 5, 10, 16, 20, 30, 40, 50, 60, 70, 80]

x_labels = [1, 20, 40, 60, 80]


class PiBenchExperiment:
    NUM_REPLICATES = 3


if __name__ == '__main__':
    NUM_RECORDS = 100_000_000
    SECONDS = 10

    # start plotting
    def plot_bar_chart(key_type='dense-int'):
        nrows = 1
        ncols = 2

        dataframe = pd.read_csv(os.path.join(
            'data', 'All.csv')).iloc[:, 1:]

        markers = ['v', '^', '<', '>', 'P']
        sp = sns.color_palette()
        palette = [sp[0], sp[2], sp[5], sp[6], sp[7]]

        ylabels = ['Balanced\nMillion ops/s']
        read_ratios = [0.5]
        update_ratios = [0.5]

        distributions = ['uniform', 'selfsimilar']
        skew_factors = [0.0, 0.2]
        xlabels = ['Threads\n(a) Low contention', 'Threads\n(b) High contention']

        fig, axs = plt.subplots(nrows, ncols)
        fig.set_size_inches(3.3, 0.75, forward=True)
        fig.subplots_adjust(wspace=1)  # space between subfigs

        for r in range(nrows):
            for c in range(ncols):
                ax = axs[c]

                df = dataframe[(dataframe['exp'] == 'scalability')
                               & (dataframe['key-type'] == key_type)
                               & (dataframe['index'].isin(indexes))
                               & (dataframe['distribution'] == distributions[c])
                               & (dataframe['skew-factor'] == skew_factors[c])
                               & (dataframe['Read-ratio'] == read_ratios[r])
                               & (dataframe['Update-ratio'] == update_ratios[r])]
                df1 = df[['thread', 'index', 'replicate', 'succeeded']]
                assert(df1.shape[0] == len(
                    threads) * len(indexes) * PiBenchExperiment.NUM_REPLICATES)

                ax.yaxis.set_major_locator(MaxNLocator(5))

                for i, index in enumerate(indexes):
                    line = df1[df1['index'] == index]
                    g = sns.lineplot(data=line, x='thread', y='succeeded',
                                     marker=markers[i], markersize=6, color=palette[i],
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
        axs[0].set_ylim([0, 160_000_000])
        axs[1].set_ylim([0, 45_000_000])

        lines, _ = axs[0].get_legend_handles_labels()
        fig.legend(lines, latch_labels, loc='center', bbox_to_anchor=(
            0.4, 1.25), ncol=3, frameon=False, handletextpad=0.5, columnspacing=0.7)

        # fig.text(0.01, 0.5, "Million ops/s", va='center', rotation='vertical')

        fig.subplots_adjust(left=0.0, right=0.98, bottom=0.05,
                            top=0.9, hspace=0.4, wspace=0.35)
        savefig(plt, 'BarChart-BwTree')

    plt.rcParams.update({'font.size': 10})
    plt.rcParams['text.usetex'] = True
    plt.rcParams['text.latex.preamble'] = '\\usepackage{libertine}\n\\usepackage{libertinust1math}\n\\usepackage[T1]{fontenc}'
    plt.rcParams["font.family"] = "serif"

    plot_bar_chart(key_type='dense-int')
