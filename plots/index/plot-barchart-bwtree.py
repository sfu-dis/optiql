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
    'B+-tree\n(OptLock)',
    'B+-tree\n(OptiQL)',
    'ART\n(OptLock)',
    'ART\n(OptiQL)',
    'OpenBw-Tree',
]

latch_labels = labels

threads = [10, 20, 40]

legends = [f'{t} threads' for t in threads]


class PiBenchExperiment:
    NUM_REPLICATES = 10


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
        palette = sns.color_palette("Set1")

        ylabels = ['Balanced\nMillion ops/s']
        read_ratios = [0.5]
        update_ratios = [0.5]

        distributions = ['uniform', 'selfsimilar']
        skew_factors = [0.0, 0.2]
        xlabels = ['(a) Low contention', '(b) High contention']

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

                ax.yaxis.set_major_locator(MaxNLocator(5))

                df2 = df1[df1['thread'].isin(threads)]
                g = sns.barplot(data=df2, x='index', y='succeeded', hue='thread', palette=palette,
                                ax=ax, edgecolor='.2', linewidth='0.5')
                g.legend_.remove()
                ax.set_xlabel("")
                ax.set_ylabel("")
                if c == 0:
                    ax.set_ylabel(ylabels[r])
                ax.set_xlabel(xlabels[c])

                ticks_y = ticker.FuncFormatter(
                    lambda x, pos: '{0:g}'.format(x/1e6))
                ax.yaxis.set_major_formatter(ticks_y)

                ax.set_xticklabels(labels)
                for tick in ax.get_xticklabels():
                    tick.set_rotation(60)
                    tick.set_fontsize(7)

        # axs[0, 0].set_ylim([0, 125_000_000])
        # axs[0, 1].set_ylim([0, 80_000_000])
        # axs[0, 2].set_ylim([0, 50_000_000])
        # axs[1, 0].set_ylim([0, 200_000_000])
        # axs[1, 1].set_ylim([0, 80_000_000])
        # axs[1, 2].set_ylim([0, 50_000_000])
        axs[0].set_ylim([0, 100_000_000])
        axs[1].set_ylim([0, 65_000_000])

        lines, _ = axs[0].get_legend_handles_labels()
        fig.legend(lines, legends, loc='center', bbox_to_anchor=(
            0.45, 1.15), ncol=len(legends), frameon=False, handletextpad=0.5, columnspacing=0.7)

        # fig.text(0.01, 0.5, "Million ops/s", va='center', rotation='vertical')

        fig.subplots_adjust(left=0.0, right=0.98, bottom=0.05,
                            top=0.9, hspace=0.4, wspace=0.35)
        savefig(plt, 'BarChart-BwTree')

    plt.rcParams.update({'font.size': 10})
    plt.rcParams['text.usetex'] = True
    plt.rcParams['text.latex.preamble'] = '\\usepackage{libertine}\n\\usepackage{libertinust1math}\n\\usepackage[T1]{fontenc}'
    plt.rcParams["font.family"] = "serif"

    plot_bar_chart(key_type='dense-int')
