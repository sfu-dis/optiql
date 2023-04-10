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
    'btreeomcs_leaf_offset',
    'btreeomcs_leaf_op_read',
    'btreeomcs_leaf_op_read_new_api',
]
labels = [
    'B+-tree OptLock',
    'B+-tree OptiQL-NOR',
    'B+-tree OptiQL',
    'B+-tree New API',
]
page_sizes = [256, 512, 1024, 2048, 4096, 8192, 16384]
page_size_suffices = ['', '_512', '_1K', '_2K', '_4K', '_8K', '_16K']
latch_labels = ['OptLock', 'OptiQL-NOR', 'OptiQL', 'New API']

threads = [1, 20, 40]

x_labels = ['256', '512', '1K', '2K', '4K', '8K', '16K']


class PiBenchExperiment:
    NUM_REPLICATES = 3


if __name__ == '__main__':
    NUM_RECORDS = 100_000_000
    SECONDS = 10

    # start plotting
    def plot_page_size(key_type='dense-int'):
        nrows = 3
        ncols = 5

        dataframe = pd.read_csv(os.path.join(
            'data', 'All.csv')).iloc[:, 1:]

        def find_page_size(index):
            tokens = index.split('_')
            suffices = {
                '512': 512,
                '1K': 1024,
                '2K': 2048,
                '4K': 4096,
                '8K': 8192,
                '16K': 16384,
            }
            if tokens[-1] in suffices:
                return ('_'.join(tokens[:-1]), suffices[tokens[-1]])
            return (index, 256)

        # I should learn how to use Pandas someday
        dataframe['page_size'] = dataframe.apply(
            lambda r: find_page_size(r['index'])[1], axis=1)
        dataframe['index'] = dataframe.apply(
            lambda r: find_page_size(r['index'])[0], axis=1)

        markers = ['v', '^', 'o', '*', 'd', '>', 'P', 'd', 'h']

        distributions = ['selfsimilar', 'selfsimilar', 'selfsimilar']
        skew_factors = [0.2, 0.2, 0.2]
        ylabels = ['B+-tree (1t)', 'B+-tree (20t)', 'B+-tree (40t)']
        threads = [1, 20, 40]

        titles = ['Read-only', 'Read-heavy',
                  'Balanced', 'Write-heavy', 'Update-only']
        read_ratios = [1.0, 0.8, 0.5, 0.2, 0.0]
        update_ratios = [0.0, 0.2, 0.5, 0.8, 1.0]

        fig, axs = plt.subplots(nrows, ncols)
        fig.set_size_inches(9, 3, forward=True)
        fig.subplots_adjust(wspace=0.25)  # space between subfigs

        for r in range(nrows):
            for c in range(ncols):
                ax = axs[r, c]

                df = dataframe[(dataframe['exp'] == 'page-size')
                               & (dataframe['key-type'] == key_type)
                               & (dataframe['index'].isin(indexes))
                               & (dataframe['distribution'] == distributions[r])
                               & (dataframe['skew-factor'] == skew_factors[r])
                               & (dataframe['Read-ratio'] == read_ratios[c])
                               & (dataframe['Update-ratio'] == update_ratios[c])
                               & (dataframe['thread'] == threads[r])]

                df1 = df[['index', 'page_size', 'replicate', 'succeeded']]
                assert(df1.shape[0] == len(page_sizes) *
                       len(indexes) * PiBenchExperiment.NUM_REPLICATES)

                ax.yaxis.set_major_locator(MaxNLocator(5))

                # markers = ['^', 'o', '*']
                palette = sns.color_palette()
                for i, index in enumerate(indexes):
                    line = df1[df1['index'] == index]
                    g = sns.lineplot(data=line, x='page_size', y='succeeded',
                                     marker=markers[i], markersize=6, color=palette[i],
                                     linewidth=1, markeredgecolor='black', markeredgewidth=0.3,
                                     ax=ax, label=latch_labels[i], legend=False)
                #g = sns.lineplot(data=df1, ax=ax, legend=False)
                g.set_xscale('log')
                g.set_xticks(page_sizes, x_labels)
                g.get_xaxis().set_tick_params(which='minor', size=0)
                g.get_xaxis().set_tick_params(which='minor', width=0)

                ax.grid(axis='y', alpha=0.4)
                ax.set_xlabel("")
                ax.set_ylabel("")
                if r == 0:
                    ax.set_title(titles[c], fontsize=10)
                if c == 0:
                    ax.set_ylabel(ylabels[r])
                if r == 1:
                    ax.set_xlabel("Threads")

                ticks_y = ticker.FuncFormatter(
                    lambda x, pos: '{0:g}'.format(x/1e6))
                ax.yaxis.set_major_formatter(ticks_y)

        # if key_type == 'dense-int':
        #     axs[0, 0].set_ylim([0, 125_000_000])
        #     axs[0, 1].set_ylim([0, 80_000_000])
        #     axs[0, 2].set_ylim([0, 50_000_000])
        #     axs[1, 0].set_ylim([0, 200_000_000])
        #     axs[1, 1].set_ylim([0, 80_000_000])
        #     axs[1, 2].set_ylim([0, 50_000_000])

        lines, _ = axs[0, 0].get_legend_handles_labels()
        fig.legend(lines, latch_labels, loc='center', bbox_to_anchor=(
            0.5, 1.0), ncol=len(latch_labels), frameon=False)

        fig.text(0.01, 0.5, "Million ops/s", va='center', rotation='vertical')

        fig.subplots_adjust(left=0.08, right=0.98, bottom=0.05,
                            top=0.9, hspace=0.4, wspace=0.25)
        savefig(plt, 'Page-size-skewed-all')

    plt.rcParams.update({'font.size': 10})
    plt.rcParams['text.usetex'] = True
    plt.rcParams['text.latex.preamble'] = '\\usepackage{libertine}\n\\usepackage{libertinust1math}\n\\usepackage[T1]{fontenc}'
    plt.rcParams["font.family"] = "serif"

    plot_page_size(key_type='dense-int')
