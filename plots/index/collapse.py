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

indexes = ['btreeolc_upgrade', 'btreeomcs_leaf_op_read']
labels = ['Centralized optimistic lock', 'OptiQL (this work)']
distributions = ['uniform', 'selfsimilar']
threads = [1, 2, 5, 10, 16, 20, 30, 40, 50, 60, 70, 80]
x_labels = [1, 20, 40, 60, 80]

plotFinishType = 'completed'


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
        df[i] = dataframe[(dataframe['exp'] == 'scalability')
                        & (dataframe['key-type'] == 'dense-int')
                        & (dataframe['index'].isin(indexes))
                        & (dataframe['distribution'] == distributions[i])
                        & (dataframe['Update-ratio'] == 1.0)]
        df[i] = df[i][['thread', 'index', 'replicate', 'succeeded']]

    plt.rcParams.update({'font.size': 10})
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

    savefig(plt, 'collapse')
