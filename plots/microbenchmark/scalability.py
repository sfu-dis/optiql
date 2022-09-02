#!/bin/python3

from matplotlib.ticker import FormatStrFormatter
from matplotlib.ticker import MaxNLocator
import matplotlib.transforms as mtransforms

import os
import re
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from microbenchmark import *

def load(contention):
    base_dirs = ['../../latch-microbenchmarks']
    ratio = "R0-W100"
    dfs = []
    for base_dir in base_dirs:
        filename = os.path.join(base_dir, 'Latch-{}-{}.csv'.format(contention, ratio))
        dfs.append(pd.read_csv(filename).iloc[:, 1:])
    return pd.concat(dfs, ignore_index=True)

contentions += ["FIXED"]
def plot():
    # plt.rcParams.update({'font.size': 8})
    plt.rcParams['text.usetex'] = True
    plt.rcParams['text.latex.preamble'] = '\\usepackage{libertine}\n\\usepackage{libertinust1math}\n\\usepackage[T1]{fontenc}'
    plt.rcParams["font.family"] = "serif"
    fig, axs = plt.subplots(1, len(contentions))
    fig.set_size_inches(9, 1, forward=True)
    contention_idx = 0
    for i, contention in enumerate(contentions):
        ax = axs[i]
        ax.yaxis.set_major_locator(MaxNLocator(3))
        contention = contentions[i]
        df = load(contention)
        for idx, latch in enumerate(rw_latches + wo_latches):
            plot_df = df[df['latch'] == latch]
            g = sns.lineplot(x='thread', y='successes',
                            data=plot_df, ax=ax,
                            ci='sd',
                            label=printed_latches[latch], legend=False,
                            linewidth=1,
                            marker=markers[idx], markersize=6, markeredgecolor="black", markeredgewidth=0.3)
        ax.set_xlim([0, 80])
        if i == 0:
            fig.text(0.17, 0.78, printed_contentions[contention], va='center', rotation='horizontal')
            ax.set_ylim([0, 25_000_000])
        elif i == 1:
            fig.text(0.38, 0.78, printed_contentions[contention], va='center', rotation='horizontal')
            ax.set_ylim([0, 25_000_000])
        elif i == 2:
            fig.text(0.55, 0.18, printed_contentions[contention], va='center', rotation='horizontal')
            ax.set_ylim([0, 400_000_000])
        elif i == 3:
            fig.text(0.76, 0.18, printed_contentions[contention], va='center', rotation='horizontal')
            ax.set_ylim([0, 400_000_000])
        elif i == 4:
            fig.text(0.885, 0.18, "No Contention", va='center', rotation='horizontal')
            ax.set_ylim([0, 950_000_000])

        g.set_xlabel("Threads")
        g.set_ylabel("")
        g.set_ylim(bottom=0)
        yticks = ['{}'.format(int(x)//1000_000) for x in ax.get_yticks().tolist()]
        ax.set_yticklabels(yticks)
        ax.set_xticks([1, 20, 40, 60, 80])
        
        ax.grid(axis='y', alpha=0.4)
        ax.axvspan(20, 40, facecolor='0.2', alpha=0.10)
        ax.axvspan(40, 80, facecolor='0.2', alpha=0.20)


    lines, labels = axs[0].get_legend_handles_labels()
    fig.legend(lines, labels, loc='upper right', bbox_to_anchor=(0.95, 1.2), ncol=7, frameon=False)

    axs[0].set_ylabel("Throughput (Mops/s)")

    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.05, top=0.9, hspace=0.2, wspace=0.25)
    plt.savefig(f'scalability.pdf', format='pdf', bbox_inches='tight', pad_inches=0)

if __name__=="__main__":
    plot()
