#!/bin/python3
from matplotlib.ticker import MaxNLocator
import os
import re
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from microbenchmark import *

def parse_output(text):
    rows = []
    pattern = r'(.+),(.+),(.+),(.+),(.+)'
    for line in text.split('\n'):
        m = re.match(pattern, line)
        if m:
            if m.group(1) == "All":
                return pd.DataFrame([[float(m.group(2)), float(m.group(3)), float(m.group(4)), float(m.group(5))]],
                                    columns=["operations", "successes", "reads", "read_successes"])

def load(base_dir, name, latches, rw_ratios, threads):
    dfs = []
    REPLICAS = 2
    for latch in latches:

        for replica in range(1, REPLICAS+1):
            for rw_ratio in rw_ratios:
                filename = os.path.join(base_dir, "{name}-{rw_ratio}-{latch}-{threads}-{replica}-stdout.txt".format(name=name, rw_ratio=rw_ratio, latch=latch, threads=threads, replica=replica))
                with open(filename) as f:
                    df = parse_output(f.read())
                    df["latch"] = latch
                    df["replica"] = replica
                    df["rw_ratio"] = rw_ratio
                    dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

mixed_rw_ratios = rw_ratios + ["R90-W10"] # ["R90-W10", "R95-W5"]

def plot(base_dir):
    # plt.rcParams.update({'font.size': 8})
    plt.rcParams['text.usetex'] = True
    plt.rcParams['text.latex.preamble'] = '\\usepackage{libertine}\n\\usepackage{libertinust1math}\n\\usepackage[T1]{fontenc}'
    plt.rcParams["font.family"] = "serif"

    fig, axs = plt.subplots(2, 2)
    fig.set_size_inches(4, 2.1, forward=True)
    contention_idx = 0
    for i in range(2):
        for j in range(2):
            thread = 80
            contention = contentions[contention_idx]
            contention_idx += 1
            ax = axs[i, j]
            ax.yaxis.set_major_locator(MaxNLocator(3))
            df = load(base_dir, "Latch-{}".format(contention), rw_latches, mixed_rw_ratios, thread)
            for (k, latch) in enumerate(rw_latches):
                plot_df = df[(df['latch'] == latch)]
                g = sns.lineplot(x='rw_ratio', y='successes', # TODO add read successes to show successful rate somehow
                                data=plot_df, ax=ax,
                                ci='sd',
                                label=printed_latches[latch], legend=False,
                                linewidth=1,
                                marker=markers[k], markersize=6, markeredgecolor="black", markeredgewidth=0.3)
            g.set_xticklabels([printed_ratios[ratio] for ratio in mixed_rw_ratios])
            #g.set_title(printed_contentions[contention])


            if i == 0:
                if j == 0:
                    fig.text(0.33, 0.85, printed_contentions[contention], va='center', rotation='horizontal')
                    ax.set_ylim([0, 7_500_000])
                else:
                    fig.text(0.89, 0.6, printed_contentions[contention], va='center', rotation='horizontal')
                    ax.set_ylim([0, 23_000_000])
            else:
                if j == 0:
                    fig.text(0.33, 0.1, printed_contentions[contention], va='center', rotation='horizontal')
                    ax.set_ylim([0, 675_000_000])
                else:
                    fig.text(0.89, 0.1, printed_contentions[contention], va='center', rotation='horizontal')
                    ax.set_ylim([0, 475_000_000])

            if i == 1:
                g.set_xlabel("Read/write ratio")
            else:
                g.set_xlabel("")
            g.set_ylabel("")
            yticks = ['{}'.format(int(x)//1000_000) for x in ax.get_yticks().tolist()]
            ax.set_yticklabels(yticks)
            ax.grid(axis='y', alpha=0.4)


    lines, _ = axs[0, 0].get_legend_handles_labels()
    labels = [printed_latches[latch] for latch in rw_latches]
    fig.legend(lines, labels, handletextpad=0.5, columnspacing=0.7, loc='upper right', bbox_to_anchor=(1.015, 1.06), ncol=len(rw_latches), frameon=False)
    
    fig.text(-0.04, 0.45, "Throughput (million ops/s)", va='center', rotation='vertical')

    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.05, top=0.9, hspace=0.4, wspace=0.25)
    plt.savefig(f'mixed.pdf', format='pdf', bbox_inches='tight', pad_inches=0)


if __name__ == '__main__':
    base_dir = "../../latch-microbenchmarks"
    plot(base_dir)
