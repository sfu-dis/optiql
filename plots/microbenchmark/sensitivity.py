#!/bin/python3
from matplotlib.ticker import MaxNLocator
import os
import re
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
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


def load(base_dir, name, latches, rw_ratio, cs_cycles, threads):
    dfs = []
    REPLICAS = 20
    for latch in latches:
        for replica in range(1, REPLICAS+1):
            for cs in cs_cycles:
                filename = os.path.join(base_dir, "{name}-{rw_ratio}-CS{cs}-{latch}-{threads}-{replica}-stdout.txt".format(
                    name=name, rw_ratio=rw_ratio, cs=cs, latch=latch, threads=threads, replica=replica))
                with open(filename) as f:
                    df = parse_output(f.read())
                    df["latch"] = latch
                    df["replica"] = replica
                    df["cs_cycles"] = cs
                    dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


# all_cs_cycles = [5, 10, 15, 20, 25, 50, 75, 100, 125, 150, 200]
cs_cycles = [5, 15, 25, 50, 75, 100, 125, 150, 200]
printed_cs_cycles = [5, 50, 100, 150, 200]
contentions = ["Low-1M", "High-5", "High-5"]
printed_rw_ratios = ["80/20", "80/20", "20/80"]
rw_ratios = ["R80-W20", "R80-W20", "R20-W80"]
rw_latches = ['optlock_st', 'omcs_offset', 'omcs_offset_op_read_numa_qnode']


def plot(base_dir):
    # plt.rcParams.update({'font.size': 8})
    plt.rcParams['text.usetex'] = True
    plt.rcParams['text.latex.preamble'] = '\\usepackage{libertine}\n\\usepackage{libertinust1math}\n\\usepackage[T1]{fontenc}'
    plt.rcParams["font.family"] = "serif"

    fig, axs = plt.subplots(1, 2)
    fig.set_size_inches(3.3, 0.75, forward=True)
    for i in range(2):
        for j in range(1):
            thread = 80
            rw_ratio = rw_ratios[i]
            contention = contentions[i]
            ax = axs[i]
            ax.yaxis.set_major_locator(MaxNLocator(3))
            df = load(base_dir, "Latch-{}".format(contention),
                      rw_latches, rw_ratio, cs_cycles, thread)
            for (k, latch) in enumerate(rw_latches):
                plot_df = df[(df['latch'] == latch)]
                g = sns.lineplot(x='cs_cycles', y='successes',  # TODO add read successes to show successful rate somehow
                                 data=plot_df, ax=ax,
                                 ci='sd',
                                 label=printed_latches[latch], legend=False,
                                 linewidth=1,
                                 marker=markers[k], markersize=6, markeredgecolor="black", markeredgewidth=0.3)
            g.set_xticks(printed_cs_cycles)
            g.set_xticklabels(printed_cs_cycles, fontsize=10)

            # if i == 0:
            #     if j == 0:
            #         fig.text(0.33, 0.86, printed_contentions[contention], va='center', rotation='horizontal')
            #         # ax.set_ylim([0, 8_000_000])
            #     else:
            #         fig.text(0.89, 0.86, printed_contentions[contention], va='center', rotation='horizontal')
            #         ax.set_ylim([0, 24_000_000])
            # elif i == 1:
            #     if j == 0:
            #         fig.text(0.33, 0.55, printed_contentions[contention], va='center', rotation='horizontal')
            #         # ax.set_ylim([0, 14_000_000])
            #     else:
            #         fig.text(0.89, 0.55, printed_contentions[contention], va='center', rotation='horizontal')
            #         ax.set_ylim([0, 24_000_000])
            # else:
            #     if j == 0:
            #         fig.text(0.33, 0.24, printed_contentions[contention], va='center', rotation='horizontal')
            #         # ax.set_ylim([0, 18_000_000])
            #     else:
            #         fig.text(0.89, 0.24, printed_contentions[contention], va='center', rotation='horizontal')
            #         ax.set_ylim([0, 32_000_000])

            if i == 0:
                fig.text(
                    0.36, 0.76, printed_contentions[contention], va='center', rotation='horizontal')
                g.set_xlabel(
                    # "Critical section length\n(a) 80\% read, 20\% write")
                    "Critical section length")
            if i == 1:
                fig.text(
                    0.88, 0.76, printed_contentions[contention], va='center', rotation='horizontal')
                g.set_xlabel(
                    # "Critical section length\n(b) 80\% read, 20\% write")
                    "Critical section length")
            if i == 2:
                fig.text(
                    0.90, 0.76, printed_contentions[contention], va='center', rotation='horizontal')
                g.set_xlabel(
                    "Critical section length\n(c) 20\% read, 80\% write")
            if j == 1:
                g.yaxis.set_label_position("right")
                g.set_ylabel(printed_rw_ratios[i])
            else:
                g.set_ylabel("")
            # yticks = ['{}'.format(int(x)//1000_000) for x in ax.get_yticks().tolist()]
            # ax.set_yticklabels(yticks)
            ticks_y = ticker.FuncFormatter(
                lambda x, pos: '{0:g}'.format(x/1e6))
            ax.yaxis.set_major_formatter(ticks_y)
            ax.grid(axis='y', alpha=0.4)

    axs[0].set_ylim([0, 600_000_000])
    axs[1].set_ylim([0, 35_000_000])
    # axs[2].set_ylim([0, 20_000_000])

    lines, _ = axs[0].get_legend_handles_labels()
    labels = [printed_latches[latch] for latch in rw_latches]
    fig.legend(lines, labels, loc='center', bbox_to_anchor=(
        0.5, 1.1), ncol=len(rw_latches), frameon=False, handletextpad=0.5, columnspacing=0.7)

    fig.text(-0.05, 0.45, "Million ops/s", va='center', rotation='vertical')

    # fig.text(1.03, 0.45, "Read/write ratio", va='center', rotation='vertical')

    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.05,
                        top=0.9, hspace=0.4, wspace=0.35)
    plt.savefig(f'sensitivity.pdf', format='pdf',
                bbox_inches='tight', pad_inches=0)


if __name__ == '__main__':
    base_dir = "../../latch-microbenchmarks"
    plot(base_dir)
