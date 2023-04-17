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

def load(base_dir, name, latches, rw_ratio, cs_cycles, threads):
    dfs = []
    REPLICAS = 20
    for latch in latches:
        for replica in range(1, REPLICAS+1):
            for cs in cs_cycles:
                filename = os.path.join(base_dir, "{name}-{rw_ratio}-CS{cs}-{latch}-{threads}-{replica}-stdout.txt".format(name=name, rw_ratio=rw_ratio, cs=cs, latch=latch, threads=threads, replica=replica))
                with open(filename) as f:
                    df = parse_output(f.read())
                    df["latch"] = latch
                    df["replica"] = replica
                    df["cs_cycles"] = cs
                    dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

# all_cs_cycles = [5, 10, 15, 20, 25, 50, 75, 100, 125, 150, 200]
cs_cycles = [5, 15, 25, 50, 75, 100, 125, 150, 200]
printed_cs_cycles = [5, 25, 50, 100, 150, 200]
contentions = ["1-Max", "High-5"]
printed_rw_ratios = ["50/50", "80/20", "90/10"]
rw_ratios = ["R50-W50", "R80-W20", "R90-W10"]
rw_latches = ['optlock_st', 'omcs_offset', 'omcs_offset_op_read_numa_qnode']
def plot(base_dir):
    # plt.rcParams.update({'font.size': 8})
    plt.rcParams['text.usetex'] = True
    plt.rcParams['text.latex.preamble'] = '\\usepackage{libertine}\n\\usepackage{libertinust1math}\n\\usepackage[T1]{fontenc}'
    plt.rcParams["font.family"] = "serif"

    fig, axs = plt.subplots(3, 2)
    fig.set_size_inches(4, 3.2, forward=True)
    for i in range(3):
        for j in range(2):
            thread = 80
            rw_ratio = rw_ratios[i]
            contention = contentions[j]
            ax = axs[i, j]
            ax.yaxis.set_major_locator(MaxNLocator(3))
            df = load(base_dir, "Latch-{}".format(contention), rw_latches, rw_ratio, cs_cycles, thread)
            for (k, latch) in enumerate(rw_latches):
                plot_df = df[(df['latch'] == latch)]
                g = sns.lineplot(x='cs_cycles', y='successes', # TODO add read successes to show successful rate somehow
                                data=plot_df, ax=ax,
                                ci='sd',
                                label=printed_latches[latch], legend=False,
                                linewidth=1,
                                marker=markers[k], markersize=6, markeredgecolor="black", markeredgewidth=0.3)
            g.set_xticks(printed_cs_cycles)
            g.set_xticklabels(printed_cs_cycles)


            if i == 0:
                if j == 0:
                    fig.text(0.33, 0.86, printed_contentions[contention], va='center', rotation='horizontal')
                    ax.set_ylim([0, 8_000_000])
                else:
                    fig.text(0.89, 0.86, printed_contentions[contention], va='center', rotation='horizontal')
                    ax.set_ylim([0, 22_000_000])
            elif i == 1:
                if j == 0:
                    fig.text(0.33, 0.55, printed_contentions[contention], va='center', rotation='horizontal')
                    ax.set_ylim([0, 14_000_000])
                else:
                    fig.text(0.89, 0.55, printed_contentions[contention], va='center', rotation='horizontal')
                    ax.set_ylim([0, 30_000_000])
            else:
                if j == 0:
                    fig.text(0.33, 0.24, printed_contentions[contention], va='center', rotation='horizontal')
                    ax.set_ylim([0, 18_000_000])
                else:
                    fig.text(0.89, 0.24, printed_contentions[contention], va='center', rotation='horizontal')
                    ax.set_ylim([0, 38_000_000])


            if i == 2:
                g.set_xlabel("Critical section length")
            else:
                g.set_xlabel("")
            if j == 1:
                g.yaxis.set_label_position("right")
                g.set_ylabel(printed_rw_ratios[i])
            else:
                g.set_ylabel("")
            yticks = ['{}'.format(int(x)//1000_000) for x in ax.get_yticks().tolist()]
            ax.set_yticklabels(yticks)
            ax.grid(axis='y', alpha=0.4)


    lines, _ = axs[0, 0].get_legend_handles_labels()
    labels = [printed_latches[latch] for latch in rw_latches]
    fig.legend(lines, labels, handletextpad=0.5, columnspacing=0.7, loc='upper right', bbox_to_anchor=(0.9, 1), ncol=len(rw_latches), frameon=False)
    
    fig.text(-0.01, 0.45, "Throughput (million ops/s)", va='center', rotation='vertical')
    
    fig.text(1.03, 0.45, "Read/write ratio", va='center', rotation='vertical')

    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.05, top=0.9, hspace=0.4, wspace=0.17)
    plt.savefig(f'sensitivity.pdf', format='pdf', bbox_inches='tight', pad_inches=0)


if __name__ == '__main__':
    base_dir = "../../latch-microbenchmarks"
    plot(base_dir)
