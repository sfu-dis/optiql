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

def parse_output(text):
    rows = []
    thread_id = 1
    pattern = r'(.+),(.+),(.+),(.+),(.+)'
    for line in text.split('\n'):
        m = re.match(pattern, line)
        if m:
            if m.group(1) != "All":
                rows.append([float(m.group(2)), float(m.group(3)), float(m.group(4)), float(m.group(5))])
                thread_id += 1

    return pd.DataFrame(rows, columns=["operations", "successes", "reads", "read_successes"])

def load(base_dir, name, rw_ratio, latches, threads, replica):
    dfs = []
    for latch in latches:
        filename = os.path.join(base_dir, "{name}-{rw_ratio}-{latch}-{threads}-{replica}-stdout.txt".format(name=name, rw_ratio=rw_ratio, latch=latch, threads=threads, replica=replica))
        with open(filename) as f:
            df = parse_output(f.read())
            df["latch"] = latch
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def plot(base_dir):    
    # plt.rcParams.update({'font.size': 8})
    plt.rcParams['text.usetex'] = True
    plt.rcParams['text.latex.preamble'] = '\\usepackage{libertine}\n\\usepackage{libertinust1math}\n\\usepackage[T1]{fontenc}'
    plt.rcParams["font.family"] = "serif"
    threads = 40
    fig, axs = plt.subplots(2, 2)
    fig.set_size_inches(3.8, 2, forward=True)
    contention_idx = 0
    for i in range(2):
        for j in range(2):
            ax = axs[i, j]
            ax.yaxis.set_major_locator(MaxNLocator(3))
            contention = contentions[contention_idx]
            contention_idx += 1
            rw_ratio = "R50-W50"
            df = load(base_dir, "Latch-{}".format(contention), rw_ratio, rw_latches, threads, replica=1)
            g = sns.boxplot(x="latch", y="successes", data=df[1:], ax=ax, notch=True, linewidth=0.6)
            g.set_xticklabels([])
            g.set_xlabel("")
            if i == 0:
                if j == 0:
                    ax.set_ylim([0, 450000])
                    g.set_ylabel("Kops/s")
                    fig.text(0.33, 0.83, printed_contentions[contention], va='center', rotation='horizontal')
                else:
                    ax.set_ylim([0, 750000])
                    g.set_ylabel("")
                    fig.text(0.89, 0.83, printed_contentions[contention], va='center', rotation='horizontal')

                yticks = ['{}'.format(int(x)//1_000) for x in ax.get_yticks().tolist()]
                ax.set_yticklabels(yticks)
            else:
                if j == 0:
                    ax.set_ylim([0, 9000000])
                    g.set_ylabel("Mops/s")
                    fig.text(0.33, 0.1, printed_contentions[contention], va='center', rotation='horizontal')
                else:
                    ax.set_ylim([0, 7500000])
                    g.set_ylabel("")
                    fig.text(0.89, 0.1, printed_contentions[contention], va='center', rotation='horizontal')

                yticks = ['{}'.format(int(x)//1_000000) for x in ax.get_yticks().tolist()]
                ax.set_yticklabels(yticks)
    
            ax.grid(axis='y', alpha=0.4)
            g.set_ylim(bottom=0)

    for ax in axs[1,:]:
        ax.set_xticklabels([printed_latches[latch] for latch in rw_latches], rotation=25)

    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.05, top=0.9, hspace=0.2, wspace=0.3)
    plt.savefig(f'fairness.pdf', format='pdf', bbox_inches='tight', pad_inches=0)

if __name__=="__main__":
    base_dir = "../../latch-microbenchmarks"
    plot(base_dir)
