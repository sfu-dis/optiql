from mixed import *
from IPython import embed
from scipy import stats

def table(contention):
    df = load("../../latch-microbenchmarks", "Latch-{}".format(contention), ["omcs_offset", "omcs_offset_op_read_numa_qnode"], mixed_rw_ratios, 80)
    df1 = df[(df['replica'] <= 20) & (df['rw_ratio'] != 'R0-W100')]
    df1['success_ratio'] = df1['read_successes'] / df1['reads']
    df2 = df1[['latch', 'replica', 'rw_ratio', 'success_ratio']]
    df3 = pd.DataFrame(df2.groupby(["latch", "rw_ratio"]).success_ratio.apply(stats.gmean))
    df3 = df3.reset_index()
    return (df3.pivot("latch", "rw_ratio", "success_ratio") * 100).round(2)

table("High-5").to_csv("read_successful_rate.csv")
