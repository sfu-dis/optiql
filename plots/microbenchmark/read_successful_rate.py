from mixed import *

def table(contention):
    df = load("../../latch-microbenchmarks", "Latch-{}".format(contention), ["omcs_offset", "omcs_offset_op_read_numa_qnode"], mixed_rw_ratios, 80)
    df1 = df[(df['replica'] == 1) & (df['rw_ratio'] != 'R0-W100')]
    df1['success_ratio'] = df1['read_successes'] / df1['reads']
    df2 = df1[['latch', 'rw_ratio', 'success_ratio']]
    return df2.pivot("latch", "rw_ratio", "success_ratio")

table("High-5").to_csv("read_successful_rate.csv")
