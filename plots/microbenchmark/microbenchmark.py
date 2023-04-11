rw_ratios = ["R0-W100", "R20-W80", "R50-W50", "R80-W20"]
printed_ratios = {
    "R0-W100": "0/100",
    "R20-W80": "20/80",
    "R50-W50": "50/50",
    "R80-W20": "80/20",
    "R90-W10": "90/10",
    "R95-W5": "95/5",
    }

rw_latches = ['optlock_st', 'omcs_offset', 'omcs_offset_op_read_numa_qnode',
              'mcsrw_offset']
wo_latches = ['tatas_st', 'mcs']
printed_latches = {
    'optlock_st': 'OptLock',
    'omcs_offset': 'OptiQL-NOR',
    'omcs_offset_op_read_numa_qnode': 'OptiQL',
    'mcsrw_offset': 'MCS-RW',
    'tatas_st': 'TTS',
    'mcs': 'MCS',
    }

contentions = ["1-Max", "High-5", "Medium-30000", "Low-1M"]
printed_contentions = {
    "1-Max": "Extreme",
    "High-5": "High",
    "Medium-30000": "Medium",
    "Low-1M": "Low",
}

markers = ['v', '^', 'o', 'P', 'd', '>']
