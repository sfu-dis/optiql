#!/usr/bin/env python3

import glob

# FIXME(shiges): Linux-specific way of querying topology
# We assume all nodes look like the same

def get_num_sockets():
    return len(glob.glob('/sys/devices/system/node/node*'))

def get_num_cores():
    cores = set()
    for file in glob.glob('/sys/devices/system/node/node0/cpu[0-9]*/topology/thread_siblings_list'):
        with open(file) as f:
            text = f.readline().strip()
            physical_core = text.split(',')[0]
            cores.add(physical_core)
    return len(cores)

NUM_SOCKETS = get_num_sockets()
NUM_CORES = get_num_cores()
