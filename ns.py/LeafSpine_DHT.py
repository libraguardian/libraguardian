from functools import partial
from msvcrt import getch
from os import abort
from random import expovariate, sample
from typing import Dict

import numpy as np
import simpy

from ns.packet.dist_generator import DistPacketGenerator
from ns.packet.sink import PacketSink
from ns.switch.switch import SimplePacketSwitch
from ns.switch.switch import FairPacketSwitch
from ns.topos.fattree import build as build_fattree
from LeafSpine import buildLeafSpine
from ns.topos.utils import generate_fib
from ns.flow.flow import Flow
import networkx as nx

from utils import my_generate_flows, PathGenerator
import utils
from pprint import pprint
from ZipfPacketGenerator import ZipfPacketGenerator
from LeafSpineSwitch import LeafSpineSwitchDHT
from GlobalController import GlobalController

from LeafSpineData import LeafSpineData

import time

def headlinesPrint(str):
    print()
    print()
    print('*' * 60)
    print("\t\t", str)
    print('*' * 60)

spineN = 8
leafN = 16
hostN = 4
n_flows = 100000

pir = 10000
buffer_size = 10000000

leafSpine = buildLeafSpine(spineN, leafN, hostN)
# utils.plot_fattree(leafSpine)
all_host = list(range(spineN+leafN, spineN + leafN + leafN * hostN ))
all_flows: Dict[int, Flow] = dict()

for n in leafSpine.nodes():
    node = leafSpine.nodes[n]
    print(node)


"""
Generate flows
"""
for flow_id in range(n_flows):
    src = 0
    dst = 0
    while(True):
        src, dst = sample(all_host, 2)
        max_one = max(src, dst)
        min_one = min(src, dst)
        if leafSpine.nodes[src]['connect_leaf'] != leafSpine.nodes[dst]['connect_leaf']:
            break
    all_flows[flow_id] = Flow(flow_id, src, dst)
    all_flows[flow_id].path = sample(
            list(nx.all_shortest_paths(leafSpine, src, dst)),
        1)[0]
headlinesPrint("Generate flows successfully.")


# make flows available, adding pg, ps, setting packet_num, packet_interval, etc.
size_dist = lambda : 1

true_flow_size = dict()

env = simpy.Environment()

for fid in all_flows:
    arr_dist = partial(expovariate, 1 + np.random.rand())
    pg = ZipfPacketGenerator(env,
                             f"Flow_{fid}",
                             arr_dist,
                             size_dist,
                             flow_id=fid,
                             alpha=2.5)

    true_flow_size[fid] = pg.flow_size

    ps = PacketSink(env, rec_arrivals=False, rec_waits=False)

    all_flows[fid].pkt_gen = pg
    all_flows[fid].size = pg.flow_size
    all_flows[fid].pkt_sink = ps


print(f"Total number of packets = {sum(true_flow_size.values())}")
sortedFlow = sorted(true_flow_size.items(), key = lambda x: x[1], reverse=True)
print(sortedFlow[:20])

headlinesPrint("Packet Generator Done.")

ft = generate_fib(leafSpine, all_flows)

n_classes_per_port = 4
weights = {c: 1 for c in range(n_classes_per_port)}


def flow_to_classes(f_id, n_id=0, fib=None):
    return (f_id + n_id + fib[f_id]) % n_classes_per_port

for node_id in ft.nodes():
    node = ft.nodes[node_id]
    n_ports = max(hostN + spineN, leafN)

    flow_classes = partial(flow_to_classes,
                           n_id=node_id,
                           fib=node['flow_to_port'])
   
    node['device'] = LeafSpineSwitchDHT(env,
                                      ft,
                                      n_ports,
                                      pir,
                                      buffer_size,
                                      weights,
                                      'DRR',
                                      flow_classes,
                                      element_id = node_id,
                                      all_flows = all_flows
                                      )
    node['device'].demux.fib = node['flow_to_port']


for n in ft.nodes():
    node = ft.nodes[n]
    for port_number, next_hop in node['port_to_nexthop'].items():
        node['device'].ports[port_number].out = ft.nodes[next_hop]['device']


for target_flow_id, flow in all_flows.items():
    flow.pkt_gen.out = ft.nodes[flow.src]['device']
    ft.nodes[flow.dst]['device'].demux.ends[target_flow_id] = flow.pkt_sink


headlinesPrint("The topo looks like as follows")
for n in ft.nodes():
    pass
    # nodes[n] is a dict
    # print(f'{n}: ', end=' ')
    # pprint(ft.nodes[n])


"""
Initial Leaf Spine Data
"""
LeafSpineData.leafN = leafN
LeafSpineData.spineN = spineN
LeafSpineData.hostN = hostN
LeafSpineData.setHashRings(300)
LeafSpineData.initialWeight(leafSpine)
# LeafSpineData.adjustNodeWeight(leafSpine, 0, 3)

"""
Simulation starts
"""
headlinesPrint("Simulation starts")
tic1 = time.perf_counter()
env.run()
tic2 = time.perf_counter()
print(f"Simulation consumed {tic2 - tic1} (s)")


resDict = LeafSpineData.switches_to_flow
for key in resDict.keys():
    print(key, len(resDict[key]))