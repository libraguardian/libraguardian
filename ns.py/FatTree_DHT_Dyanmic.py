from functools import partial
from random import expovariate, random, sample
import random

import numpy as np
import simpy

from ns.packet.dist_generator import DistPacketGenerator
from ns.packet.sink import PacketSink
from ns.switch.switch import SimplePacketSwitch
from ns.switch.switch import FairPacketSwitch
from ns.topos.fattree import build as build_fattree
from ns.topos.utils import generate_fib

import networkx as nx

from utils import my_generate_flows, PathGenerator, headlinesPrint
import utils
from pprint import pprint
from ZipfPacketGenerator import ZipfPacketGenerator
from MonitorSwitch import MonitorSwitchDHT
from GlobalController import GlobalController

from MonitorData import MonitorData

import time
import sys


def queryPathSketch(ft, flow_id: int)->None:
    headlinesPrint(f"Query Path of Flow {flow_id}")
    print("Querying all sketches in the path of this flow")
    path = all_flows[flow_id].path
    pprint(path)
    for hop in path:
        if(ft.nodes[hop]['type'] != 'switch'):
            continue
        switch_name = ft.nodes[hop]['device'].element_id
        res = ft.nodes[hop]['device'].query(flow_id)
        # ft.nodes[hop]['device'].print()
        print(f'Query at switch {switch_name}: result = {res}')


env = simpy.Environment()


random.seed(12345)
np.random.seed(12345)

# n_flows = 100_000
n_flows = hash_ring_num = k = -1
fname = ""
adjR = 0

if len(sys.argv) >= 5:
    n_flows = int(sys.argv[1])
    hash_ring_num = int(sys.argv[2])
    k = int(sys.argv[3])
    fname = str(sys.argv[4])
    adjR = int(sys.argv[5])
else:
    n_flows = 1000000
    hash_ring_num = 500
    k = 8
    fname = "res/new_default.txt"

print(n_flows, hash_ring_num, k, fname)

pir = 10000    
buffer_size = 10000000
mean_pkt_size = 100.0   

ft: nx.Graph = build_fattree(k)   

print("Fat Tree({}) with {} nodes.".format(k, ft.number_of_nodes()))

hosts = set()
for n in ft.nodes():
    if ft.nodes[n]['type'] == 'host':
        hosts.add(n)

print("All Hosts {}".format(hosts))

tic1 = time.perf_counter()
all_flows = PathGenerator(ft, hosts).generate_flows(n_flows)
headlinesPrint("Generate Flows Succeed")
tic2 = time.perf_counter()
print(tic2 - tic1)

size_dist = lambda : 1

true_flow_size = dict()

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

max_flow_id = sortedFlow[0][0]
max_flow_size = sortedFlow[0][1]

print(f'The biggest flow is {(max_flow_id, max_flow_size)}')

ft = generate_fib(ft, all_flows)

n_classes_per_port = 4
weights = {c: 1 for c in range(n_classes_per_port)}


def flow_to_classes(f_id, n_id=0, fib=None):
    return (f_id + n_id + fib[f_id]) % n_classes_per_port

for node_id in ft.nodes():
    node = ft.nodes[node_id]


    flow_classes = partial(flow_to_classes,
                           n_id=node_id,
                           fib=node['flow_to_port'])
    """
    # env:
    # k: parameter K
    # pir: port_rate
    # buffer_size: buffer_size
    # weights: 1, 1, 1, 1
    # 'DRR'
    # flow_classes: a function mapping flow id to class id
    # element_id = node_id
    # node['device'] = FairPacketSwitch(env,
    #                                   k,
    #                                   pir,
    #                                   buffer_size,
    #                                   weights,
    #                                   'DRR',
    #                                   flow_classes,
    #                                   element_id=f"Switch_{node_id}")
    """
    
    node['device'] = MonitorSwitchDHT(env,
                                      ft,
                                      k,
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


controller = GlobalController(env, 100, finish=3000, topo=ft, all_flows = all_flows)
MonitorData.setHashRings(k, hash_ring_num)
MonitorData.initialWeight(ft)

# MonitorData.adjustNodeWeight(ft, 0, 0.5)
# MonitorData.adjustNodeWeight(ft, 1, 1)
# MonitorData.adjustNodeWeight(ft, 2, 1.5)
# MonitorData.adjustNodeWeight(ft, 3, 2)

headlinesPrint("Simulation Started")
event = env.process(controller.crashAt(crashNode = 16, t = 0))
headlinesPrint("Global weights of switches before starting")
print(MonitorData.global_weights)
tic1 = time.perf_counter()
env.run()
tic2 = time.perf_counter()
headlinesPrint(f'Simulation Finished at {env.now}')
print(f"Simulation consumed time {tic2 - tic1} (s).")


target_flow_id = max_flow_id

headlinesPrint("MonitorData")

layerFlows = {}

for node_id in ft.nodes():
    curNode = ft.nodes[node_id]
    if curNode['type'] == 'host':
        continue
    else:
        switch_layer = curNode['layer']
        layerFlows.setdefault(switch_layer, 0)
        print(node_id, len(MonitorData.switches_to_flow[node_id]))
        layerFlows[switch_layer] += len(MonitorData.switches_to_flow[node_id])

print(layerFlows)
with open(fname, 'a') as file1:
    file1.write(f"{layerFlows['core']}\t{layerFlows['aggregation']}\t{layerFlows['edge']}\n")

with open(fname, 'a') as file2:
    length = []
    for k,v in MonitorData.switches_to_flow.items():
        length.append(str(len(v)))
    file2.write(f"{' '.join(length)}\n")