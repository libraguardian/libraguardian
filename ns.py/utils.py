from random import random
from unittest.util import _MAX_LENGTH
import numpy as np
import random

from random import sample
import networkx as nx

from ns.flow.flow import Flow
from pkg_resources import yield_lines
import matplotlib.pyplot as plt
import simpy

def generate_zipf(a, begin, end):
    flow_size = np.random.zipf(a)
    if flow_size > 10000:
        r1 = random.randint(0, begin)
    elif flow_size > 1000:
        r1 = random.randint(0, begin*4)
    else:
        r1 = random.randint(0, end - 1)
    return r1, flow_size


def headlinesPrint(str):
    print('*' * 60)
    print("\t\t", str)
    print('*' * 60)

def merge_switch_to_segment(switch_segment, hasmax=True):
    device_true_segment_result = []
    # device_true_segment_result_len = []
    if hasmax:
        for i in range(len(switch_segment[0])):
            temp = {}
            for j in range(len(switch_segment)):
                if len(switch_segment[j][i])==0:
                    continue
                temp = merge_dict_with_max(temp, switch_segment[j][i])
            device_true_segment_result.append(temp)
            # device_true_segment_result_len.append(len(temp))
    else:
        for i in range(len(switch_segment[0])):
            temp = {}
            for j in range(len(switch_segment)):
                if len(switch_segment[j][i])==0:
                    continue
                temp = merge_dict(temp, switch_segment[j][i])
            device_true_segment_result.append(temp)
            # device_true_segment_result_len.append(len(temp))
    return device_true_segment_result

def max_dict_update(table, x, freq, cur_ts, int_result):
    assert x in table
    item = table[x]
    item[0] += freq
    if item[0] > 1:
        item[2] = max(item[2], int_result)
    item[1] = cur_ts
    return table

def merge_dict(dict1, dict2):
    for k, v in dict2.items():
        if k in dict1.keys():
            dict1[k] += v
        else:
            dict1[k] = v
    return dict1

def merge_dict_with_max(dict_1, dict_2):
    for k, v in dict_2.items():
        if k in dict_1.keys():
            dict_1[k][0] += v[0]
            if dict_1[k][0] > 1:
                dict_1[k][2] = max(dict_1[k][2], dict_2[k][2])
            dict_1[k][1] = max(dict_1[k][1], v[1])
        else:
            dict_1[k] = [v[0], v[1], v[2]]
    return dict_1

def my_generate_flows(G, hosts, nflows):
    all_flows = dict()
    for flow_id in range(nflows):
        src = 0
        dst = 0
        while True:
            src, dst = sample(hosts, 2)
            if G.nodes[src]['pod'] != G.nodes[dst]['pod']:
                break
        all_flows[flow_id] = Flow(flow_id, src, dst)
        all_simple_path = list(nx.all_shortest_paths(G, src, dst))
        
        all_flows[flow_id].path = sample(
            all_simple_path,
            1)[0]
    return all_flows

class PathGenerator:
    def __init__(self, G, hosts) -> None:
        self.topo = G
        self.hosts = hosts
        self.pathDict = {}
        for src in hosts:
            for dst in hosts:
                if G.nodes[src]['pod'] != G.nodes[dst]['pod']:
                    self.pathDict[(src,dst)] = list(nx.all_shortest_paths(self.topo, src, dst))
        
    def generate_flows(self, nflows):
        all_flows = dict()
        for flow_id in range(nflows):
            src = 0
            dst = 0
            while True:
                src, dst = sample(self.hosts, 2)
                if self.topo.nodes[src]['pod'] != self.topo.nodes[dst]['pod']:
                    break
            all_flows[flow_id] = Flow(flow_id, src, dst)
            all_flows[flow_id].path = sample(
                self.pathDict[(src,dst)],
                1)[0]
        return all_flows

def printSampleFlow(ft, all_flows):
    for flow_id in sample(list(all_flows.keys()), 1):
        path = all_flows[flow_id].path
        print(path)
        for hop in path:
            switch_name = ft.nodes[hop]['device'].element_id
            res = ft.nodes[hop]['device'].query(flow_id)
            print(f'Query at switch {switch_name}: result = {res}')
            
        print(f"Flow {flow_id}")
        print("Packets Wait")
        print(all_flows[flow_id].pkt_sink.waits)
        print("Packet Arrivals")
        print(all_flows[flow_id].pkt_sink.arrivals)
        print("Arrival Perhop Times")
        print(all_flows[flow_id].pkt_sink.perhop_times)
        print(all_flows[flow_id].pkt_sink.packet_times)


def queryTopK(ft):
    global_topK = {}
    for node_id in ft.nodes():
        node = ft.nodes[node_id]
        if node['type'] == 'switch':
            local_topK = node['device'].SKETCH.flow_table
            global_topK.update(local_topK)

    _ = sorted(global_topK.items(), key = lambda x: x[1], reverse=True)
    return _

def plot_fattree(ft):
    pos = nx.multipartite_layout(ft, subset_key="layer")
    nx.draw(ft, pos, with_labels = ft.nodes)
    plt.show()