from os import abort
from typing import Tuple
from ring import HashRing
from typing import Dict
import networkx as nx
from utils import headlinesPrint


class MonitorData:
    # flow id --> set of switches
    flow_to_switches = {}
    # switches --> set of flow ids
    switches_to_flow = {}
    for i in range(20):
        switches_to_flow[i] = set()
    numCopies = -1
    global_dht: Dict[Tuple, HashRing] = {}
    global_weights = {}
    fat_tree_k = 0

    def setHashRings(fat_tree_k: int, num: int):
        MonitorData.fat_tree_k = fat_tree_k
        MonitorData.numCopies = num
        for i in range(fat_tree_k):
            for j in range(fat_tree_k):
                MonitorData.global_dht[(i, j)] = HashRing([0, 1, 2, 3, 4], int(MonitorData.numCopies / 5),
                                                          [0.2, 0.2, 0.2, 0.2, 0.2])

    def initialWeight(ft: nx.Graph):
        for n in ft.nodes():
            node = ft.nodes[n]
            if node['type'] != 'host':
                MonitorData.global_weights[n] = 1

    def adjustNodeWeight(ft: nx.Graph, nodeIdx: int, w: float):
        MonitorData.global_weights[nodeIdx] = w
        MonitorData.reCalculateDHT(ft)

    def reCalculateDHT(ft: nx.Graph):
        currentW = MonitorData.global_weights
        K = MonitorData.fat_tree_k
        core_weight = 0
        max_core_weight = 0
        pod_weight = {}
        for i in range(K):
            pod_weight[i] = {}
            pod_weight[i]['leaf'] = 0
            pod_weight[i]['aggregation'] = 0
            pod_weight[i]['max_leaf'] = 0
            pod_weight[i]['max_aggregation'] = 0
        for i in range(int(K * K // 4)):
            core_weight += currentW[i]
            max_core_weight = max(max_core_weight, currentW[i])
        for i in range(int(K * K // 4)):
            ft.nodes[i]['device'].valid = currentW[i] / max_core_weight

        for pod in range(K):
            for i in range(K * K // 4 + pod * K, K * K // 4 + pod * K + K // 2):
                pod_weight[pod]['aggregation'] += currentW[i]
                pod_weight[pod]['max_aggregation'] = max(currentW[i], pod_weight[pod]['max_aggregation'])
            for i in range(K * K // 4 + pod * K, K * K // 4 + pod * K + K // 2):
                ft.nodes[i]['device'].valid = currentW[i] / pod_weight[pod]['max_aggregation']

            for i in range(K * K // 4 + pod * K + K // 2, K * K // 4 + pod * K + K):
                pod_weight[pod]['leaf'] += currentW[i]
                pod_weight[pod]['max_leaf'] = max(currentW[i], pod_weight[pod]['max_leaf'])
            for i in range(K * K // 4 + pod * K + K // 2, K * K // 4 + pod * K + K):
                ft.nodes[i]['device'].valid = currentW[i] / pod_weight[pod]['max_leaf']

        for i in range(K):
            for j in range(K):
                if i == j:
                    continue
                # print(i, j)
                extraInAggre = pod_weight[i]['max_aggregation'] * K / 2 - pod_weight[i]['aggregation']
                extraInAggre2 = pod_weight[j]['max_aggregation'] * K / 2 - pod_weight[j]['aggregation']
                extrCore = max_core_weight * K * K / 4 - core_weight

                nextWei = [pod_weight[i]['max_leaf'], pod_weight[i]['max_aggregation'], max_core_weight,
                           pod_weight[j]['max_aggregation'], max(0, pod_weight[j]['max_leaf'] - (
                                extraInAggre + extraInAggre2) / (K / 2) - extrCore / (K * K / 4))]
                MonitorData.global_dht[(i, j)].reWeights(
                    [i / sum(nextWei) for i in nextWei]
                )