from lib2to3.pytree import Leaf
from os import abort
from typing import Tuple
from MonitorData import MonitorData
from ring import HashRing
from typing import Dict
import networkx as nx
from utils import headlinesPrint

class LeafSpineData:

    # flow id --> set of switches
    flow_to_switches = {}
    # switches --> set of flow ids
    switches_to_flow = {}
    for i in range(24):
        switches_to_flow[i] = set()
    numCopies = -1
    global_dht: Dict[Tuple, HashRing] = {}
    global_weights = {}
    leafN = 0
    spineN = 0
    hostN = 0

    def setHashRings(num: int):
        LeafSpineData.numCopies = num
        for i in range(LeafSpineData.spineN , LeafSpineData.spineN + LeafSpineData.leafN):
            for j in range(LeafSpineData.spineN , LeafSpineData.spineN + LeafSpineData.leafN):
                for k in range(LeafSpineData.spineN):
                    LeafSpineData.global_dht[(i,k,j)] = HashRing([0,1,2], int(LeafSpineData.numCopies), [0.33, 0.33, 0.33])    

    def initialWeight(ft: nx.Graph):
        for n in ft.nodes():
            node = ft.nodes[n]
            if node['type'] != 'host':
                LeafSpineData.global_weights[n] = 1
        headlinesPrint("Global weights of switches")
        print(LeafSpineData.global_weights)

    def adjustNodeWeight(ft: nx.Graph, nodeIdx: int, w: float):
        LeafSpineData.global_weights[nodeIdx] = w
        LeafSpineData.reCalculateDHT(ft)

    def reCalculateDHT(ft: nx.Graph):
        currentW = LeafSpineData.global_weights
        spineN = LeafSpineData.spineN
        leafN = LeafSpineData.leafN
        for i in range(spineN , spineN + leafN):
            for j in range(spineN , spineN + leafN):
                for k in range(spineN):
                    nextWei = [LeafSpineData.global_weights[i], LeafSpineData.global_weights[k], LeafSpineData.global_weights[j]]
                    LeafSpineData.global_dht[(i, k, j)].reWeights([i/sum(nextWei) for i in nextWei])