import time
import mmh3
import bisect
import random
import math

from numpy import rint, uint32

class HashRing:
    def __init__(self, nodes: list = None, redundance: int = 2, weights: list = None):
        # default redundance of each node
        self.defaultRedundance = redundance
        # dict: node-->number of this node
        self.numCopies = {}
        # dict: virtual node --> node
        self.mapping = {}
        # list of all virtual nodes
        self.positions = []
        # a random seed
        # self.global_seed = random.randint(1,10000)
        # self.global_seed = 0
        self.global_seed = time.time()
        # initialize: add nodes with redundance copies
        for n in nodes:
            self.addNode(n)
        if weights != None:
            self.reWeights(weights)
        
        
    def addNode(self, node: any, copy: int = None) -> None:
        """
        add *node* with *copy* replicas, copy is set to self.redundance by default
        """
        if copy == None:
            copy = self.defaultRedundance

        self.numCopies[node] = copy

        for i in range(copy):
            pos = self.murmurKey(node, i)
            bisect.insort(self.positions, pos)
            self.mapping[pos] = node

    def addNodeUniform(self, nodeList:list)->None:
        numNodes = len(nodeList)
        start: uint32 = 0
        delta: uint32 = 0x33333333
        for item in nodeList:
            self.numCopies[item] = 1
            bisect.insort(self.positions, start)
            self.mapping[start] = item
            start += delta


    def removeNode(self, node: any):
        """
        remove a node (remove all of its replications)
        """
        tmp = self.positions[:]
        for pos in tmp:
            if self.mapping[pos] == node:
                self.mapping.pop(pos)
                self.positions.remove(pos)
        
        self.numCopies.pop(node)

    def getNode(self, node: any) -> list:
        lst = list(set(self.numCopies.keys()))
        return lst


    def murmurKey(self, node: any, seed: int = 0):
        """
        Generally, node = input, seed = i where i in range(#replications)
        """
        return mmh3.hash(str(node), int(seed + self.global_seed), signed = False)


    def hash(self, key: any, verbose: bool = False):
        hashedPos = self.murmurKey(key)

        if verbose:
            print("The ring looks as follows:")
            for _, i in enumerate(self.positions):
                print('   ',_, i, "--->", self.mapping[i])
            print(f'hash value of key is {hashedPos}')    
        
        # for pos in self.positions:
        #     if hashedPos < pos:
        #         return self.mapping[pos]
        
        idx = bisect.bisect(self.positions, hashedPos)
        if idx == len(self.positions):
            idx = 0
        # hashedPos > all positions
        return self.mapping[self.positions[idx]]
        
    def printVerbose(self):
        print("The ring looks as follows:")
        for _, i in enumerate(self.positions):
            print('   ',_, i, "--->", self.mapping[i])
        print("Node --> number of copies")
        for _, i in enumerate(self.numCopies.keys()):
            print('   ',_, i, "--->", self.numCopies[i])


    # adjust the weights in a 5-hop path
    def reWeights(self, weights: list):
        if len(weights) < 5:
            print("Error: too few args in reWeights")
        elif len(weights) > 5:
            print("Warning: too many args in reWeights")
        else:
            weights = weights[:5]
        
        # minW = min(weights)
        # weights = [w/minW for w in weights]
        # print("Re Weights... ",weights)

        base = self.defaultRedundance

        # the node name is 0, 1, 2, 3, 4
        for i in range(5):
            expectedPos = math.ceil(weights[i] * self.defaultRedundance)
            self.adjustCopies(i, expectedPos)

    def getCopies(self, node):
        return self.numCopies.get(node, 0)
    
    def adjustCopies(self, node, copies):
        curCopies = self.numCopies.get(node, 0)

        if curCopies > copies:
            for i in range(curCopies - 1, copies - 1, -1):
                pos = self.murmurKey(node, i)
                self.positions.remove(pos)
                self.mapping.pop(pos)
        elif curCopies < copies:
            for i in range(curCopies, copies):
                pos = self.murmurKey(node, i)
                bisect.insort(self.positions, pos)
                self.mapping[pos] = node
        self.numCopies[node] = copies
    
if __name__ == "__main__":
    print("Testing Hash Ring")
    ring = HashRing([0, 1, 2, 3, 4])
    
    keyToHash = 'sth'

    # print(ring.hash(keyToHash, True))
    # ring.removeNode(ring.hash(keyToHash))

    # print(ring.hash(keyToHash, True))

    # ring.addNode("N4", 10)

    # print(ring.hash(keyToHash, True))

    # ring.addNode('N5', 4)
    
    # res = dict.fromkeys(list(ring.numCopies.keys()), 0)
    # # print(res)
    # for i in range(10000):
    #     node = ring.hash(i)
    #     res[node] += 1
    
    # print(ring.numCopies)
    # print(res)

    # print(ring.getCopies(0))
    # print(ring.getCopies(100))
    # ring.adjustCopies(0,10)
    # ring.printVerbose()
    # ring.adjustCopies(0,3)
    # ring.printVerbose()

    # ring.printVerbose()
    # ring.reWeights([1,1,1,1,1])
    # ring.printVerbose()
    # ring.reWeights([1,2,1,1,2])
    # ring.printVerbose()
    # ring.reWeights([2,2,3,0.5,1])
    # ring.printVerbose()