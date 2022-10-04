import random, logging
import networkx as nx

from MonitorData import MonitorData

class GlobalController:
    def __init__(self, env, do_dynamic, interval = 100, finish = float("inf"), topo = None, all_flows = None):
        self.env = env
        self.interval = interval
        self.topo = topo
        self.finish = finish
        self.all_flows = all_flows
        self.do_dynamic = do_dynamic
        self.action = env.process(self.run())
        self.flip_num = 0
        self.last_chose = []
        self.now_chose = []
        self.original = [0, 1, 2, 3, 4, 5, 6, 7, 16, 17, 18, 19]

    def run(self):
        while self.env.now < self.finish:
            yield self.env.timeout(self.interval)
            self.flip_num += 1
            print(f"Flip at time {self.env.now}")
            for node_id in self.topo.nodes():
                node = self.topo.nodes[node_id]
                if node['type'] == 'switch':
                    node['device'].flip(1)


    # aggregation or edge switch *crashNode* crashes at time *t*
    def crashAt(self, crashNode:int, t:int):
        # yield self.env.timeout(t)
        nd = self.topo.nodes[crashNode]
        if(nd['type'] == 'host'):
            return
        crashLayer = nd['layer']
        crashPod = None

        if(crashLayer != 'core'):
            crashPod = nd['pod']
            
        # print("##### Crash Info:", crashNode, crashLayer, crashPod)
        # print()
        for fid in self.all_flows:
            flow = self.all_flows[fid]
            for i in range(len(flow.path) - 1):
                if(flow.path[i + 1]) == crashNode:
                    # print("##### Affect flow: ",flow)

                    src = flow.path[0]
                    dst = flow.path[-1]
                    all_path = list(nx.all_shortest_paths(self.topo, src, dst))
                    # print(all_path)
                    # print("Src and Dst Host", src, dst)

                    newPath = flow.path
                    while True:
                        newPath = random.choice(all_path)
                        if(newPath != flow.path and crashNode not in newPath):
                            flow.path = newPath
                            break      
                    
                    # print("##### New Path:", flow)
                    # print("##### demux.fib\tport_to_nexthop")
                    ft = self.topo
                    for curHop, nxtHop in zip(newPath[1:5], newPath[2:6]):
                        # print(curHop, nxtHop)
                        curNode = ft.nodes[curHop]
                        nxtNode = ft.nodes[nxtHop]

                        for port_number, next_hop in curNode['port_to_nexthop'].items():
                            if next_hop == nxtHop:
                                curNode['device'].demux.fib[fid] = port_number


                    # print(newPath[-2], newPath[-1])
                    lastEdge = newPath[-2]
                    # print(self.topo.nodes[lastEdge]['device'].demux.fib, self.topo.nodes[lastEdge]['port_to_nexthop'])
                    
                    break


        # 3
        K = MonitorData.fat_tree_k
        if crashLayer == 'core':
            for i in range(K):
                for j in range(K):
                    MonitorData.global_dht[(i, j)].reWeights([1/((K*K/4-1)/(K*K/4)+4), 1/((K*K/4-1)/(K*K/4)+4), (K*K/4-1)/(K*K/4)/((K*K/4-1)/(K*K/4)+4), 1/((K*K/4-1)/(K*K/4)+4), 1/((K*K/4-1)/(K*K/4)+4)])
                    pass
        # 2/4
        elif crashLayer == 'aggregation':
            for i in range(K):
                if i == crashNode:
                    continue
                #2
                MonitorData.global_dht[(i, crashPod)].reWeights([1/((K/2-1)/(K/2)+4), 1/((K/2-1)/(K/2)+4), 1/((K/2-1)/(K/2)+4), (K/2-1)/(K/2)/((K/2-1)/(K/2)+4), 1/((K/2-1)/(K/2)+4)])
                MonitorData.global_dht[(crashPod, i)].reWeights([1/((K/2-1)/(K/2)+4), (K/2-1)/(K/2)/((K/2-1)/(K/2)+4), 1/((K/2-1)/(K/2)+4), 1/((K/2-1)/(K/2)+4), 1/((K/2-1)/(K/2)+4)])
        # 1/5
        elif crashLayer == 'edge':
            pass
        else:
            pass


    def quitMeasure(self, quitNode:int, t:int):
        yield self.env.timeout(t)
        nd = self.topo.nodes[quitNode]
        # print(nd)
        nd['device'].valid = False
        if(nd['type'] == 'host'):
            return
        crashLayer = nd['layer']
        crashPod = None

        if(crashLayer != 'core'):
            crashPod = nd['pod']
            
        print("##### Quit Measurement Info:", quitNode, crashLayer, crashPod)
        
        return

        K = MonitorData.fat_tree_k
        if crashLayer == 'core':
            for i in range(K):
                for j in range(K):
                    MonitorData.global_dht[(i, j)].reWeights([1, 1, 1, 0.5, 1])
                    pass
        elif crashLayer == 'aggregation':
            for i in range(K):
                if i == crashPod:
                    continue
                #2
                MonitorData.global_dht[(crashPod, i)].reWeights([1, 1, 0.5, 1, 1])
                #4
                MonitorData.global_dht[(i, crashPod)].reWeights([1, 1, 1, 1, 0.5])
        elif crashLayer == 'edge':
            for i in range(K):
                if i == crashPod:
                    continue
                # 1
                MonitorData.global_dht[(crashPod, i)].reWeights([1, 0.5, 1, 1, 1])
                # 5
                MonitorData.global_dht[(i, crashPod)].reWeights([1, 1, 1, 1, 0.0])
        else:
            pass

