from ns.switch.switch import FairPacketSwitch
# from ConsisSketch import SuMax
from collections.abc import Callable
from ns.packet.packet import Packet
from  utils import merge_dict

from LeafSpineData import LeafSpineData

# memory = 500 * 1024
# flowtable_size = 10
# hashtable_size = 10
# threshold_ratio = [0.5, 0.6, 0.7]

class LeafSpineSwitchDHT(FairPacketSwitch):
    def __init__(self,
                 env,
                 topo,
                 nports: int,
                 port_rate: float,
                 buffer_size: int,
                 weights,
                 server: str,
                 flow_classes: Callable = lambda x: x,
                 element_id: str = "",
                 debug: bool = False,
                 all_flows: dict = {}
                 ) -> None:
        super().__init__(env, 
                        nports, 
                        port_rate, 
                        buffer_size,
                        weights, 
                        server, 
                        flow_classes, 
                        element_id, 
                        debug)
        self.topo = topo
        self.global_flows = all_flows
        self.element_id = element_id
        # self.SKETCH = SuMax(memory, flowtable_size, hashtable_size, threshold_ratio)
        self.prefix = "PrefixPrefix"
 
        self.valid = 1.0

        self.ground_truth = {}
        self.ground_truth_segment = []
        self.segment_ground_truth = {}
        self.num_count = 0

        self.dht = LeafSpineData.global_dht
    
    def put(self, packet:Packet):
        # ! host, put the packet directly
        if(self.topo.nodes[self.element_id]['type'] == 'host'):
            self.demux.put(packet)
            return
        
        src = self.global_flows[packet.flow_id].path[0]
        dst = self.global_flows[packet.flow_id].path[4]
        srcLeaf = self.global_flows[packet.flow_id].path[1]
        midSpine = self.global_flows[packet.flow_id].path[2]
        dstLeaf = self.global_flows[packet.flow_id].path[3]
        selectedIndex = self.dht[(srcLeaf, midSpine, dstLeaf)].hash(packet.flow_id) + 1
        selected = self.global_flows[packet.flow_id].path[selectedIndex]
        if(selected != self.element_id):
            pass
        else:  
            LeafSpineData.flow_to_switches.setdefault(packet.flow_id, set())
            LeafSpineData.flow_to_switches[packet.flow_id].add(self.element_id)

            LeafSpineData.switches_to_flow.setdefault(self.element_id, set())
            LeafSpineData.switches_to_flow[self.element_id].add(packet.flow_id)
            toInsert = str(str(packet.flow_id) + self.prefix)[:13]

            if toInsert in self.segment_ground_truth:
                self.segment_ground_truth[toInsert] += 1
            else:
                self.segment_ground_truth[toInsert] = 1
            self.num_count += 1

            # self.SKETCH.insert(toInsert, 1)

        # don't forget to put
        self.demux.put(packet)

    # def flip(self):
    #     self.ground_truth = merge_dict(self.ground_truth, self.segment_ground_truth)
    #     self.ground_truth_segment.append(self.segment_ground_truth)
    #     self.segment_ground_truth = {}
    #     self.SKETCH.change_segment()
    #     self.SKETCH.reset_ske(memory)
