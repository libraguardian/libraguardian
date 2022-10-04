import copy
import logging
import argparse
import pickle
from functools import partial
from random import expovariate
import random
import numpy as np
import simpy
from ns.packet.sink import PacketSink
from ns.topos.fattree import build as build_fattree
from ns.topos.utils import generate_fib
import networkx as nx
from utils import PathGenerator, headlinesPrint, merge_dict_with_max, merge_switch_to_segment
from pprint import pprint
from ZipfPacketGenerator import ZipfPacketGenerator
from MonitorSwitch import MonitorSwitchDHT
from GlobalController import GlobalController
from MonitorData import MonitorData
import time

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

parser = argparse.ArgumentParser()
parser.add_argument("--dht", action="store_true", dest="is_dht")       #true:dht ring     false: normal hash
parser.add_argument("--n_flows", type=int, default=100000)      #flow num

parser.add_argument("--memory", type=float, nargs='+')      #memory of switch

parser.add_argument("--k", type=int, default=8)      #Fattree k
parser.add_argument("--switch_num", type=int)      #the switch number of Fattree

parser.add_argument("--flow_alpha", type=float, default=2.5)
parser.add_argument("--arr_dist_alpha", type=float, default=1.4)
parser.add_argument("--limit_memory", action="store_true", dest="do_limit_memory")
parser.add_argument("--memory_limit", type=list, default=[])      #number of limited switch
parser.add_argument("--flowtable_size_limit", type=list, default=[])
parser.add_argument("--hashtable_size_limit", type=list, default=[])
parser.add_argument("--memory_limit_ratio", type=float, default=0.1)      #memory of limited switch

parser.add_argument("--random_seed", type=int, default=45721)
parser.add_argument("--inter", type=int, default=10000)      #the interval of flip
parser.add_argument("--finish", type=int, default=100000)
parser.add_argument("--begin", type=int, default=10000)
parser.add_argument("--end", type=int, default=100000)
parser.add_argument("--mean_pkt_size", type=float, default=100.0)

parser.add_argument("--flowtable_size_ratio", type=float, default=0.1)      #ratio of flowtable to memory
parser.add_argument("--flowtable_size", type=list, default=[])      #flowtable_zise
parser.add_argument("--hashtable_size_ratio", type=float, default=2.0)      #the ratio of hashtable_zise and flowtable_size
parser.add_argument("--hashtable_size", type=list, default=[])      #flowtable_zise

parser.add_argument("--threshold_ratio", type=list, default=[0.5, 0.7, 0.9])
parser.add_argument("--d", type=int, default=3)      #sketch
parser.add_argument("--cols", type=list, default=[])      #sketch

parser.add_argument("--heavy_hitter_thres", type=int, default=1000)      #heavy_hitter_thres
parser.add_argument("--heavy_change_ratio", type=int, default=0.0001)      #heavy_change_ratio
parser.add_argument("--heavy_change_thres", type=int)      #heavy_change_thres = ratio * flow number

parser.add_argument("--pir", type=int, default=10000)      #pir
parser.add_argument("--buffer_size", type=int, default=10000000)      #buffer_size
parser.add_argument("--log_file", type=str, default="test.log")

args = parser.parse_args()
np.set_printoptions(linewidth=400)


if __name__ == "__main__":

    random.seed(args.random_seed)
    np.random.seed(args.random_seed)

    logging.basicConfig(format='%(message)s', level=logging.DEBUG, filename=args.log_file, filemode='a')
    handler = logging.StreamHandler()
    handler.terminator = ""

    logging.info(f"\n\nn_flows: {args.n_flows}, memory:{args.memory}, is_dht:{args.is_dht}, heavy_hitter_thres:{args.heavy_hitter_thres}, heavy_change_ratio:{args.heavy_change_ratio}")

    for i in range(len(args.memory)):
        args.memory[i] *= 1024
    if args.do_limit_memory:
        for i in range(len(args.memory)):
            args.memory_limit.append(int(args.memory[i] * float(args.memory_limit_ratio)))
    args.switch_num = int(5 * args.k **2 / 4)

    for i in range(len(args.memory)):
        args.cols.append(int(args.memory[i] * (1.- args.flowtable_size_ratio) / args.d / 4 / 2))       #cols = int(memory / 3 / 4 / 2)  #memory/3 hash/counter 4Bytes/sum and max
        args.flowtable_size.append(int(args.memory[i] * args.flowtable_size_ratio / 12))   #memory * 10% / 12 (flowtable{key:(sum, ts, max)})
        args.hashtable_size.append(int(args.flowtable_size[i] * args.hashtable_size_ratio))
        args.flowtable_size_limit.append(int(args.flowtable_size[i] * args.memory_limit_ratio))
        args.hashtable_size_limit.append(int(args.hashtable_size[i] * args.memory_limit_ratio))

    env = simpy.Environment()
    ft: nx.Graph = build_fattree(args.k)   # 构建fat tree topology
    print("Fat Tree({}) with {} nodes.".format(args.k, ft.number_of_nodes()))

    hosts = set()
    for n in ft.nodes():
        if ft.nodes[n]['type'] == 'host':
            hosts.add(n)

    if args.do_limit_memory:
        memory_limit_host = [0,1,2,3, 4,5,6,7, 16,17,18,19]
    else:
        memory_limit_host = []

    logging.info(f"memory_limit_host: {memory_limit_host}")
    tic1 = time.perf_counter()
    all_flows = PathGenerator(ft, hosts).generate_flows(args.n_flows)
    headlinesPrint("Generate Flows Succeed")
    tic2 = time.perf_counter()
    print(f"Generate Flows time: {tic2 - tic1}s")
    logging.info(f"Generate Flows time: {tic2 - tic1}s")

    # size distribution: expovariate distribution
    # with lambda = 1.0 / mean_pkt_size, which means E(size) = mean_pkt_size
    size_dist = partial(expovariate, 1.0 / args.mean_pkt_size)
    true_flow_size = dict()
    for fid in all_flows:
        arr_dist = partial(expovariate, args.arr_dist_alpha)
        pg = ZipfPacketGenerator(env, f"Flow_{fid}",
                                 arr_dist,size_dist,
                                 flow_id=fid, alpha=args.flow_alpha,
                                 begin=args.begin,end=args.end)

        true_flow_size[fid] = pg.flow_size

        ps = PacketSink(env)
        all_flows[fid].pkt_gen = pg
        all_flows[fid].size = pg.flow_size
        all_flows[fid].pkt_sink = ps

    ft = generate_fib(ft, all_flows)
    total_packet_num = sum(true_flow_size.values())
    print(f"Total number of packets = {total_packet_num}")
    logging.info(f"Total number of packets = {total_packet_num}")
    assert total_packet_num < 12000000

    args.heavy_change_thres = int(total_packet_num * args.heavy_change_ratio)

    n_classes_per_port = 4
    weights = {c: 1 for c in range(n_classes_per_port)}
    def flow_to_classes(f_id, n_id=0, fib=None):
        return (f_id + n_id + fib[f_id]) % n_classes_per_port
    for node_id in ft.nodes():
        node = ft.nodes[node_id]
        flow_classes = partial(flow_to_classes, n_id=node_id, fib=node['flow_to_port'])
        if node_id in memory_limit_host:        #limited memory
            node['device'] = MonitorSwitchDHT(env,ft, args.memory_limit, args.flowtable_size_limit, args.hashtable_size_limit, args.threshold_ratio,
                                              args.is_dht, args.k, args.pir, args.buffer_size, weights, 'DRR', flow_classes,
                                              element_id=node_id, all_flows = all_flows)
        else:                       #memory
            node['device'] = MonitorSwitchDHT(env, ft, args.memory, args.flowtable_size, args.hashtable_size, args.threshold_ratio,
                                              args.is_dht, args.k, args.pir, args.buffer_size, weights, 'DRR', flow_classes,
                                              element_id=node_id, all_flows=all_flows
                                              )
        node['device'].demux.fib = node['flow_to_port']

    for n in ft.nodes():
        node = ft.nodes[n]
        for port_number, next_hop in node['port_to_nexthop'].items():
            node['device'].ports[port_number].out = ft.nodes[next_hop]['device']
    for target_flow_id, flow in all_flows.items():
        flow.pkt_gen.out = ft.nodes[flow.src]['device']
        ft.nodes[flow.dst]['device'].demux.ends[target_flow_id] = flow.pkt_sink

    logging.info(args)
    logging.info(f"total_packet_num:{total_packet_num}")

    controller = GlobalController(env, args.do_dynamic, interval=args.inter, finish=args.finish, topo=ft, all_flows = all_flows)
    MonitorData.setHashRings(args.k, 250)
    MonitorData.initialWeight(ft)
    if args.do_limit_memory:
        for index in memory_limit_host:
            MonitorData.adjustNodeWeight(ft, index, args.memory_limit_ratio)

    print("Global weights of switches", MonitorData.global_weights)
    logging.info(f"Global weights of switches: {MonitorData.global_weights}")

    headlinesPrint("Simulation Started")
    tic1 = time.perf_counter()
    env.run(until=100001)
    tic2 = time.perf_counter()
    headlinesPrint(f'Simulation Finished at {env.now}')
    print(f"Simulation consumed time {tic2 - tic1} (s).")
    logging.info(f"Simulation consumed time {tic2 - tic1} (s).")

    headlinesPrint("Tasks")
    logging.info(f"Start tasks...")
    from Tasks import queryflow, flow_estimate, heavy_hitter, heavy_change, flow_cardinality, flow_entropy, max_interval
    true, est_noflow, est_hasflow, device_true_segment, result_segment, original_flow_table_segment, \
    sketch_segment, ground_truth_segment_len, entropy_segment = queryflow(ft, args.memory, args)
    print(f"{len(est_hasflow)} = {len(true)} ==  {len(args.memory)}")

    # f = open(f"{args.log_file}.p", "wb")
    # pickle.dump((true, est_noflow, est_hasflow, device_true_segment, result_segment, original_flow_table_segment,sketch_segment, ground_truth_segment_len, entropy_segment), f)
    # f.close()

    device_true_segment_result = merge_switch_to_segment(device_true_segment)
    est_hasflow_segment = merge_switch_to_segment(est_hasflow)

    all_are_esti, all_aae_esti, all_are_hit, all_aae_hit, all_precision, all_recall, all_f1, all_cardinality, all_entropy, all_are_max, all_aae_max = [],[],[],[],[],[],[],[],[],[],[]
    all_pre_hit, all_recall_hit, all_f1_hit = [], [], []
    all_distri = []
    for num in range(len(args.memory)):
        # print(f"\n{num}, {args.memory[num]/1024} KB: ")

        are_esti, aae_esti = flow_estimate(true, est_hasflow[num])

        are_hit, aae_hit, pre_hit, recall_hit, f1_hit = heavy_hitter(device_true_segment_result, result_segment[num], args.heavy_hitter_thres)     #每segment挨个做are 然后求均值

        precision, recall, f1 = heavy_change(args.heavy_change_thres, device_true_segment_result, result_segment[num])

        entropy = flow_entropy(device_true_segment_result, entropy_segment[num])

        cardinality = flow_cardinality(original_flow_table_segment[num], sketch_segment[num], ground_truth_segment_len)

        are_max, aae_max = max_interval(dict(true), dict(est_hasflow[num]))

        all_are_esti.append(are_esti)
        all_aae_esti.append(aae_esti)
        all_are_hit.append(are_hit)
        all_aae_hit.append(aae_hit)
        all_pre_hit.append(pre_hit)
        all_recall_hit.append(recall_hit)
        all_f1_hit.append(f1_hit)
        all_precision.append(precision)
        all_recall.append(recall)
        all_f1.append(f1)
        all_entropy.append(entropy)
        all_cardinality.append(cardinality)
        all_are_max.append(are_max)
        all_aae_max.append(aae_max)

    # logging.info(
    #     f"\n\nn_flows: {args.n_flows}, memory:{args.memory}, is_dht:{args.is_dht}, heavy_hitter_thres:{args.heavy_hitter_thres}, heavy_change_ratio:{args.heavy_change_ratio}")
    # logging.info(args)
    # logging.info(f"\n\n\t\t\testimate:\nare:{np.round(all_are_esti,5)} \naae:{np.round(all_aae_esti,5)}")
    # logging.info(f"\t\t\theavyhitter:\nare:{np.round(all_are_hit,5)} \naae:{np.round(all_aae_hit,5)}, \npre:{np.round(all_pre_hit,5)} \nrecall:{np.round(all_recall_hit,5)} \nf1:{np.round(all_f1_hit,5)}")
    # logging.info(f"\t\t\theavychange:\npre:{np.round(all_precision,5)} \nrecall:{np.round(all_recall,5)} \nf1:{np.round(all_f1,5)}")
    # logging.info(f"\t\t\tentropy:\n{np.round(all_entropy,5)}")
    # logging.info(f"\t\t\tcardinality:\n{np.round(all_cardinality, 5)}")
    # logging.info(f"\t\t\tmax_inter:\nare:{np.round(all_are_max,5)} \naae:{np.round(all_aae_max,5)}")
