import networkx as nx
def buildLeafSpine(spines:int , leaves:int, hosts: int):
    """
    Leaf Spine
    m: number of 'spines'
    n: number of 'leaves'
    """
    # validate input arguments
    if not isinstance(spines, int):
        raise TypeError('m argument must be of int type')
    if not isinstance(leaves, int):
        raise TypeError('n argument must be of int type')
    if spines > leaves:
        raise ValueError('m must <= n')

    topo = nx.Graph()
    topo.name = f"leaf_spine_topology ({spines}, {leaves})"

    # Create spine nodes
    topo.add_nodes_from([v for v in range(spines)],
                        layer='spine',
                        type='switch')

    # Create leaf
    leaf_start_node = topo.number_of_nodes()
    leaf_end_node = leaf_start_node + leaves
    leaf_nodes = range(leaf_start_node, leaf_end_node)
    topo.add_nodes_from(leaf_nodes,
                        layer = 'leaf',
                        type = 'switch' )
    topo.add_edges_from([(u, v) for u in leaf_nodes for v in range(0, spines)], type = 'switch_edge')


    # Create hosts (each switch with 3 hosts)
    for u in [v for v in topo.nodes() if topo.nodes[v]['layer'] == 'leaf']:
        host_nodes = range(topo.number_of_nodes(),
                           topo.number_of_nodes() + hosts)
        topo.add_nodes_from(host_nodes,
                            layer='host',
                            type='host',
                            connect_leaf = u)
        topo.add_edges_from([(u, v) for v in host_nodes], type='leaf_edge')

    return topo