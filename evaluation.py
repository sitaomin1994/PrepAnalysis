import networkx as nx
import numpy as np
from  sklearn.metrics.pairwise import cosine_similarity
from scipy.spatial import distance_matrix


def evaluate_embedding(embedding, graph):

    nodes = [(node, len(list(graph.neighbors(node)))) for node in graph.nodes()]

    nodes_sort = sorted(nodes, key = lambda x: x[1], reverse = True)

    n_frequent_nodes = 3000

    nodes = [n[0] for n in nodes_sort[0:n_frequent_nodes]]

    num_less_accu = 0
    num_middle_accu = 0
    num_high_accu = 0
    accuracy = []
    n_nodes = n_frequent_nodes
    similarity_matrix = cosine_similarity(embedding)

    for idx, node in enumerate(nodes):

        if(idx%100 == 0):
            print(idx)

        nbrs = list(graph.neighbors(node))
        n_nbrs = len(nbrs)
        if n_nbrs >= 100:
            top_k = 10 #int(n_nbrs*0.1)
        else:
            top_k = int(n_nbrs*0.2) if int(n_nbrs*0.2) >= 1 else 1 #int(n_nbrs*0.5)

        top_k_emb = 600 #int(n_nodes*0.1)

        # print(top_k)

        edges = np.array([graph[node][nbr]['weight'] for nbr in nbrs])
        nbrs_arr = np.array([int(nbr) for nbr in nbrs])

        # print(edges)
        # print(nbrs_arr)

        top_k_graph = nbrs_arr[np.argpartition(edges, -top_k)[-top_k:]]

        node_sim = similarity_matrix[int(node),]

        top_k_emb = np.argpartition(node_sim, -(top_k_emb))[-(top_k_emb):]

        top_both = set(top_k_graph).intersection(set(top_k_emb))

        # print(top_k_emb)
        # print(top_k_graph)
        # print(top_both)

        accu = len(top_both) / top_k

        # print(accu)

        if accu > 0.7:
            num_high_accu += 1
        elif accu > 0.4:
            num_middle_accu += 1
        else:
            num_less_accu += 1

        accuracy.append(accu)

    return sum(accuracy)/n_nodes, num_high_accu/n_nodes*100, num_middle_accu/n_nodes*100, num_less_accu/n_nodes*100


def evaluate_largeVis(coords, graph):

    dis_matrix = distance_matrix(coords, coords)
    nodes = [(node, len(list(graph.neighbors(node)))) for node in graph.nodes()]
    nodes_sort = sorted(nodes, key=lambda x: x[1], reverse=True)
    n_frequent_nodes = graph.number_of_nodes()
    nodes = [n[0] for n in nodes_sort[0:n_frequent_nodes]]
    num_less_accu = 0
    num_middle_accu = 0
    num_high_accu = 0
    accuracy = []
    n_nodes = n_frequent_nodes

    print(n_nodes)

    for idx, node in enumerate(nodes):

        if (idx%1000 == 0):

            print(idx)

        nbrs = list(graph.neighbors(node))
        n_nbrs = len(nbrs)
        # set numbers
        if n_nbrs >= 100:
            top_k = 10  # int(n_nbrs*0.1)
        else:
            top_k = int(n_nbrs * 0.2) if int(n_nbrs * 0.2) >= 1 else 1  # int(n_nbrs*0.5)
        top_k_emb = 600  # int(n_nodes*0.1)

        # graph
        edges = np.array([graph[node][nbr]['weight'] for nbr in nbrs])
        nbrs_arr = np.array([int(nbr) for nbr in nbrs])
        top_k_graph = nbrs_arr[np.argpartition(edges, -top_k)[-top_k:]]
        # embedding
        node_sim = dis_matrix[int(node),]
        top_k_emb = np.argpartition(node_sim, (top_k_emb))[:(top_k_emb)]
        top_both = set(top_k_graph).intersection(set(top_k_emb))

        accu = len(top_both) / top_k

        if accu > 0.7:
            num_high_accu += 1
        elif accu > 0.4:
            num_middle_accu += 1
        else:
            num_less_accu += 1

        accuracy.append(accu)

    return sum(accuracy) / n_nodes, num_high_accu / n_nodes * 100, num_middle_accu / n_nodes * 100, num_less_accu / n_nodes * 100

if __name__ == '__main__':

    print("Reading file ... ")
    embedding = np.load("./data/embedding_netmf64_ord1_m20.npy")
    graph =  nx.read_weighted_edgelist("./graph/sub_G_m20.edgelist")
    with open("./graph/graph_vec2D_m20.txt", "r") as f:
        coords = f.readlines()

    coords = coords[1:]
    coords = [[int(coord.split(" ")[0]), float(coord.split(" ")[1]), float(coord.split(" ")[2])] for coord in coords]
    coords = sorted(coords, key = lambda x: x[0])
    idx = [coord[0] for coord in coords]
    coords = np.array([[coord[1], coord[2]] for coord in coords])

    #print("evaluate embedding ... ")
    #accu, h_r, m_r, l_r = evaluate_embedding(embedding, graph)

    print("evaluate largevis ...")
    accu, h_r, m_r, l_r = evaluate_largeVis(coords, graph)


    print(accu, h_r, m_r, l_r)



