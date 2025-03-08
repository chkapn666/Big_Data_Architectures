import networkx as nx
from itertools import combinations
from random import random


def ER(n, p):
    V = set([v for v in range(n)])
    E = set()
    for combination in combinations(V, 2):
        a = random()
        if a < p:
            E.add(combination)

    g = nx.Graph()
    g.add_nodes_from(V)
    g.add_edges_from(E)

    return g


n = 100
p = 0.3
G = ER(n, p)
with open('url_pages.txt', 'w') as f:
    for (k, v) in dict(G.adjacency()).items():
        adjs = [str(v) for v in list(v.keys())]
        f.write('{0} {1}\n'.format(k, ' '.join(adjs)))

#pos = nx.spring_layout(G)
#nx.draw_networkx(G, pos)
#plt.title("Random Graph Generation Example")
#plt.show()
