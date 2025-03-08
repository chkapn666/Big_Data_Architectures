import matplotlib.pyplot as plt

FNAME = 'ranks1.txt'

ppage = 2  # page to plot ratings

with open(FNAME) as f:
    while (True):
        line = f.readline()
        if not line:
            print('Page {0} not found.'.format(ppage))
            break
        line = line.strip().split()
        page = int(line[0])
        ranks = list(map(lambda x: float(x), line[1:]))
        if page == ppage:
            plt.bar([str(i) for i in range(len(ranks))], ranks, width=0.5)
            plt.ylabel('Rank')
            plt.xlabel('Iteration')
            plt.title('Ranks for page {0}.'.format(page))
            plt.show()
            break
