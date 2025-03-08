from pyspark import SparkContext

IterN = 2
INFNAME = 'url_pages1.txt'
OUTFNAME = 'ranks1.txt'

sc = SparkContext('local[*]', 'PageRank')

urls = sc.textFile('url_pages1.txt')
tlinks = urls.map(lambda line: line.split()).map(lambda ntry: list(map(int, ntry)))
links = tlinks.map(lambda ntry: (ntry[0], ntry[1:]))
# sequence of rank values is maintained as a list
ranks = links.map(lambda link: (link[0], [1.0]))


def compute_contribs(pair):
    [url, [links, ranks]] = pair  # split key-value pair
    return [(dest, ranks[-1] / len(links)) for dest in links]

# purely functional approach, efficiency can be improved with the use of dictionaries
for i in range(IterN):
    contribs = links.join(ranks).flatMap(compute_contribs)
    nranks = contribs.reduceByKey(lambda x, y: x + y) \
                     .mapValues(lambda x: 0.15 + 0.85 * x)
    ranks = ranks.join(nranks).mapValues(lambda x: x[0] + [x[1]])


ranksf = ranks.collect()
with open(OUTFNAME, 'w') as f:
    for p in ranksf:
        f.write('{0} '.format(p[0]))
        f.write(' '.join(map(lambda x: '{0:.2f}'.format(x), p[1])))
        f.write('\n')
