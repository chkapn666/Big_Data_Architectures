from pyspark import SparkContext

IterN = 3

sc = SparkContext()

links = sc.parallelize([(1, [2]), (2, [4]), (3, [1, 2]), (4, [2, 3])]) # RDD of (url, neighbors) pairs
ranks = sc.parallelize([(1, 1), (2, 1), (3, 1), (4, 1)])  # RDD of (url, rank) pairs


def compute_contribs(pair):
    [url, [links, rank]] = pair  # split key-value pair
    return [(dest, rank / len(links)) for dest in links]


for i in range(IterN):
    contribs = links.join(ranks).flatMap(compute_contribs)
    ranks = contribs.reduceByKey(lambda x, y: x + y) \
                    .mapValues(lambda x: 0.15 + 0.85 * x)

print(ranks.collect())
