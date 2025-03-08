from pyspark import SparkContext

sc = SparkContext()

nums = sc.parallelize([1, 2, 3, 4, 5])

sq = nums.map(lambda x: x * x).collect()
for n in sq:
    print('{0}'.format(n))
