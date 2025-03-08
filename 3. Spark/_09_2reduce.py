from pyspark import SparkContext

sc = SparkContext()

nums = sc.parallelize([1, 2, 3, 4, 5])

sum_sq = nums.map(lambda x: x * x).reduce(lambda x, y: x + y)

print('Sum of squares: {0}'.format(sum_sq))
