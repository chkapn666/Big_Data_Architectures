from pyspark import SparkContext

maxNum = 100_000

sc = SparkContext("local[1]", "app")
rdd = sc.parallelize(range(maxNum), 5)

print("No# partitions:", rdd.getNumPartitions())
print("Action: First Element", rdd.first())
print("Action: invoke built-in sum and mean methods,", (rdd.sum(), rdd.mean()))

print("Action: first 10 values of the RDD")
for n in rdd.take(10):
    print(n)

print("Action: RDD values smaller than 10")
rdd = rdd.filter(lambda x: x < 10).foreach(print)

rddCollect = rdd.collect()
print("Action: RDD converted to Array[Int], first ten values:")
for n in rddCollect[:10]:
    print(n)