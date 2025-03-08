from pyspark import SparkContext

sc = SparkContext("local[1]", '6107app')

rdd = sc.parallelize(range(10000))

sumC = rdd.aggregate((0, 0),
                     lambda acc, val: (acc[0] + val, acc[1] + 1),
                     lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
                    )
print(sumC[0] / sumC[1])
