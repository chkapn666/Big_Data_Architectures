from pyspark import SparkContext
# For some reason i need to run this every time in order to get it work
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home" 

sc = SparkContext("local","app")

###########
## OPERATIONS ON RDDs WITH SINGLE-VALUE ELEMENTS
rdd = sc.parallelize([1,3,6,8,3,4,6,7])

print("Demonstrating the functionality of the FOLD method of Spark")
sum = rdd.fold(0, lambda x,y: x+y)
minv = rdd.fold(rdd.take(1)[0], lambda x,y: min(x,y))
maxv = rdd.fold(rdd.take(1)[0], lambda x,y: max(x,y))
print(f'Min: {minv}, Max: {maxv}, Sum: {sum}\n\n\n\n\n')


print("Demonstrating the functionality of the REDUCE method of Spark")
sum = rdd.reduce(lambda x,y: x+y)
minv = rdd.reduce(lambda x,y: min(x,y))
maxv = rdd.reduce(lambda x,y: max(x,y))
print(f'Min: {minv}, Max: {maxv}, Sum: {sum}\n\n\n\n\n')


print("Demonstrating Spark Operation Pipelines")
sum_sq = rdd.map(lambda x: x**2).reduce(lambda x,y: x+y)
print(f"Sum of Squares: {sum_sq}")


#########
## OPERATIONS ON RDDs WITH TUPLE-VALUE ELEMENTS ~ key-value pairs
pets = sc.parallelize([('cat', 1), ('dog', 1), ('cat', 2)])

rdd1 = pets.reduceByKey(lambda x, y: x + y)
print(rdd1.collect())

rdd2 = pets.sortByKey()
print(rdd2.collect())

rdd3 = pets.groupByKey()
print(rdd3.mapValues(lambda x: list(x)).collect())


#########
print("Demonstrating the functionality of the AGGREGATE method of Spark")

rdd = sc.parallelize([1,2,4,5,2,4,6,1])
r = rdd.aggregate(
    0, 
    lambda acc, v: acc + v,
    lambda acc1, acc2: acc1 + acc2
)

print(f"Aggregate output #1, {r}")


rdd2 = sc.parallelize([("A", 10), ("B", 20), ("B", 30), ("C", 40), ("D", 30), ("E", 60)])
r = rdd2.aggregate(
    0, 
    lambda acc, v: acc + v[1],
    lambda acc1, acc2: acc1 + acc2
)

print(f"Aggregate output #2, {r}")

rdd3 = sc.parallelize(range(10_000))
count, sum = rdd3.aggregate(
    (0,0),
    lambda acc, v: (acc[0] + 1, acc[1] + v),  # the values MUST be surrounded by parentheses - be put into tuples
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # the values MUST be in tuple form
)
print("Output #3", round(sum/count, 2))

#########
print("Demonstrating the functionality of the FOREACH method of Spark")
rdd4 = sc.parallelize(range(10))
rdd4.foreach(lambda x: print(x))