from pyspark import SparkContext

sc = SparkContext()
lines = sc.textFile('1661-0.txt')
counts = lines.flatMap(lambda line: line.split(' ')) \
              .map(lambda word: (word, 1)) \
              .reduceByKey(lambda x, y: x + y) \
              .sortBy(lambda kv: kv[1], ascending=False) \
              .take(10)

print(counts)

