from pyspark import SparkContext

sc = SparkContext("local[1]", '6107app')

colors = {'blue': (0, 0, 255), 'green': (0, 255, 0), 'red': (255, 0, 0),
          'yellow': (255, 255, 0), 'white': (255, 255, 255), 'black': (0, 0, 0)}

bcolor = sc.broadcast(colors)

articles = [('table', 'green'), ('fridge', 'white'), ('TV', 'black'), ('book', 'yellow')]

rdd = sc.parallelize(articles)
rdd1 = rdd.map(lambda x: (x[0], colors[x[1]]))
print(rdd1.collect())
