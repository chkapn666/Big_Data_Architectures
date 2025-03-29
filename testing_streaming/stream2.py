from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a SparkContext
spark = SparkContext("local[2]", "WordCount")

# Create a StreamingContext with a 10-second interval
ssc = StreamingContext(spark, 10)

# Read text files from the directory
lines = ssc.socketTextStream('localhost', 9999)

# Split lines into words
words = lines.flatMap(lambda line: line.split(" "))

# Map words to (word, 1) pairs
pairs = words.map(lambda word: (word, 1))

# Reduce pairs by key to get word counts
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print word counts
wordCounts.pprint()

# Start the computation
ssc.start()

# Wait for termination
ssc.awaitTermination()

