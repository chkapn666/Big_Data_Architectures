from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

# Create a SparkContext
spark = SparkContext("local[2]", "JSON processing")

# Create a StreamingContext with a 10-second interval
ssc = StreamingContext(spark, 10)

# Read text files from the directory
lines = ssc.socketTextStream('localhost', 9999)

lines.pprint()
parsed = lines.map(lambda v: json.loads(v))
parsed.count() \
        .map(lambda x: 'Tweets in this batch: %s' % x) \
        .pprint()

authors = parsed.map(lambda tw: tw["user"])
authors_c = authors.countByValue()
authors_c.pprint()

# Start the computation
ssc.start()

# Wait for termination
ssc.awaitTermination()


