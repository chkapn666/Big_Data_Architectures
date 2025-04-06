### Importing libraries and dependencies

from pyspark import SparkContext
# For some reason i need to run this every time in order to get it work
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home" 

sc = SparkContext("local","app")
####### ####### ####### #######


rdd = sc.textFile("file:///" + os.getcwd() + "/data.txt", 3)  # the ```minPartitions=3``` parameter is just a hint, NOT a guarantee!!!
# The 'rdd' is an rdd containing sub-rdds, each representing a single line of the input text file 

numAs = rdd.filter(lambda s: 'a' in s).count()  # the 'filter' method will iterate through each line-level rdd and keep only those sub-rdds
# for which this logical expression evaluates to True; then I count() these sub-RDDs to get all true values
numBs = rdd.filter(lambda s: 'b' in s).count()

print(f"Lines with 'a': {numAs}, lines with 'b': {numBs}")


