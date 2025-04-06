from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pandas as pd 

# For some reason i need to run this every time in order to get it work
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home" 

# SparkSession is our gateway to spark DFs functionality 

# the .config("option", "value") arguments allow us to perform refined file I/O
spark = SparkSession.builder.master("local[1]") \
            .appName("app") \
            .config("option", "value") \
            .getOrCreate()

df_salaries = spark.read.csv('file:///' + os.getcwd() + '/salariesH.csv', header=True)  # there is a parameter 'header' that is by default set to False!
#df_salaries.printSchema()
#df_salaries.show()

df_tweets = spark.read.option("multiline", "true").json("file:///" + os.getcwd() + '/tweets.json')
#df_tweets.printSchema()
#df_tweets.show()

# Simple methods:
df_salaries_gross = df_salaries.select(
                df_salaries["Name"], 
                (df_salaries["Monthly"] + df_salaries["Bonus"]).alias("gross pay")
                )
df_salaries_gross.filter(
                df_salaries_gross["gross pay"] > 3200
            ).show()

data = [['Nick', 26], ['Helen', 28], ['Mary', 30], ['John', 28]]
df = pd.DataFrame(data, columns=['Name', 'Age'])
sdf = spark.createDataFrame(df)
sdf.groupBy(sdf['Age']).count().show()


sdf.registerTempTable("df")  # make sure to invoke this method on the Spark DF and NOT the pandas DF !!!
result = spark.sql("""
    SELECT * 
    FROM df 
    WHERE Age > 29
    ORDER BY Age DESC
    LIMIT 1
""")
result.show()

