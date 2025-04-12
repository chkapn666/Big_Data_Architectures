from pyspark import SparkContext
import os
os.chdir('/Users/chkapsalis/Documents/GitHub/Big_Data_Architectures/previous_final_exam')
from datetime import datetime 
from time import sleep 


# For some reason i need to run this every time in order to get it work
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home" 

spark = SparkContext("local[1]", "app")

cols = 'transaction_id,transaction_date,transaction_time,store_id,store_location,product_id,transaction_qty,unit_price,Total_Bill,product_category,product_type,product_detail,Size,Month Name,Day Name,Hour,Month,Day of Week'.split(',')

# We're ingesting a csv file using plain Spark methods - no spark Dataframes.
# This necessitates the manual removal of the header row in the 'CoffeeShop.csv' file.

def parse(str_row):
    fields = str_row.split(',')
    try:
        transaction_date = datetime.strptime(fields[1], "%d-%m-%Y")
        transaction_month = datetime.strftime(transaction_date, "%m-%Y")
        store_location = fields[4]
        bill = float(fields[8])
        product_category = fields[9]
        product_detail = fields[11]
        return (transaction_month, store_location, bill, product_category, product_detail)
    except: 
        return  # ignoring invalid lines - this will return a None element; i will filter it out


rdd = spark.textFile('file:///' + os.getcwd() + '/CoffeeShop.csv') \
    .map(parse) \
    .filter(lambda x: x is not None)  # ignoring invalid lines

#print(rdd.take(5))  # peaking into the formulation of the super-rdds contents

### Questions 1 & 2 ### 
q12_rdd = rdd.map(lambda x: (x[0], (x[2], 1)))  # the first value is a month value; it is the first value so it will be treated as the key in my app
q12_sol = q12_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
for month, (total_sum, total_count) in q12_sol.collect():
    print(f"In {month}, the CoffeeShop had {total_count} transactions leading to a revenue total of {total_sum}.")


### Question 3 ### 
# !!! Comparison of a column's values to a date happens as if I am comparing strings !!!
q3_rdd = rdd.filter(lambda x: (x[3] == "Coffee") & (x[0] == "06-2023")) \
            .map(lambda x: (1,1)) \
            .reduceByKey(lambda x, y: x + y)  # i mapped all relevant lines/sub-rdds to a particular common 
            # key (1) - I could have used whatever non-null key!
print(f"There were {q3_rdd.collect()[0][1]} transactions in June 2023 which contained Coffee")


### Question 4 ### 
q4_rdd = rdd.filter(lambda x: x[1] == "Astoria") 
            
zeroValue = (0,0)  # zeroValue for (total_sum, total_count)

def seqOp(acc, v):
    return (acc[0] + v[2], acc[1] + 1)

def combOp(acc1, acc2):
    return (acc1[0] + acc2[0], acc1[1] + acc2[1])

total_q4_sum, total_q4_count = q4_rdd.aggregate(
    zeroValue,
    seqOp,
    combOp
)
print(f"The average bill amount for the Astoria store was {round(total_q4_sum / total_q4_count, 2)}.")


### Question 5 ### 
q5_rdd = rdd.filter(lambda x: (x[-1] == "Brazilian-Organic") & (x[1] == "Lower Manhattan"))
q5_total_count = q5_rdd.aggregate(
    0,
    (lambda acc, v: acc + 1),
    (lambda acc1, acc2: acc1 + acc2)
)
print(f"The total number of transactions for which Brazilian-Organic Coffee Beans were sold in the Lower Manhattan store is {q5_total_count}.")