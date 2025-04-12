from pyspark import SparkContext
import os
# os.chdir('/Users/chkapsalis/Documents/GitHub/Big_Data_Architectures/Assignments/my_assignment_3')

# For some reason i need to run this every time in order to get it work
# os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home" 

spark = SparkContext("local[1]", "app")

datafile= spark.textFile('file:///' + os.getcwd() + '/Global_Cybersecurity_Threats_2015-2024.csv')

import datetime 

def parse(row):
    """ This function allows for the more straightforward ingestion of the file's contents per line. 
    It also facilitates making sure we ignore any invalid lines"""
    try:
        country = row[0]
        year = int(row[1])
        attack_type = row[2]
        target_industry = row[3]
        fin_loss = float(row[4])
        no_affected_users = int(row[5])
        vulnerability_type = row[7]
        resolution_time = int(row[9])
        return (country, year, attack_type, target_industry, fin_loss, no_affected_users,resolution_time)
    except:
        return  # Ignore invalid lines


# Now we will ingest all data of interest pertaining to flights; date,airline code,arr_delay,dept_delay
attacks = datafile.map(lambda full_line: full_line.split(',')) \
                .map(parse) \
                .filter(lambda x: x is not None).cache()  # this rdd will be used multiple times throughout the
                # exercise, so I cache its evaluation into the primary memory, to prevent spark from 
                # repeating the transformations that lead to it multiple times


### Question 1: Calculate total financial loss per country
# I create (country, fin_loss_value) pairs
total_loss_per_country = attacks.map(lambda x: (x[0], x[4])) \
                .reduceByKey(lambda x, y: x+y) \
                .collect()
print("*** Q1: Total Financial Loss Per Country ***")
print(total_loss_per_country)

### Question 2: Overall average number of affected users across attacks
result = attacks \
    .map(lambda x: (1, (1, x[5]))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .mapValues(lambda x: x[1] / x[0])  # Calculate the average

# Collect the result
count_attacks, avg_users_affected = result.collect()[0]
print("*** Q2: Overall average number of affected users across attacks ***")
print(avg_users_affected)


### Question 3: names of targeted industries per country
# I create (country, industry) pairs
industries_per_country = attacks.map(lambda x: (x[0], x[3])) \
                .distinct() \
                .groupByKey() \
                .map(lambda x: (x[0], list(x[1]))) \
                .collect()
print("*** Q3: names of targeted industries by country ***")
print(industries_per_country)

### Question 4: the number of attacks per year
# I create (year, attack) pairs
attacks_per_year = attacks.map(lambda x: (x[1], 1)) \
                .reduceByKey(lambda x, y: x+y) \
                .collect()
print("*** Q4: number of attacks per year ***")
print(attacks_per_year)

### Question 5: the sources of attack per country
# I create (country, attack) pairs
sources_per_country = attacks.map(lambda x: (x[0], x[2])) \
                .distinct() \
                .groupByKey() \
                .map(lambda x: (x[0], list(x[1]))) \
                .collect()
print("*** Q5: sources of attack per country ***")
print(sources_per_country)




### RESULTS ### 
### Question 1:
# *** Q1: Total Financial Loss Per Country ***                                    
#[('China', 13714.469999999988), ('India', 14566.119999999997), ('UK', 16502.98999999999), ('Germany', 15793.240000000002), ('France', 14972.280000000019), ('Australia', 15402.999999999996), ('Russia', 14734.73), ('Brazil', 15782.62000000001), ('Japan', 15197.34000000001), ('USA', 14812.12)]

### Question 2: 
# *** Q2: Overall average number of affected users across attacks ***
# 504684.1363333333

### Question 3: 
# *** Q3: names of targeted industries by country ***
# [('China', ['Education', 'Retail', 'Telecommunications', 'Healthcare', 'Government', 'IT', 'Banking']), ('India', ['IT', 'Banking', 'Education', 'Government', 'Healthcare', 'Retail', 'Telecommunications']), ('UK', ['Telecommunications', 'Healthcare', 'Education', 'Retail', 'IT', 'Banking', 'Government']), ('Germany', ['IT', 'Retail', 'Telecommunications', 'Banking', 'Healthcare', 'Education', 'Government']), ('France', ['Government', 'Healthcare', 'IT', 'Education', 'Telecommunications', 'Banking', 'Retail']), ('Australia', ['Banking', 'Government', 'Education', 'Healthcare', 'Telecommunications', 'Retail', 'IT']), ('Russia', ['Healthcare', 'Telecommunications', 'Education', 'Banking', 'IT', 'Retail', 'Government']), ('Brazil', ['Retail', 'Telecommunications', 'Healthcare', 'Banking', 'Education', 'IT', 'Government']), ('Japan', ['Telecommunications', 'Education', 'Banking', 'Retail', 'IT', 'Healthcare', 'Government']), ('USA', ['Retail', 'IT', 'Telecommunications', 'Education', 'Banking', 'Healthcare', 'Government'])]

### Question 4: 
# *** Q4: number of attacks per year ***
# [(2019, 263), (2017, 319), (2024, 299), (2018, 310), (2016, 285), (2023, 315), (2022, 318), (2015, 277), (2021, 299), (2020, 315)]

### Question 5: 
# *** Q5: sources of attack per country ***
# [('China', ['Phishing', 'Ransomware', 'SQL Injection', 'Man-in-the-Middle', 'Malware', 'DDoS']), ('India', ['Man-in-the-Middle', 'Ransomware', 'DDoS', 'SQL Injection', 'Phishing', 'Malware']), ('UK', ['Ransomware', 'DDoS', 'Malware', 'SQL Injection', 'Man-in-the-Middle', 'Phishing']), ('Germany', ['Man-in-the-Middle', 'DDoS', 'Ransomware', 'Phishing', 'SQL Injection', 'Malware']), ('France', ['SQL Injection', 'DDoS', 'Ransomware', 'Malware', 'Phishing', 'Man-in-the-Middle']), ('Australia', ['Phishing', 'DDoS', 'SQL Injection', 'Malware', 'Man-in-the-Middle', 'Ransomware']), ('Russia', ['Man-in-the-Middle', 'DDoS', 'SQL Injection', 'Malware', 'Ransomware', 'Phishing']), ('Brazil', ['Ransomware', 'DDoS', 'Phishing', 'Malware', 'Man-in-the-Middle', 'SQL Injection']), ('Japan', ['Phishing', 'Malware', 'Ransomware', 'SQL Injection', 'Man-in-the-Middle', 'DDoS']), ('USA', ['DDoS', 'Ransomware', 'Malware', 'SQL Injection', 'Phishing', 'Man-in-the-Middle'])]
