# Imports
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import json
import os
import subprocess

# Functions
def main(sc,argument):
    
    monthyr = 'hdfs://localhost:9000/user/haduser/' + argument
    print monthyr

    # declare an empty RDD for aggregating later
    finalRdd = sc.emptyRDD()
    
    # find the individual filenames
    cmd = ('hdfs dfs -find ' + monthyr + ' -name pagecount*').split()
    files = subprocess.check_output(cmd).strip().split('\n')

    # for each file, calculate total number of visits, total page size returned and count of searches
    for path in files:
        # trim the data to extract relevant columns, with language as key and visits & size as values
        single_file = sc.textFile(path).map(trimmed_data)

        # declare the format of values as three separate integers
        # first value [0] is summed to derive visits for each language
        # second value [1] is summed to derive total pagesizes each language
        # third value [2] is derived to count number of searches
        total_hits = single_file.aggregateByKey((0,0,0),(lambda x, y: (x[0]+y[0],x[1]+y[1],x[2]+1)),(lambda rdd1, rdd2: (rdd1[0]+rdd2[0], rdd1[1]+rdd2[1], rdd1[2]+rdd2[2])))

        # RDDs are added with each iteration
        finalRdd = finalRdd.union(total_hits)

    # same aggregation is performed once all RDDs are iterated over
    final_total_hits = finalRdd.aggregateByKey((0,0,0),(lambda x, y: (x[0]+y[0],x[1]+y[1],x[2]+1)),(lambda rdd1, rdd2: (rdd1[0]+rdd2[0], rdd1[1]+rdd2[1], rdd1[2]+rdd2[2])))

    # second level of aggregation is performed to derive average pagesize per language
    # final output shows key (language), total visits and average pagesize per key (language)
    convertedRdd = final_total_hits.map(lambda (k, v): convertToRdd(k, v))

    sparkSession = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/demoDB.wikipedia_stats") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/demoDB.wikipedia_stats") \
        .getOrCreate()

    wikipediaDataFrame = sparkSession.createDataFrame(convertedRdd, ["language", "total_hits", "average_pagesize"])

    wikipediaDataFrame.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

def convertToRdd(k, v):
    return k, v[0], v[1]/v[2]

# data is trimmed to only return key and value pair
# language as key, and visits & pagesize as values
def trimmed_data(data):
    language,name,visits,size = data.split(" ")
    return language,(int(visits),int(size))

# Spark configuration
sc = SparkContext()

# monthyear command-line argument is captured within the code 
argument = sys.argv[1]

# Execute Main functionality
main(sc, argument)
