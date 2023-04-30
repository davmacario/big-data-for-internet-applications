from pyspark import SparkContext, SparkConf
import sys
import time

start = time.time()
# The prefix is passed as a command line argument (first one)
prefix = sys.argv[1]
# The output path is the second command line argument 
outputPath = sys.argv[2]
# Input file path (HDFS)
path = '/data/students/bigdata_internet/lab2/word_frequency.tsv'
# Create SparkContext object
conf = SparkConf().setAppName('Exercise 02, lab 02')
sc = SparkContext(conf=conf)
# Create pair RDD (sepRDD)
inRDD = sc.textFile(path)
sepRDD = inRDD.map(lambda l: (l.split('\t')[0], int(l.split('\t')[1])))
# Isolate elements whose key starts with the specified prefix
filteredRDD = sepRDD.filter(lambda couple: couple[0].startswith(prefix))
# Produce output file
outRDD = filteredRDD.map(lambda c: c[0]+' - '+str(c[1])+',')
outRDD.saveAsTextFile(outputPath)
stop = time.time()
print(f"The program takes {stop-start} seconds to run")
