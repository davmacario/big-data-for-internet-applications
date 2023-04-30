# Probably won't need to deliver this, as it's been implemented in a 
# Jupyter cell

from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Exercise 3")
sc = SparkContext(conf = conf)

rdd = sc.textFile("/data/students/bigdata_internet/lab1/lab1_dataset.txt")
fields_rdd = rdd.map(lambda line: line.split(','))
reduced_rdd = fields_rdd.reduceByKey(lambda v1, v2: int(v1)+int(v2))
sample_loc = reduced_rdd.take(3)
print(sample_loc)

reduced_rdd.saveAsTextFile('hdfs://BigDataHA/user/s315054')
