from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)

# Read data from HDFS
data = sc.textFile("/user/cloudera/pyspark/shopify_about_word_count.txt")

# Apply Data Transformation 
dataFlatMap = data.flatMap(lambda x: x.split(" "))
dataMap = dataFlatMap.map(lambda x: (x, 1))
dataReduceByKey = dataMap.reduceByKey(lambda x,y: x + y)

# Apply Actions
dataReduceByKey.saveAsTextFile("/user/cloudera/pyspark/wordcount_output")
for rec in dataReduceByKey.collect():
  print(rec)
