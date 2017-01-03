from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

jdbcurl = 'jdbc:mysql://localhost:3306/retail_db?user=retail_dba&password=cloudera'

df = sqlContext.load(source="jdbc", url=jdbcurl, dbtable="departments")
rdd = df.rdd

print(type(rdd))
for rec in rdd.take(5):
  print(rec)

rdd.saveAsTextFile("/user/cloudera/pyspark/departments")
