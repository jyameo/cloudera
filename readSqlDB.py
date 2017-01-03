from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

jdbcurl = 'jdbc:mysql://localhost:3306/retail_db?user=retail_dba&password=cloudera'

df = sqlContext.load(source="jdbc", url=jdbcurl, dbtable="departments")

for rec in df.collect():
  print(rec)

