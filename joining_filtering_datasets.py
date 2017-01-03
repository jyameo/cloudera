'''
	Problem: Check if there are any cancelled orders with amount greater than 1000$
'''

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)

# Read data from HDFS
ordersRDD = sc.textFile("/user/cloudera/pyspark/orders")
orderItemsRDD = sc.textFile("/user/cloudera/pyspark/order_items")

# Filter data into a smaller dataset - Get only cancelled orders
ordersParsedRDD = ordersRDD.filter(lambda rec: "CANCELED" in rec.split(",")[7]).map(lambda rec: (int(rec.split(",")[0].split("=")[1]), rec))

# Apply Data Transformations
orderItemsParsedRDD = orderItemsRDD.map(lambda rec: (int(rec.split(",")[1].split("=")[1]), float(rec.split(",")[4].split("=")[1])))

# Generate sum(order_item_subtotal) per order
orderItemsAgg = orderItemsParsedRDD.reduceByKey(lambda acc, value: (acc + value))

# Join orders and order items
ordersJoinOrderItems = orderItemsAgg.join(ordersParsedRDD)

# Filter data which amount to greater than 1000$
ordersResults = ordersJoinOrderItems.filter(lambda rec: rec[1][0] >= 1000)

for rec in ordersResults.take(5):
  print(rec)
