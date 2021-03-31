from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time

kafka_topic_name = "orderstopic"
kafka_bootstrap_servers ='localhost:9092'
customers_data_file_path = ""

mysql_host_name = "localhost"
mysql_port_no = "3306"
mysql_database_name = "sales_db"
mysql_driver_class = "com.mysql.jdbc.Driver"
mysql_table_name = "total_sales_by_source_state"
mysql_user_name = "newuser1"
mysql_password = "1234"
mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name

cassandra_host_name = "localhost"
cassandra_port_no = "9042"
cassandra_keyspace_name = "sales_db"
cassandra_table_name = "orders_tbl"

def save_to_cassandra(current_df, epoc_id):
	print("printing epoc id:")
	print(epoc_id)

	print("Printing before Cassandra table save:" + str(epoc_id))
	current_df \
		.write \
		.format("org.apache.spark.sql.cassandra")\
		.mode("append")\
		.options(table=cassandra_table_name, keyspace=cassandra_keyspace_name)\
		.save()
	print("Printing before Cassandra table save:"+ str(epoc_id))

def save_to_mysql(current_df, epoc_id):
	db_credentials = {"user": mysql_user_name,
					"password":mysql_password,
					"driver": mysql_driver_class}

	print("Printing epoc_id:")
	print(epoc_id)

	processed_at = time.strftime("%Y-%m-%d %H:%M:%S")

	current_df_final = current_df \
		.withColumn("processed_at", lit(processed_at)) \
		.withColumn("batch_id", lit(epoc_id))

	print("Printing before nysql table save:"+ str(epoc_id))
	current_df_final \
		.write \
		.jdnc(url=mysql_jdbc_url,
			table=mysql_table_name,
			mode="append",
			properties=db_credentials)
	print("Printing after Mysql table save:"+str(epoc_id))


if __name__=="__main__":
	print("welcome to datamaking !!!")
	print("data processing application started")
	print(time.strftime("%Y-%m-%d %H:%M:%s"))

	spark = SparkSession.builder \
			.appName("PySpark Structured Streaming with Kafka and Cassandra")\
			.master("local[*]")\
			.config('spark.cassandra.connection.host',cassandra_host_name)\
			.config('spark.cassandra.connection.port',cassandra_port_no)\
			.getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")


	orders_df = spark \
		.readStream \
		.format("kafka")\
		.option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
		.option("subscribe", kafka_topic_name) \
		.option("startingOffsets","latest")\
		.load()

	print("printing schema of oreders:")
	orders_df.printschema()

	orders_df1 = orders_df.selectExpr("CAST(value AS STRING)","timestamp")
