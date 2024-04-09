# Databricks notebook source
# Defining Cassandra connection details in Scala as it is only currently supported with Databricks
%scala
// define the cluster name and cassandra host name
val sparkClusterName = "test1"
val cassandraHostIP = "ec2-54-205-226-21.compute-1.amazonaws.com"

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.rm("/databricks/init/$sparkClusterName/cassandra.sh")

# COMMAND ----------

# MAGIC %scala
# MAGIC //adding the hostname to all worker nodes via init script
# MAGIC dbutils.fs.put(s"/databricks/init/$sparkClusterName/cassandra.sh",
# MAGIC   s"""
# MAGIC      #!/usr/bin/bash
# MAGIC      echo '[driver]."spark.cassandra.connection.host" = "$cassandraHostIP"' >> /home/ubuntu/databricks/common/conf/cassandra.conf
# MAGIC    """.trim, true)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

# COMMAND ----------

# Test the single batch file
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/email@gmail.com/spark-streaming/counterdata1.csv")
df1.show(5)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

from pyspark.streaming import StreamingContext
ssc = StreamingContext(sc, 5)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# define the schema
TrafSchema = StructType([ StructField("cosit", StringType(), True), StructField("year", StringType(), True), StructField("month", StringType(), True), StructField("day", StringType(), True), StructField("hour", StringType(), True), StructField("minute", StringType(), True), StructField("second", StringType(), True), StructField("millisecond", StringType(), True), StructField("minuteofday", StringType(), True), StructField("lane", StringType(), True), StructField("lanename", StringType(), True), StructField("straddlelane", StringType(), True), StructField("straddlelanename", StringType(), True), StructField("class", StringType(), True), StructField("classname", StringType(), True), StructField("length", StringType(), True), StructField("headway", StringType(), True), StructField("gap", StringType(), True), StructField("speed", StringType(), True), StructField("weight", StringType(), True), StructField("temperature", StringType(), True), StructField("duration", StringType(), True), StructField("validitycode", StringType(), True),StructField("numberofaxles", StringType(), True),StructField("axleweights", StringType(), True),StructField("axlespacings", StringType(), True)])


# COMMAND ----------

streamingInputDF = (
  spark
    .readStream
    .schema(TrafSchema)
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .format("csv")
    .load("dbfs:/FileStore/shared_uploads/email@gmail.com/streaming-files/")
)

# COMMAND ----------

# Total number of counts by vehicle class
streamingVehicleClassCountsDF = (
  streamingInputDF
    .groupBy(
      streamingInputDF.classname)
    .count()
)

# COMMAND ----------

streamingVehicleClassCountsDF.isStreaming

# COMMAND ----------

display(streamingVehicleClassCountsDF)

# COMMAND ----------

# Writing the Dataframe to Cassandra 
%scala
//Writing the dataframe directly to cassandra
import org.apache.spark.sql.cassandra._


streamingVehicleClassCountsDF.write
  .format("org.apache.spark.sql.cassandra")
  .mode("overwrite")
  .options(Map( "table" -> "streamingVehicleClassCountsDF", "keyspace" -> "test_keyspace"))
  .save()

# COMMAND ----------

query = (
  streamingVehicleClassCountsDF
    .writeStream
    .format("memory")      
    .queryName("counts")  
    .outputMode("complete")
    .start()
)

# COMMAND ----------

import pyspark.sql.functions as F
streamingVehicleClassAvgSpeedCountsDF = (
  streamingInputDF
    .groupBy(
      streamingInputDF.classname)
    .agg(F.round(F.avg(streamingInputDF.speed), 3))
)

# COMMAND ----------

display(streamingVehicleClassAvgSpeedCountsDF)

# COMMAND ----------

# Writing the Dataframe to Cassandra 
%scala
//Writing the dataframe directly to cassandra
import org.apache.spark.sql.cassandra._


streamingVehicleClassAvgSpeedCountsDF.write
  .format("org.apache.spark.sql.cassandra")
  .mode("overwrite")
  .options(Map( "table" -> "streamingVehicleClassAvgSpeedCountsDF", "keyspace" -> "test_keyspace"))
  .save()

# COMMAND ----------

streamingCountCositDF = (
  streamingInputDF
    .groupBy(
      streamingInputDF.cosit)
    .count()
    .withColumnRenamed('count', 'total_count')
    .orderBy()
)

# COMMAND ----------

display(streamingCountCositDF)

# COMMAND ----------

streamingCountCositDF.createTempView('count_cosit')

# COMMAND ----------

streamingTop3CositDF = spark.sql('select cosit, total_count from count_cosit order by total_count desc LIMIT 3')


# COMMAND ----------

display(streamingTop3CositDF)

# COMMAND ----------

# Writing the Dataframe to Cassandra 
%scala
//Writing the dataframe directly to cassandra
import org.apache.spark.sql.cassandra._


streamingTop3CositDF.write
  .format("org.apache.spark.sql.cassandra")
  .mode("overwrite")
  .options(Map( "table" -> "streamingTop3CositDF", "keyspace" -> "test_keyspace"))
  .save()

# COMMAND ----------

streamingHGVCountsDF = (
  streamingInputDF
    .where((streamingInputDF.classname == 'HGV_RIG') | (streamingInputDF.classname == 'HGV_ART'))
    .groupBy(
      streamingInputDF.classname)
    .count()
)

# COMMAND ----------

display(streamingHGVCountsDF)

# COMMAND ----------

# Writing the Dataframe to Cassandra 
%scala
//Writing the dataframe directly to cassandra
import org.apache.spark.sql.cassandra._


streamingHGVCountsDF.write
  .format("org.apache.spark.sql.cassandra")
  .mode("overwrite")
  .options(Map( "table" -> "streamingHGVCountsDF", "keyspace" -> "test_keyspace"))
  .save()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


