from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import time
import datetime
from pyspark.sql.functions import expr
from pyspark.sql.functions import window
from pyspark.sql.functions import explode
from pyspark.sql.functions import split



if __name__ == "__main__":
    print ("HELOOOOOOO");

    
    # Set your local host to be the master node of your cluster
    # Set the appName for your Spark session
    # Join session for app if it exists, else create a new one
    sparkSession = SparkSession.builder.master("local")\
                              .appName("SparkStructuredStreamingSlidingWindowTest")\
                              .getOrCreate()


    # ERROR log level will generate fewer lines of output compared to INFO and DEBUG                          
    sparkSession.sparkContext.setLogLevel("ERROR")


    # InferSchema not yet available in spark structured streaming 
    # (it is available in static dataframes)
    # We explicity state the schema of the input data
    schema = StructType([StructField("timestamp", StringType(), False),\
                         StructField("key", StringType(), False),\
                         StructField("value", StringType(), False)])


    # Read stream into a dataframe  # Since the csv data includes a header row, we specify that here# We state the schema to use and the location of the csv files# maxFilesPerTrigger sets the number of new files to be considered in each trigger
    fileStreamDF = sparkSession.readStream\
                               .option("header", "false")\
							   .schema(schema)\
                               .csv("./data")
                               #.option("maxFilesPerTrigger", 1)\
							   
                               
    # The User Defined Function (UDF)
    # Create a timestamp from the current time and return it
    def convert_timestamp(ts):
        ts_int = int(ts)
        #timestamp = datetime.datetime.utcfromtimestamp(ts_int/1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        timestamp = datetime.datetime.utcfromtimestamp(ts_int/1000)
        print(timestamp)
        return timestamp

    def show_time_only(ts):
        ts_str = str(ts)
        return ts_str[10:]
    # Register the UDF
    # Set the return type to be a String
    # A name is assigned to the registered function 
    convert_timestamp_udf = udf(convert_timestamp, TimestampType())
    show_time_only_udf = udf(show_time_only, StringType())

    # Create a new column called "timestamp" in fileStreamDF
    # Apply the UDF to every row in fileStreamDF - assign its return value to timestamp column
	# Unused
    fileStreamWithTS = fileStreamDF.withColumn("timestamp2", convert_timestamp_udf(fileStreamDF.timestamp))


    # window(timeColumn, windowDuration, slideDuration=None, startTime=None)
    # timeColumn gives the time field to use when creating a window
    # windowDuration gives the length of the window
    # slideDuration is the gap between each window (Windows can overlap)
    # slideDuration must be <= windowDuration
    # The #convictions for a particular window will likely increase with each batch of files processed - 
    # this is because more timestamps within that window will be encountered in the new batch
	#query = splittedResult.writeStream.outputMode("update").option("numRows","1000").format("console").trigger(processingTime='5 milliseconds').start().awaitTermination()
#=========================================================================================================================	
	## ProcessingTime trigger with five-milliseconds micro-batch interval
    # windowedCounts = fileStreamWithTS.withWatermark("timestamp2", "2 milliseconds")\
                                      # .groupBy(
                                        # window("timestamp2", #window(fileStreamWithTS.timestamp2
                                                # "10 milliseconds", 
                                                # "10 milliseconds"),"key")\
                                      # .agg({"value": "count"})\
                                      # .withColumnRenamed("count(value)", "cnt")
    # splittedResult = windowedCounts.select(show_time_only_udf("window.start"),show_time_only_udf("window.end"),"cnt").withColumnRenamed("show_time_only(window.start)","start")#.orderBy("start",ascending=True)
    # query = splittedResult.writeStream.outputMode("update").option("numRows","1000").format("console").trigger(processingTime='5 milliseconds').start().awaitTermination()
#=========================================================================================================================	
	## Default trigger (runs micro-batch as soon as it can) and Tumbling window 
    # windowedCounts = fileStreamWithTS.withWatermark("timestamp2", "2 milliseconds")\
                                      # .groupBy(
                                        # window("timestamp2", #window(fileStreamWithTS.timestamp2
                                                # "10 milliseconds", 
                                                # "10 milliseconds"),"key")\
                                      # .agg({"value": "count"})\
                                      # .withColumnRenamed("count(value)", "cnt")
    # splittedResult = windowedCounts.select(show_time_only_udf("window.start"),show_time_only_udf("window.end"),"cnt").withColumnRenamed("show_time_only(window.start)","start")#.orderBy("start",ascending=True)
    # query = splittedResult.writeStream.outputMode("update").option("numRows","1000").format("console").start().awaitTermination()

	
#=========================================================================================================================
	# ## one-time trigger and Tumbling window 
    # windowedCounts = fileStreamWithTS.withWatermark("timestamp2", "2 second")\
                                      # .groupBy(
                                        # window("timestamp2", #window(fileStreamWithTS.timestamp2
                                                # "10 second", 
                                                # "10 second"),"key")\
                                      # .agg({"value": "count"})\
                                      # .withColumnRenamed("count(value)", "cnt")
    # splittedResult = windowedCounts.select(show_time_only_udf("window.start"),show_time_only_udf("window.end"),"cnt").withColumnRenamed("show_time_only(window.start)","start")#.orderBy("start",ascending=True)
    # query = splittedResult.writeStream.outputMode("update").option("numRows","1000").format("console").trigger(once=True).start().awaitTermination()

#===============================triggering without windowing==========================================================================================   
   # ## Default trigger (runs micro-batch as soon as it can) and Tumbling window 
    # windowedCounts = fileStreamWithTS.withWatermark("timestamp2", "1 milliseconds")\
                                      # .groupBy("key")\
                                      # .agg({"value": "count"})\
                                      # .withColumnRenamed("count(value)", "cnt")
    # query = windowedCounts.writeStream.outputMode("update").option("numRows","1000").format("console").trigger(processingTime='1 milliseconds').start().awaitTermination()

# #=============================Mis Test=====================================================================================
 
    windowedCounts = fileStreamWithTS.withWatermark("timestamp2", "2 second")\
                                      .groupBy(
                                        window("timestamp2", #window(fileStreamWithTS.timestamp2
                                                "10 second", 
                                                "10 second"),"timestamp")\
                                      .agg({"timestamp": "count"})\
                                      .withColumnRenamed("count(timestamp)", "cnt")
    splittedResult = windowedCounts.select(show_time_only_udf("window.start"),show_time_only_udf("window.end"),"cnt").withColumnRenamed("show_time_only(window.start)","start")#.orderBy("start",ascending=True)
    query = splittedResult.writeStream.outputMode("update").option("numRows","1000").format("console").trigger(once=True).start().awaitTermination()
