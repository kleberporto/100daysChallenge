from pyspark.sql import SparkSession
from pyspark.sql.functions import split
import connection_config

spark = SparkSession \
    .builder \
    .appName("SocialMediaInsights") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
stream_data = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", connection_config.PORT) \
    .load()

split_col = split(stream_data.value, '\t',)
tweets_df = stream_data.withColumn('User', split_col.getItem(0)) \
    .withColumn('Date', split_col.getItem(1)) \
    .withColumn('Tweet', split_col.getItem(2)) \
    .drop('value')

query = tweets_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option('truncate', False) \
    .start()

query.awaitTermination()
