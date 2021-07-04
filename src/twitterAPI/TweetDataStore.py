from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import length

import connection_config

STORE_DATA = True


def main():
    spark = SparkSession.builder.appName("SocialMediaInsights").getOrCreate()

    stream_data = (
        spark.readStream.format("socket")
        .option("host", "127.0.0.1")
        .option("port", connection_config.PORT)
        .load()
    )

    split_col = split(
        stream_data.value,
        "\t",
    )

    # Creating the Dataframe Schema
    tweets_df = (
        stream_data.withColumn("User_ID", split_col.getItem(0))
        .withColumn("Date", split_col.getItem(1))
        .withColumn("Tweet", split_col.getItem(2))
        .drop("value")
    )

    tweets_df = tweets_df.withColumn("Length", length(tweets_df["Tweet"]))

    # Creates the CSV files if data must be stored, print to console otherwise
    if STORE_DATA:
        query = (
            tweets_df.writeStream.format("csv")  # csv or parquet
            .outputMode("append")  # streaming data can only be "append"
            .option(
                "path", connection_config.RAW_DATA_PATH
            )  # path where data will be saved
            .trigger(
                processingTime="20 seconds"
            )  # Frequency of the data Stored to the Sink
            .option(
                "checkpointLocation", connection_config.CHECKPOINT_PATH
            )  # write-ahead logs for recovery purposes
            .start()
        )
    else:
        query = (
            tweets_df.writeStream.outputMode("append")
            .format("console")
            .trigger(
                processingTime="20 seconds"
            )  # Frequency when data displayed on the console
            .option("truncate", True)
            .start()
        )

    query.awaitTermination()


if __name__ == "__main__":
    main()
