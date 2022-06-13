from pyspark.sql import types as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col


def get_record_schema():
    return st.StructType([
        st.StructField('id',  st.StringType(), nullable=True),
        st.StructField('data',   st.StringType(), nullable=True),
        st.StructField('channel',  st.StringType(), nullable=True),
        st.StructField('authorName',   st.StringType(), nullable=True),
        st.StructField('authorLink',   st.StringType(), nullable=True),
        st.StructField('images',      st.ArrayType(
            elementType=st.StringType()), nullable=True),
        st.StructField('text',  st.ArrayType(
            elementType=st.StringType()), nullable=True),
        st.StructField('videos',   st.ArrayType(
            elementType=st.StringType()), nullable=True),
        st.StructField('views',      st.StringType(), nullable=True),
    ])


def main():
    spark = SparkSession.builder \
        .appName("warplatforms") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    TOPIC = "telegram-messages"
    KAFKASERVER = "kafkaserver:29092"
    # KAFKASERVER = "localhost:9092"

    schema = get_record_schema()

    df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', KAFKASERVER) \
        .option('subscribe', TOPIC) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .selectExpr("data.*")

    df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    main()
