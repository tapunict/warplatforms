from pyspark.sql import types as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, unix_timestamp
from pyspark.sql.functions import split, from_json, col
from pyspark.sql.functions import lit, to_timestamp, substring, to_date, expr, udf, trim, length
from pyspark.sql.functions import max as sparkMax
from urlScraper import findAllUrls, ensureProtocol, loadAndParse

extractUrls = udf(lambda x: ensureProtocol(findAllUrls(x)),
                  st.ArrayType(elementType=st.StringType()))
getTextFromHtml = udf(lambda x: loadAndParse(x))


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
        st.StructField('@timestamp',    st.StringType(), nullable=True)
    ])


# Create a Spark Session
spark = SparkSession \
    .builder \
    .appName("warplatforms") \
    .getOrCreate()

schema = get_record_schema()

schema2 = st.StructType([
    st.StructField('id',  st.StringType(), nullable=True),
    st.StructField('channel',  st.StringType(), nullable=True),
    st.StructField('url',   st.StringType(), nullable=True),
])

TOPIC = "telegram-messages"
KAFKASERVER = "kafkaserver:29092"
# KAFKASERVER = "localhost:9092"

spark.sparkContext.setLogLevel("ERROR")

# Create DataFrame representing the stream of input lines from connection to tapnc:9999
df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKASERVER) \
    .option('subscribe', TOPIC) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .selectExpr("data.*")

df.printSchema()
# sentences = df.select(data.id,data.channel,explode(data.text).alias('sentence'))

# Split the lines into words
sentences = df.select(
    col('id'),
    col('channel'),
    col('@timestamp'),
    explode(
        df.text
    ).alias("sentence")
)
sentences.printSchema()

# extractUrls(col('sentence'))
urls = sentences.select(
    col('id'),
    col('channel'),
    col('@timestamp'),
    explode(
        extractUrls(col('sentence'))
    ).alias('url')
)

df = urls.withColumn('page_text', getTextFromHtml(urls.url))

words = df.select(
    col('id'),
    col('channel'),
    col('@timestamp'),
    explode(
        split(col('page_text'), " ")
    ).alias("word")
).where(length(col("word")) >= 5)

#split(lines.text, " ")
# Generate running word count
wordCounts = words.groupBy("id", "channel", "word", "@timestamp")\
    .count()

# .applyInPandas(normalize, schema="id string, channel string, url string")\

# Start running the query that prints the running counts to the console
wordCounts \
    .withColumn('timestamp', unix_timestamp(col('@timestamp'), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(st.TimestampType())) \
    .withWatermark("timestamp", "1 minutes") \
    .orderBy('timestamp', ascending=False)\
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()\
    .awaitTermination()


#    .orderBy(col("count").desc()) \

#    .groupBy(col("id")) \ da

# .applyInPandas(predict, get_resulting_df_schema())
# .agg(max(col('timestamp')).alias("timestamp")) \
