from codecs import getreader
from distutils.command.config import config
from doctest import ELLIPSIS_MARKER
from pyspark.sql import types as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, col, udf,split
from elasticsearch import Elasticsearch
from pyspark.conf import SparkConf
from pyspark import SparkContext

# our modules
from urlScraper import findAllUrls, ensureProtocol, loadAndParse
from whoIsManager import whoIsManager
from sentimentAnalysis import getModel, saveModel, cleanText
from geocoding import findCitiesInText, getLocationAsString

whoIs = whoIsManager()

extractUrls = udf(lambda x: ensureProtocol(findAllUrls(x)),
                  st.ArrayType(elementType=st.StringType()))
getTextFromHtml = udf(loadAndParse)
udf_cleanText = udf(cleanText)
udf_whois = udf(whoIs.getRelevantFields)
equivalent_emotion = udf(lambda x: "positive" if x == 1.0 else "negative")
ukrainian_cities = udf(findCitiesInText)
city_location = udf(getLocationAsString)


def get_spark_session():
    spark_conf = SparkConf()\
        .set('es.nodes', 'elasticsearch')\
        .set('es.port', '9200')
    sc = SparkContext(appName='spark-to-es', conf=spark_conf)
    return SparkSession(sc)


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
        st.StructField('translation',      st.StringType(), nullable=True),
        st.StructField('@timestamp',    st.StringType(), nullable=True)
    ])


def get_tr_schema():
    return st.StructType([
        st.StructField('service',  st.StringType(), nullable=True),
        st.StructField('text',   st.StringType(), nullable=True),
    ])


def get_elastic_schema():
    return {
        "mappings": {
            "properties": {
                "id": {"type": "text"},
                "channel": {"type": "text"},
                "url": {"type": "text"},
                "@timestamp":       {"type": "date", "format": "epoch_second"},
                "content": {"type": "text"}
            }
        }
    }


# Create a Spark Session
spark = get_spark_session()

spark.sparkContext.setLogLevel("ERROR")

schema = get_record_schema()

tr_schema = get_tr_schema()

es_mapping = get_elastic_schema()


def getReader(mode=""):
    if mode == "test":
        return spark.readStream \
                    .format("socket") \
                    .option("host", "localhost") \
                    .option("port", 9997) \
                    .option("startingOffsets", "latest") \
                    .load()
    return spark.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', KAFKASERVER) \
                .option('subscribe', TOPIC) \
                .option("startingOffsets", "latest") \
                .load()


def outputStream(stream, mode="", index=""):
    if mode == "test":
        return stream.writeStream \
                     .outputMode("append") \
                     .format("console") \
                     .start()
    else:
        es = Elasticsearch('http://elasticsearch:9200')

        response = es.indices.create(index=index, ignore=400)

        if 'acknowledged' in response:
            if response['acknowledged'] == True:
                print("Successfully created index:", response['index'])

        return stream.writeStream \
                     .option("checkpointLocation", "/tmp/") \
                     .format('es') \
                     .start(index)


TOPIC = "telegram-messages"
KAFKASERVER = "kafkaserver:29092"
# KAFKASERVER = "localhost:9092"

# Pipeline emotion_detection
pipelineFit = getModel(task="emotion_detection",
                       inputCol="content", labelCol="label")
saveModel(pipelineFit, task="emotion_detection")


df = getReader().select(from_json(col("value").cast("string"), schema).alias("data")) \
    .selectExpr("data.*")

df.printSchema()

# Split the list into sentences
sentences = df.select(
    col('id'),
    col('channel'),
    col('@timestamp'),
    col('data'),
    explode(
        df.text
    ).alias("sentence")
)
sentences.printSchema()

message_analysis = df.select(
    col('id'),
    col('channel'),
    col('@timestamp').alias("timestamp"),
    from_json(col('translation').cast("string"), tr_schema).alias("tr"),
).selectExpr("id", "channel", "timestamp", "tr.*")\
    .select("id", "channel", "timestamp", udf_cleanText("text").alias("content"))

# Extract Urls from sentences
urls = sentences.select(
    col('id'),
    col('channel'),
    col('@timestamp'),
    col('data'),
    explode(
        extractUrls(col('sentence'))
    ).alias('url')
)

# add text from html webpage and whois info
df = urls.withColumn('content', udf_cleanText(getTextFromHtml(urls.url)))\
         .withColumn('whois', udf_whois(urls.url))

# Sentiment Analysis: emotion_detection
out_df = pipelineFit.transform(df)\
                    .withColumn('emotion_detection', equivalent_emotion(col('prediction')))\
                    .select('id',
                            'channel',
                            '@timestamp',
                            'content',
                            'url',
                            'whois',
                            "emotion_detection")

message_analysis = pipelineFit.transform(message_analysis)\
    .withColumn('emotion_detection', equivalent_emotion(col('prediction')))\
    .select('id',
            'channel',
            'timestamp',
            col('content').alias("traduction"),
            "emotion_detection")\
    .withColumn('city', explode(ukrainian_cities(split(col("traduction"),",")))) \
    .withColumn('location', city_location(col('city')))  # esclusivamente città ucraine

# col('prediction')
out_df.printSchema()

out_stream = outputStream(out_df, index='spark-to-es')

out_stream2 = outputStream(message_analysis, index='message-analysis')


out_stream.awaitTermination()
out_stream2.awaitTermination()
