from codecs import getreader
from distutils.command.config import config
from doctest import ELLIPSIS_MARKER
from pyspark.sql import types as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, col, udf
from elasticsearch import Elasticsearch
from pyspark.conf import SparkConf
from pyspark import SparkContext
from urlScraper import findAllUrls, ensureProtocol, loadAndParse
from whoIsManager import whoIsManager
from sentimentAnalysis import getPipeline, getTrainingSet

whoIs = whoIsManager()

extractUrls = udf(lambda x: ensureProtocol(findAllUrls(x)),st.ArrayType(elementType=st.StringType()))
getTextFromHtml = udf(lambda x: loadAndParse(x))
udf_whois = udf(lambda x : whoIs.getRelevantFields(x))

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
        st.StructField('@timestamp',    st.StringType(), nullable=True)
    ])

def get_elastic_schema():
    return {
            "mappings": {
                "properties": {
                    "id": {"type": "text"},
                    "channel": {"type": "text"},
                    "url": {"type": "text"},
                    "@timestamp":       {"type": "date", "format": "epoch_second"},
                    "page_text": {"type": "text"}
                    }
                }
            }

# Create a Spark Session
spark = get_spark_session()

schema = get_record_schema()

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

def outputStream(stream,mode=""):
    if mode == "test":
        stream.writeStream \
              .outputMode("append") \
              .format("console") \
              .start()\
              .awaitTermination()
    else:
        es = Elasticsearch('http://elasticsearch:9200')

        response = es.indices.create(index='spark-to-es', ignore=400)

        if 'acknowledged' in response:
            if response['acknowledged'] == True:
                print("Successfully created index:", response['index'])
        
        stream.writeStream \
              .option("checkpointLocation","/tmp/") \
              .format('es') \
              .start('spark-to-es')\
              .awaitTermination()


TOPIC = "telegram-messages"
KAFKASERVER = "kafkaserver:29092"
# KAFKASERVER = "localhost:9092"

spark.sparkContext.setLogLevel("ERROR")

######
training_set =  getTrainingSet()

pipeline = getPipeline(inputCol="page_text",labelCol="positive")

pipelineFit = pipeline.fit(training_set)
######

df = getReader("test").select(from_json(col("value").cast("string"), schema).alias("data")) \
                      .selectExpr("data.*")

df.printSchema()

# Split the list into sentences
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

#add text from html webpage and whois info
df = urls.withColumn('page_text', getTextFromHtml(urls.url))\
         .withColumn('whois', udf_whois(urls.url))

out_df = pipelineFit.transform(df).select('id','channel','@timestamp','page_text','url','whois',col('prediction').alias("mood_prediction"))

out_df.printSchema()

outputStream(out_df,mode="test")
