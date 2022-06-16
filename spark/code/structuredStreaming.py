from distutils.command.config import config
from pyspark.sql import types as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, col, udf
from urlScraper import findAllUrls, ensureProtocol, loadAndParse
from elasticsearch import Elasticsearch
from pyspark.conf import SparkConf
from pyspark import SparkContext

extractUrls = udf(lambda x: ensureProtocol(findAllUrls(x)),
                  st.ArrayType(elementType=st.StringType()))
getTextFromHtml = udf(lambda x: loadAndParse(x))


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


# Create a Spark Session
# spark = SparkSession.builder.appName("warplatforms").getOrCreate()
spark = get_spark_session()

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
    .option("startingOffsets", "latest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .selectExpr("data.*")

df.printSchema()

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

es_mapping = {
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

es = Elasticsearch('http://elasticsearch:9200')

response = es.indices.create(
    index='spark-to-es', ignore=400)

if 'acknowledged' in response:
    if response['acknowledged'] == True:
        print("Successfully created index:", response['index'])

df.printSchema()

df.writeStream \
    .option("checkpointLocation","/tmp/") \
    .format('es') \
    .start('spark-to-es')\
    .awaitTermination()
