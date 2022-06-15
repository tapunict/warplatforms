from pyspark.sql import types as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split,from_json,col
from pyspark.sql.functions import lit,to_timestamp,substring, to_date,expr,udf,trim,length
from urlScraper import findAllUrls,ensureProtocol,loadAndParse

extractUrls = udf(lambda x: ensureProtocol(findAllUrls(x)),st.ArrayType(elementType=st.StringType())) 
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
    ])



# Create a Spark Session 

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

schema = get_record_schema()

schema2 = st.StructType([
        st.StructField('id',  st.StringType(), nullable=True),
        st.StructField('channel',  st.StringType(), nullable=True),
        st.StructField('url',   st.StringType(), nullable=True),
    ])

# Create DataFrame representing the stream of input lines from connection to tapnc:9999
df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .option("startingOffsets", "earliest") \
    .load()\
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .selectExpr("data.*")

df.printSchema()
# sentences = df.select(data.id,data.channel,explode(data.text).alias('sentence')) 

# Split the lines into words
sentences = df.select(
    col('id'),
    col('channel'),
   explode(
       df.text
   ).alias("sentence")
)
sentences.printSchema()

#extractUrls(col('sentence'))
urls = sentences.select(
    col('id'),
    col('channel'),
    explode(
        extractUrls(col('sentence'))
    ).alias('url')
)

df = urls.withColumn('page_text',getTextFromHtml(urls.url))

words = df.select(
        col('id'),
        col('channel'),
        explode(
            split(col('page_text'), " ")
        ).alias("word")
    ).where(length(col("word")) >= 5)

#split(lines.text, " ")
# Generate running word count
wordCounts = words.groupBy("id","channel","word")\
                .count()

#.applyInPandas(normalize, schema="id string, channel string, url string")\


 # Start running the query that prints the running counts to the console
wordCounts \
    .orderBy(col("count").desc()) \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()\
    .awaitTermination()

#.applyInPandas(predict, get_resulting_df_schema())
