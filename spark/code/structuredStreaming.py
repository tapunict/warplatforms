from pyspark.sql import types as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split,from_json,col
from pyspark.sql.functions import lit,to_timestamp,substring, to_date,expr,udf,trim,length
from pyspark.sql import Row
from urlScraper import findAllUrls,ensureProtocol,loadAndParse
from whoIsManager import whoIsManager

extractUrls = udf(lambda x: ensureProtocol(findAllUrls(x)),st.ArrayType(elementType=st.StringType())) 
getTextFromHtml = udf(lambda x: loadAndParse(x))
#Crea un'istanza di whoIsManager

# Crea una Spark Session 
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

whoIs = whoIsManager()

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


def whoIsEnrichment(microBatchDF ,microBatchId): 
    try:
        text_list = []
        # row_list = [str(row.url) for row in urls.collect()]
        row_list = microBatchDF.collect()
        for row in row_list:
            url = str(row.url)
            print(url)
            text_list.append(Row(**row.asDict(),
                                 whois_server=whoIs.udf_whois_server(url),
                                 registrar = whoIs.udf_registrar(url),
                                 org = whoIs.udf_org(url),
                                 domain_name = whoIs.udf_domain_name(url),
                                 creation_date = whoIs.udf_creation_date(url),
                                 expiration_date = whoIs.udf_expiration_date(url),
                                 state = whoIs.udf_state(url),
                                 country = whoIs.udf_country(url),
                                 ))#name=u"john" 

        whois_df = spark.createDataFrame(text_list)
        whois_df\
        .write\
        .format("console")\
        .option("checkpointLocation","checkPoint")\
        .save()
    except Exception as e: 
        print(f"(loadAndParse): {e}")
    
    

def main():    

    #Visualizzo lo schema
    schema = get_record_schema()


    # Create DataFrame representing the stream of input lines from connection to tapnc:9999
    df = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9997) \
        .option("startingOffsets", "earliest") \
        .load()\
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .selectExpr("data.*")

    df.printSchema()

    #ottengo una row per ogni frase
    sentences = df.select(
        col('id'),
        col('channel'),
        col('data'),
    explode(
        df.text
    ).alias("sentence")
    )
    sentences.printSchema()

    #estraggo gli url
    urls = sentences.select(
        col('id'),
        col('channel'),
        col('data'),
        explode(
            extractUrls(col('sentence'))
        ).alias('url')
    )

    #ottengo l'html dagli url
    page_df = urls.withColumn('page_text',getTextFromHtml(urls.url))

    #info su whois

    page_df \
        .writeStream \
        .foreachBatch(whoIsEnrichment) \
        .outputMode("append") \
        .format("console") \
        .start()\
        .awaitTermination()


#       .outputMode("append") \
#       .format("console") \

# .foreachBatch(whoIsEnrichment) \



if __name__ == '__main__':
    main()








#    .orderBy(col("count").desc()) \
#.applyInPandas(predict, get_resulting_df_schema())

# words = df.select(
#         col('id'),
#         col('channel'),
#         explode(
#             split(col('page_text'), " ")
#         ).alias("word")
#     ).where(length(col("word")) >= 5)


# wordCounts = words.groupBy("id","channel","word")\
#                 .count()