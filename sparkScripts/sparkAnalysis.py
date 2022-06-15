from pyspark.sql import SparkSession
from pyspark.sql import types as st
from pyspark.sql import Row
from pyspark.sql.functions import col, lit,to_timestamp,substring, to_date,explode,split,expr,udf,trim,length
import datetime
import pandas as pd
# from wordcount import wordcount
from urlScraper import findAllUrls,ensureProtocol,loadAndParse

JSON_FILE = 'data/messaggi.json'

extractUrls = udf(lambda x: ensureProtocol(findAllUrls(x)),st.ArrayType(elementType=st.StringType())) 
getTextFromHtml = udf(lambda x: loadAndParse(x))

#STRUTTURA DEL MESSAGGIO
schema = st.StructType([
        st.StructField('id',  st.StringType(),nullable=True), #st.IntegerType()
        st.StructField('data',   st.StringType(),nullable=True), #st.DataType()
        st.StructField('channel',  st.StringType(),nullable=True), 
        st.StructField('authorName',   st.StringType(),nullable=True),
        st.StructField('authorLink',   st.StringType(),nullable=True),  
        st.StructField('images',      st.ArrayType(elementType=st.StringType()),nullable=True),
        st.StructField('text',  st.ArrayType(elementType=st.StringType()),nullable=True), 
        st.StructField('videos',   st.ArrayType(elementType=st.StringType()),nullable=True), 
        st.StructField('views',      st.StringType(),nullable=True), 
    ])

stopOnEnd = False

def printInfo(df):
    #mostra la tabella
    df.show() #show(truncate=False)
    #mostra la struttura
    df.printSchema()

def convertTelegramDatetime(dataframe,date_field,new_field,dropOld=True):
    #to_timestamp(col(date_field),"yyyy-MM-dd'T'HH:mm:ss")
    new_df = dataframe.withColumn(new_field,  to_timestamp(substring(date_field, 1,19),"yyyy-MM-dd'T'HH:mm:ss"))
    if dropOld:
        new_df = new_df.drop(date_field)
    return new_df

#conta i messaggi per giorno
def contaMessaggiPerGiorno(df):
    return df.select(to_date('datetime').alias("date"),'id').groupBy('date').count().show(truncate=False)

def main():
    
    spark = SparkSession.builder\
        .master("local")\
        .appName('Spark - Telegram Messages Analysis')\
        .config("spark.files.overwrite", "true") \
        .getOrCreate()

    #spark.conf.set("spark.sql.shuffle.partitions", 1000)
    
    data = spark.read.json(JSON_FILE, schema=schema)

    #ottengo un dataframe con una row per ogni frase (dei messaggi presi da kafka)
    sentences = data.select(data.id,data.channel,explode(data.text).alias('sentence')) 
    sentences.show()

    #estrae gli url
    urls = sentences.select(data.id,data.channel,explode(extractUrls(col('sentence'))).alias('url'))
    urls.show(truncate=False)


    # #bisogna sequenzializzare per evitare che troppe richieste http vadano in parallelo
    # text_list = []
    # # row_list = [str(row.url) for row in urls.collect()]
    # row_list = urls.collect()
    # for row in row_list:
    #     print(row)
    #     url = str(row.url)
    #     text_list.append(Row(**row.asDict(), page_text=loadAndParse(url)))#name=u"john" 

    # df = spark.createDataFrame(text_list)
    # df.show()
    
    df = urls.withColumn('page_text',getTextFromHtml(urls.url))
    df.show()
    #ottengo l'html dagli url
    
    words = df.select(df.id,df.channel,
        explode(
            split(df.page_text, " ")
        ).alias("word")
    ).where(length(col("word")) >= 5)
    words.show()

    wordCounts = words.groupBy("id","channel","word").count()
    wordCounts.orderBy(col("count").desc()).show()
    # wordCounts.show()





    #trova le parole
    # words = sentences.select(
    #         explode(
    #             split(sentences.sentence, " ")
    #         ).alias("word")
    #     )
    # words.show()

    # print(f"\nDato: {data}\n")

    # print(f"\Tipo: {type(data)}\n")

    # print(f"Numero di righe {data.count()}")

    # new_df = convertTelegramDatetime(data,'data','datetime')

    #filtra le righe
    # new_df.filter( (col('datetime') >= lit('2020-04-11')) & (col('datetime') <= lit('2020-04-12 09:10:53')) ).show()

    #seleziona soltanto alcune colonne
    # new_df.select(['id','channel','text']).show(5)


    # while True:
    #     print("Inserire l'URL")
    #     url = input("#>")
    #     wc = wordcount(spark, url)
    #     print(f"TIPO:::: {type(wc)}")
    #     top10 = wc.sortBy(lambda x: x[1], ascending=False).take(10)
    #     for wc in top10: print(wc)



    # if stopOnEnd:
    #     spark.stop()


def main2():
    print(findAllUrls("ciao www.esempio.it https://c.riusciremo.com"))
    return False


if __name__ == '__main__': main()
# if __name__ == '__main__': main2()