import pyspark
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession

schema = tp.StructType([
    tp.StructField(name= 'id', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'subjective',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'positive',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'negative',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'ironic',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'lpositive',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'lnegative',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'top',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'text',       dataType= tp.StringType(),   nullable= True)
])

def get_spark_session():
    spark_conf = SparkConf()\
        .set('es.nodes', 'elasticsearch')\
        .set('es.port', '9200')
    sc = SparkContext(appName='spark-to-es', conf=spark_conf)
    return SparkSession(sc)


def getPipeline(inputCol="input",labelCol="label"):
    # define stage 1: tokenize the tweet text    
    stage_1 = RegexTokenizer(inputCol= inputCol , outputCol= 'tokens', pattern= '\\W')

    # define stage 2: remove the stop words
    stage_2 = StopWordsRemover(inputCol= 'tokens', outputCol= 'filtered_words')

    # define stage 3: create a word vector of the size 100
    stage_3 = Word2Vec(inputCol= 'filtered_words', outputCol= 'vector', vectorSize= 100)

    # define stage 4: Logistic Regression Model
    model = LogisticRegression(featuresCol= 'vector', labelCol= labelCol)#'positive'

    return Pipeline(stages= [stage_1, stage_2, stage_3, model])

def getTrainingSet():
    spark = SparkSession.builder.appName("Sentiment Analysis").getOrCreate()
    return spark.read.csv('./prova_training_set.csv',
                            schema=schema,
                            header=True,
                            sep=',')
# deg getTrainingSet():


def main():
    spark = SparkSession.builder.appName("Sentiment Analysis").getOrCreate()
    print(spark)
    print(type(spark))


    training_set =  getTrainingSet()
    training_set.show(truncate=False)


    # training_set.groupBy("positive").count().show()

    pipeline = getPipeline(inputCol="page_text",labelCol="positive")


    pipelineFit = pipeline.fit(training_set)



    # try:
    #     pipelineFit.save("./dataset/model.save")
    # except Exception as e: 
    #         print(f"(pipelineFit.save): {e}")

if __name__ == '__main__':
    main()