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
from pyspark.ml import PipelineModel
import pandas as pd
from bs4 import BeautifulSoup
import re


schema = tp.StructType([
    tp.StructField(name= 'id', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'subjective',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'positive',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'negative',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'ironic',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'lpositive',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'lnegative',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'top',       dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'page_text',       dataType= tp.StringType(),   nullable= True)
])

def get_spark_session():
    spark_conf = SparkConf()\
        .set('es.nodes', 'elasticsearch')\
        .set('es.port', '9200')
    sc = SparkContext(appName='spark-to-es', conf=spark_conf)
    return SparkSession(sc)



def getPipeline(inputCol="input",labelCol="label"):

    # tokenizer = Tokenizer(inputCol=inputCol, outputCol="words")
    # hashtf = HashingTF(numFeatures=2**16, inputCol="words", outputCol='tf')
    # idf = IDF(inputCol='tf', outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
    # label_stringIdx = StringIndexer(inputCol = "target", outputCol = labelCol)
    # pipelineFit = pipeline.fit(train_set)
    # return Pipeline(stages=[tokenizer, hashtf, idf, label_stringIdx])

    stage_1 = RegexTokenizer(inputCol= inputCol , outputCol= 'tokens', pattern= '\\W')
    stage_2 = StopWordsRemover(inputCol= 'tokens', outputCol= 'filtered_words')
    stage_3 = Word2Vec(inputCol= 'filtered_words', outputCol= 'vector', vectorSize= 100)
    model = LogisticRegression(featuresCol= 'vector', labelCol= labelCol)#'positive'
    return Pipeline(stages= [stage_1, stage_2, stage_3, model])

def prepareSets(df,split=[1.0,0.0,0.0]):
    (train_set, val_set, test_set) = df.randomSplit(split, seed = 2000)#[0.98, 0.01, 0.01]
    return (train_set, val_set, test_set)

def cleanDataset(df):
    print(f"before na:{df.count()}")
    df = df.dropna()
    print(f"after na:{df.count()}")
    return df

def getTrainingSet(task="emotion_detection",split=[1.0,0.0,0.0]):
    print("Getting training set...")
    spark = SparkSession.builder.appName("Sentiment Analysis").getOrCreate()
    dataset = {}
    if task == "emotion_detection":
        dataset = spark.read.format('com.databricks.spark.csv') \
                    .options(header='true', inferschema='true') \
                    .load('cleaned_tweets_sub_0.05.csv')
    elif task == "emotion_detection_italian":
        dataset = spark.read.csv('./prova_training_set.csv',
                                schema=schema,
                                header=True,
                                sep=',')
    print("Getting training set done")
    return prepareSets(df=cleanDataset(dataset), split=split)

def trainModel(task="emotion_detection",inputCol="input",labelCol="label",split=[1.0,0.0,0.0]):
    print("Training model...")
    (train_set, val_set, test_set) =  getTrainingSet(task=task,split=split)
    pipeline = getPipeline(inputCol=inputCol,labelCol=labelCol)
    model = pipeline.fit(train_set)
    if split[2] > 0.0:
        test_predictions = model.transform(test_set)
        correct_prediction = test_predictions.filter(test_predictions.label == test_predictions.prediction).count()
        test_accuracy = correct_prediction / float(test_set.count())
        print(f"Model accuracy:{test_accuracy}")
    print("Training model done")
    return model

#redo serve per forzare il training e non usare un modello pretrainato
def getModel(task="emotion_detection",inputCol="input",labelCol="label",redo=False,split=[1.0,0.0,0.0]):
    if redo:
        return trainModel(task=task,inputCol=inputCol,labelCol=labelCol,split=split)
    try:
        return PipelineModel.load(f'models/model_{task}.save')
    except Exception as e: 
        print(f"(getModel, taks: {task}): {e}")
        return trainModel(task=task,inputCol=inputCol,labelCol=labelCol,split=split)

def saveModel(model,task="emotion_detection",overwrite=False):
    try:
        if overwrite:
             model.write().overwrite().save(f"./models/model_{task}.save")
        else:
            model.save(f"./models/model_{task}.save")
        return True
    except Exception as e: 
            print(f"(saveModel, taks: {task}): {e}")
            return False

def main():
    spark = SparkSession.builder.appName("Sentiment Analysis").getOrCreate()

    # training_set =  getTrainingSet()
    # training_set.show(truncate=False)
    # pipeline = getPipeline(inputCol="page_text",labelCol="positive")
    # pipelineFit = pipeline.fit(training_set)
    #task = input("CHE TASK VUOI PROVARE? ")
    
    print("Getting model")
    pipelineFit = getModel(inputCol="content",labelCol="label")
    saveModel(pipelineFit)

    while True:
        text = input("#>")
        if text=="exit": break
        tweetDf = spark.createDataFrame([text], tp.StringType()).toDF("content")
        tweetDf.show(truncate=False)
        pipelineFit.transform(tweetDf).select('tokens','prediction').show(truncate=False)

pat1 = r'@[A-Za-z0-9]+'
pat2 = r'https?://[A-Za-z0-9./]+'
combined_pat = r'|'.join((pat1, pat2))

def cleanText(text):
    soup = BeautifulSoup(text, 'html.parser')
    souped = soup.get_text()
    stripped = re.sub(combined_pat, '', souped)
    try:
        clean = stripped.decode("utf-8-sig").replace(u"\ufffd", "?")
    except:
        clean = stripped
    letters_only = re.sub("[^a-zA-Z]", " ", clean)
    lower_case = letters_only.lower()

    correct_spaces = re.sub("[ ]+", " ", lower_case)#SUPPONGO FUNZIONI
    return correct_spaces



def cleaningCSV():
    cols = ['sentiment','id','date','query_string','user','content']
    df = pd.read_csv("./training.1600000.processed.noemoticon.csv",header=None, names=cols, engine ='python')

    df.drop(['id','date','query_string','user'],axis=1,inplace=True)

    print(f"Sto per pulire {df.count()}")

    nums = [0,400000,800000,1200000,1600000]
    print("Cleaning and parsing the tweets...\n")
    clean_tweet_texts = []
    for group in range(1,len(nums)):
        print(f"Righe da {nums[group-1]} a {nums[group]}")
        for i in range(nums[group-1],nums[group]):
            if( (i+1)%10000 == 0 ):
                print(f"Tweet {i+1} di {nums[group]} è stato pulito")                                                                
            clean_tweet_texts.append(cleanText(df['content'][i]))

    clean_df = pd.DataFrame(clean_tweet_texts,columns=['content'])
    clean_df['label'] = [1 if t==4 else 0 for t in df.sentiment]
    clean_df.to_csv('cleaned_tweets.csv',encoding='utf-8')

# def modifyLabel():
#     df = pd.read_csv("./clean_tweet.csv",engine ='python')
#     clean_df = pd.DataFrame([1 if t==4 else 0 for t in df.label],columns=['label'])
#     clean_df['content'] = df.content
#     clean_df.to_csv('cleaned_tweets.csv',encoding='utf-8')

def makeSubset(fraction,dataset,new_name,fields):
    df = pd.read_csv(dataset,engine ='python')
    df = df.sample(frac = fraction)[fields]
    df.to_csv(new_name,encoding = 'utf-8')

def prova():
    fract = 0.05
    fields = ["content","label"]
    makeSubset(fraction=fract,dataset='cleaned_tweets.csv',new_name=f'cleaned_tweets_sub_{fract}.csv',fields=fields)
if __name__ == '__main__':
    # prova()
    main()