# -*- coding: utf-8 -*-
"""
Created on Tue Apr  3 18:21:38 2018

@author: CS
"""

import os  
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

#    Spark
from pyspark import SparkContext  
#    Spark Streaming
from pyspark.streaming import StreamingContext  
#    Kafka
from pyspark.streaming.kafka import KafkaUtils  
#    json parsing

import json
import time

import os

timestr = time.strftime("(%Y-%m-%d_%H-%M-%S)")
date_time = time.strftime("(%Y-%m-%d)")

file_path = os.getcwd()
directory = os.path.dirname(file_path)

if not os.path.exists(directory):
    os.makedirs(directory)

numDimensions = 300
maxSeqLength = 70
batchSize = 24
lstmUnits = 128
numClasses = 2
iterations = 100000


import numpy as np
import pickle
from DataPreprocessing import cleanSentences
from DataPreprocessing import _lookup_words
from DataPreprocessing import stemmer

import time
from nlp import predictSentiment
from nlp import entity_analysis

with open('dictionary.pickle', 'rb') as handle:
    wordsList = pickle.load(handle)
wordVectors = np.load('final_embeddings.npy')
        
def collectElementJSON(data):
    elements = []
    data["sentiment"], data["total_score"] = predictSentiment(cleanSentences(_lookup_words(stemmer.stem(data["text"]))))
    data["person"], data["company"], data["eventNegatif"], data["country"], data["city"], data["lat_city"], data["long_city"] = entity_analysis(data["text"])
    for key, value in data.items():
        elements.append(str(value))
    return data

sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")  
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 2)
  
#kafkaStream = KafkaUtils.createStream(ssc, 'NLP:2181', 'spark-streaming', {'weblogs':1})
kafkaStream = KafkaUtils.createDirectStream(ssc, topics = ['weblogs'], kafkaParams = {"metadata.broker.list": "NLP:9092"})
# Here to parse the inbound messages isn't valid JSON
parsed = kafkaStream.map(lambda v: json.loads(v[1]))
parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
#parsed.saveAsTextFiles("file:///D:/spark-kafka.txt")

rdd_tot = parsed.map(lambda data: collectElementJSON(data))
rdd_tot.saveAsTextFiles("file:///D:/PROJECT_MABESPOLRI/progress_spark_sentiment/{}/result_{}.json".format(date_time,timestr))

ssc.start()
ssc.awaitTermination()
  