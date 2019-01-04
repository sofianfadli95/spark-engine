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
        
import tensorflow as tf
tf.reset_default_graph()

labels = tf.placeholder(tf.float32, [batchSize, numClasses])
input_data = tf.placeholder(tf.int32, [batchSize, maxSeqLength])

data = tf.Variable(tf.zeros([batchSize, maxSeqLength, numDimensions]),dtype=tf.float32)
data = tf.nn.embedding_lookup(wordVectors,input_data)

lstmCell = tf.contrib.rnn.BasicLSTMCell(lstmUnits)
lstmCell = tf.contrib.rnn.DropoutWrapper(cell=lstmCell, output_keep_prob=0.25)
value, _ = tf.nn.dynamic_rnn(lstmCell, data, dtype=tf.float32)

weight = tf.Variable(tf.truncated_normal([lstmUnits, numClasses]))
bias = tf.Variable(tf.constant(0.1, shape=[numClasses]))
value = tf.transpose(value, [1, 0, 2])
last = tf.gather(value, int(value.get_shape()[0]) - 1)
prediction = (tf.matmul(last, weight) + bias)

correctPred = tf.equal(tf.argmax(prediction,1), tf.argmax(labels,1))
accuracy = tf.reduce_mean(tf.cast(correctPred, tf.float32))

sess = tf.InteractiveSession()
saver = tf.train.Saver()
saver.restore(sess, tf.train.latest_checkpoint('models'))
        
def collectElementJSON(data):
    elements = []
    data["sentiment"], data["total_score"] = predictSentiment(cleanSentences(_lookup_words(stemmer.stem(data["text"]))))
    data["person"], data["company"], data["eventNegatif"], data["country"], data["city"], data["lat_city"], data["long_city"] = entity_analysis(data["text"])
    for key, value in data.items():
        elements.append(str(value))
    result = "|".join(elements)
    return result

def getSentenceMatrix(sentence):
    arr = np.zeros([batchSize, maxSeqLength])
    sentenceMatrix = np.zeros([batchSize,maxSeqLength], dtype='int32')
    cleanedSentence = proc.cleanSentences(sentence)
    split = cleanedSentence.split()
    for indexCounter,word in enumerate(split):
        try:
            if word in wordsList:
                sentenceMatrix[0,indexCounter] = wordsList[word]
            else:
                sentenceMatrix[0,indexCounter] = 0 #Vector for unkown words
        except ValueError:
            sentenceMatrix[0,indexCounter] = 399999 #Vector for unkown words
    return sentenceMatrix

def sentimentCorrect(data):
    try:
        sentiment_results = {}
        #sentences = data['sentences']
        string = data.split(' ')
        exact = [(spell.correction(word)) for word in string]
        exact = ' '.join(exact)
        inputMatrix = getSentenceMatrix(proc.cleanSentences(proc._lookup_words(proc.stemmer.stem(exact))))
        predictedSentiment = sess.run(prediction, {input_data: inputMatrix})[0]
        # predictedSentiment[0] represents output score for positive sentiment
        # predictedSentiment[1] represents output score for negative sentiment
        print("Positive : ",predictedSentiment[0])
        print("Negative : ",predictedSentiment[1])
        if (predictedSentiment[0] > predictedSentiment[1]):
            result = "Positive"
        else:
            result = "Negative"
            
        sentiment_results["sentences"] = data
        sentiment_results["positiveScores"] = str(predictedSentiment[0])
        sentiment_results["negativeScores"] = str(predictedSentiment[1])
        sentiment_results["sentiment"] = result
        
        return sentiment_results
    except:
        print("Delay for 5 seconds")
        time.sleep(5)
    
def sentimentPredict(data):
        try:
            sentiment_results = {}
            #sentences = data['sentences']
            #string = sentences.split(' ')
            #exact = [get_exact_words(word) for word in string]
            #exact = ' '.join(exact)
            inputMatrix = getSentenceMatrix(proc.cleanSentences(proc._lookup_words(proc.stemmer.stem(data))))
            predictedSentiment = sess.run(prediction, {input_data: inputMatrix})[0]
            # predictedSentiment[0] represents output score for positive sentiment
            # predictedSentiment[1] represents output score for negative sentiment
            print("Positive : ",predictedSentiment[0])
            print("Negative : ",predictedSentiment[1])
            if (predictedSentiment[0] > predictedSentiment[1]):
                result = "Positive"
            else:
                result = "Negative"
                
            sentiment_results["sentences"] = data
            sentiment_results["positiveScores"] = str(predictedSentiment[0])
            sentiment_results["negativeScores"] = str(predictedSentiment[1])
            sentiment_results["sentiment"] = result
        
            return sentiment_results
        except TypeError:
            raise

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
rdd_tot.saveAsTextFiles("file:///D:/PROJECT_MABESPOLRI/progress_spark_sentiment/result.csv")

ssc.start()
ssc.awaitTermination()
  