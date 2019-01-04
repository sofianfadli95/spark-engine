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

import numpy as np
import pickle
from nltk.tokenize import word_tokenize
import DataPreprocessing as proc
import time

def main():
    
    sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
    sc.setLogLevel("WARN")
    # Creating a streaming context with batch interval of 10 sec
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")
    pwords = load_wordlist("./Dataset/positive.txt")
    nwords = load_wordlist("./Dataset/negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    
def load_wordlist(filename):
    """ 
    This function returns a list or set of words from the given filename.
    """	
    words = {}
    f = open(filename, 'rU')
    text = f.read()
    text = text.split('\n')
    for line in text:
        words[line] = 1
    f.close()
    return words

def wordSentiment(word,pwords,nwords):
    if word in pwords:
        return ('positive', 1)
    elif word in nwords:
        return ('negative', 1)
    
def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)

def sendRecord(record):
    connection = createNewConnection()
    connection.send(record)
    connection.close()


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
    ssc, topics = ['weblogs'], kafkaParams = {"metadata.broker.list": 'NLP:9092'})
    parsed = kstream.map(lambda v: json.loads(v[1]))
    parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
    # Each element of tweets will be the text of a tweet.
    # We keep track of a running total counts and print it at every time step.
    id_twitter = parsed.map(lambda tweet: tweet["id"])
    name = parsed.map(lambda tweet: tweet["name"])
    tweets = parsed.map(lambda tweet: tweet["text"])
    words = tweets.flatMap(lambda line:line.split(" "))
    positive = words.map(lambda word: ('Positive', 1) if word in pwords else ('Positive', 0))
    negative = words.map(lambda word: ('Negative', 1) if word in nwords else ('Negative', 0))
    allSentiments = positive.union(negative)
    sentimentCounts = allSentiments.reduceByKey(lambda x,y: x+y)
    runningSentimentCounts = sentimentCounts.updateStateByKey(updateFunction)
    runningSentimentCounts.pprint()
    
    # The counts variable hold the word counts for all time steps
    counts = []
    sentimentCounts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
    
    # Start the computation
    ssc.start() 
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully = True)

    return counts


if __name__=="__main__":
    main()
  