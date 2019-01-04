# -*- coding: utf-8 -*-
"""
Created on Tue Apr  3 18:21:38 2018

@author: CS
"""

import os  
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
import io

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

from DataPreprocessing import cleanSentences
from DataPreprocessing import _lookup_words
from DataPreprocessing import stemmer

from nlp import predictSentiment
from nlp import entity_analysis

logf = io.open("error.log", "w")

broker1 = "cks-svrdpw-02038:6667"
broker2 = "cks-svrdpw-02036:6667"
broker3 = "cks-svrdpw-02037:6667"

timestr = time.strftime("(%Y-%m-%d_%H-%M-%S)")
date_time = time.strftime("(%Y-%m-%d)")

file_path = os.getcwd()
directory = os.path.dirname(file_path)

# Membuat direktori berdasarkan tgl hr ini
if not os.path.exists(directory):
    os.makedirs(directory)

def collectElementJSON(data_rdd):
    try:
        result = {}
        if "raw" in data_rdd.keys():
            if 'created_at' in data_rdd["raw"].keys():
                result['created_at'] = data_rdd["raw"]['created_at']
            else:
                result['created_at'] = None
                                            
        if 'source' in data_rdd['raw'].keys():
            result['source'] = data_rdd['raw']['source']
        else:
            result['source'] = None
                                                
        if 'retweet_count' in data_rdd['raw'].keys():
            result['retweet_count'] = data_rdd['raw']['retweet_count']
        else:
            result['retweet_count']  = None
                                            
        if 'id_str' in data_rdd['raw'].keys():
            result['id_str'] = data_rdd['raw']['id_str']
        else:
            result['id_str'] = None
                                                
        if 'favorite_count' in data_rdd['raw'].keys():
            result['favorite_count'] = data_rdd['raw']['favorite_count']
        else:
            result['favorite_count'] = None
                                                
        if 'id' in data_rdd['raw'].keys(): 
            result['id_tweet'] = data_rdd['raw']['id']
        else:
            result['id_tweet'] = None
                                                
        if 'text' in data_rdd['raw'].keys():
            result['text'] = data_rdd['raw']['text']
        else:
            result['text'] = None
                                                
        if 'lang' in data_rdd['raw'].keys():
            result['language'] = data_rdd['raw']['lang']
        else:
            result['language'] = None
                                                
        if 'quote_count' in data_rdd['raw'].keys():
            result['quote_count'] = data_rdd['raw']['quote_count']
        else:
            result['quote_count'] = None
                                                
        if 'timestamp_ms' in data_rdd['raw'].keys():
            result['timestamp_ms'] = data_rdd['raw']['timestamp_ms']
        else:
            result['timestamp_ms'] = None
                                            
        if 'reply_count' in data_rdd['raw'].keys():
            result['reply_count'] = data_rdd['raw']['reply_count']
        else:
            result['reply_count'] = None
                                                
        if 'entities' in data_rdd['raw'].keys():
            if 'urls' in data_rdd['raw']['entities'].keys():
                try:
                    result['entities_urls'] = ",".join(data_rdd['raw']['entities']['urls'])
                except TypeError:
                    result['entities_urls'] = None
            else:
                result['entities_urls'] = None
                                                
            if 'hashtags' in data_rdd['raw']['entities'].keys():
                try:
                    result['entities_hashtags'] = ",".join(data_rdd['raw']['entities']['hashtags'])
                except TypeError:
                    result['entities_hashtags'] = None
            else:
                result['entities_hashtags'] = None
                                                
            if 'symbols' in data_rdd['raw']['entities'].keys():
                try:
                    result['entities_symbols'] = ",".join(data_rdd['raw']['entities']['symbols'])
                except TypeError:
                    result['entities_symbols'] = None
            else:
                result['entities_symbols'] = None
                                    
        else:
            result['entities_urls'] = None
            result['entities_hashtags'] = None
            result['entities_symbols'] = None
                                                
        if 'user' in data_rdd['raw'].keys():
            if 'screen_name' in data_rdd['raw']['user'].keys():
                result['user_screen_name'] = data_rdd['raw']['user']['screen_name']
            else:
                result['user_screen_name'] = None
                                            
            if 'id_str' in data_rdd['raw']['user'].keys():
                result['user_id'] = data_rdd['raw']['user']['id_str']
            else:
                result['user_id'] = None
        else:
            result['user_screen_name'] = None
            result['user_id'] = None
            
        result["sentiment"], result["total_score"] = predictSentiment(cleanSentences(_lookup_words(stemmer.stem(result["text"]))))
        result["person"], result["company"], result["eventNegatif"], result["country"], result["city"], result["lat_city"], result["long_city"] = entity_analysis(result["text"])
        
        return result
    except Exception as e: # most generic exception you can catch
        logf.write(str(e))
    finally:
        # optional clean up code
        pass

sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")  
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 2)

#kafkaStream = KafkaUtils.createStream(ssc, 'NLP:2181', 'spark-streaming', {'weblogs':1})
kafkaStream = KafkaUtils.createDirectStream(ssc, topics = ['enh-twitter-post'], kafkaParams = {"metadata.broker.list": broker1})
# Here to parse the inbound messages isn't valid JSON
parsed = kafkaStream.map(lambda v: json.loads(v[1]))
parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
#parsed.saveAsTextFiles("file:///D:/spark-kafka.txt")

rdd_tot = parsed.map(lambda data: collectElementJSON(data))
rdd_tot.saveAsTextFiles("/ebdesk_trans/twitter/{}/result_{}.json".format(date_time,timestr))

ssc.start()
ssc.awaitTermination()
  