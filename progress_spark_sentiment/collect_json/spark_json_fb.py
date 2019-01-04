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

import time
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
        if 'created_time' in data_rdd.keys():
            result['created_time'] = data_rdd['created_time']
        else:
            result['created_time'] = None
                                                
        if 'count' in data_rdd['comments'].keys():
            result['comments_count'] = data_rdd['comments']['count']
        else:
            result['comments_count']  = None
                                            
        if 'group_id' in data_rdd.keys():
            result['group_id'] = data_rdd['group_id']
        else:
            result['group_id'] = None
                                                
        if 'name' in data_rdd['from'].keys():
            result['from_name'] = data_rdd['from']['name']
        else:
            result['from_name'] = None
                                                
        if 'id' in data_rdd['from'].keys(): 
            result['from_id'] = data_rdd['from']['id']
        else:
            result['from_id'] = None
                                                
        if 'id' in data_rdd.keys():
            result['id_fb'] = data_rdd['id']
        else:
            result['id_fb'] = None
                                                
        if 'type' in data_rdd.keys():
            result['type_fb'] = data_rdd['type']
        else:
            result['type_fb'] = None
                                                
        if 'likes' in data_rdd.keys():
            result['likes_count'] = data_rdd['likes']['count']
        else:
            result['likes_count'] = None
                                            
        if 'status_type' in data_rdd.keys():
            result['status_type'] = data_rdd['status_type']
        else:
            result['status_type'] = None
        
        if 'name' in data_rdd.keys():
            result['name'] = data_rdd['name']
        else:
            result['name'] = None
        
        if 'link' in data_rdd.keys():
            result['link'] = data_rdd['link']
        else:
            result['link'] = None
        
        if 'message' in data_rdd.keys():
            result['message_fb'] = data_rdd['message']
        else:
            result['message_fb'] = None
            
        result["sentiment"], result["total_score"] = predictSentiment(cleanSentences(_lookup_words(stemmer.stem(result["message"]))))
        result["person"], result["company"], result["eventNegatif"], result["country"], result["city"], result["lat_city"], result["long_city"] = entity_analysis(result["message"])
        
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
kafkaStream = KafkaUtils.createDirectStream(ssc, topics = ['enh-facebook-post'], kafkaParams = {"metadata.broker.list": broker1})
# Here to parse the inbound messages isn't valid JSON
parsed = kafkaStream.map(lambda v: json.loads(v[1]))
parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
#parsed.saveAsTextFiles("file:///D:/spark-kafka.txt")

rdd_tot = parsed.map(lambda data: collectElementJSON(data))
rdd_tot.saveAsTextFiles("/ebdesk_trans/fb/{}/result_{}.json".format(date_time,timestr))

ssc.start()
ssc.awaitTermination()
  