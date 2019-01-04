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
        if 'object' in data_rdd.keys():
            if 'content' in data_rdd['object']:
                result['content'] = data_rdd['object']['content']
            else:
                result['content'] = None
        # Simpan hasil ekstrak dari call API ke dalam dictionary
        # Untuk selanjutnya nanti ditembakkan lagi ke Kafka dgn topic 'tweet-extraction'
        if 'actor' in data_rdd.keys():
            if 'id' in data_rdd['actor'].keys():
                result['actor_id'] = data_rdd['actor']['id']
            else:
                result['actor_id'] = None
            
            if 'displayName' in data_rdd['actor'].keys():
                result['actor_name'] = data_rdd['actor']['displayName']
            else:
                result['actor_name'] = None
                
            if 'url' in data_rdd['actor'].keys():
                result['actor_url'] = data_rdd['actor']['url']
            else:
                result['actor_url'] = None
            
        if 'kind' in data_rdd.keys():
            result['kind'] = ",".join(data_rdd['kind'])
        else:
            result['kind'] = None
            
        if 'verb' in data_rdd.keys():
            result['verb'] = data_rdd['verb']
        else:
            result['verb'] = None
            
        if 'etag' in data_rdd.keys():
            result['etag'] = data_rdd['etag']
        else:
            result['etag'] = None
                
        if 'id' in data_rdd.keys():
            result['id_gplus'] = data_rdd['id']
        else:
            result['id_gplus'] = None
                
        if 'published' in data_rdd.keys():
            result['published'] = data_rdd['published']
        else:
            result['published'] = None
                
        if 'title' in data_rdd.keys():
            result['title'] = data_rdd['title']
        else:
            result['title'] = None
                
        if 'updated' in data_rdd.keys():
            result['updated'] = data_rdd['updated']
        else:
            result['updated'] = None
            
        if 'url_post' in data_rdd.keys():
            result['url_post'] = data_rdd['url_post']
        else:
            result['url_post'] = None
            
        result["sentiment"], result["total_score"] = predictSentiment(cleanSentences(_lookup_words(stemmer.stem(result["content"]))))
        result["person"], result["company"], result["eventNegatif"], result["country"], result["city"], result["lat_city"], result["long_city"] = entity_analysis(result["content"])
        
        return result
    except Exception as e: # most generic exception you can catch
        print(e)
        logf.write(str(e))
    finally:
        # optional clean up code
        pass

sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")  
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 2)

#kafkaStream = KafkaUtils.createStream(ssc, 'NLP:2181', 'spark-streaming', {'weblogs':1})
kafkaStream = KafkaUtils.createDirectStream(ssc, topics = ['enh-gplus-post'], kafkaParams = {"metadata.broker.list": broker1})
# Here to parse the inbound messages isn't valid JSON
parsed = kafkaStream.map(lambda v: json.loads(v[1]))
parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
#parsed.saveAsTextFiles("file:///D:/spark-kafka.txt")

rdd_tot = parsed.map(lambda data: collectElementJSON(data))
rdd_tot.saveAsTextFiles("/ebdesk_trans/gplus/{}/result_{}.json".format(date_time,timestr))

ssc.start()
ssc.awaitTermination()
  