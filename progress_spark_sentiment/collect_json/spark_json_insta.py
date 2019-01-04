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
        if 'owner' in data_rdd.keys():
            if 'full_name' in data_rdd['owner'].keys():
                result['full_name'] = data_rdd['owner']['full_name']
            else:
                result['full_name'] = None
                        
            if 'id' in data_rdd['owner'].keys():
                result['id_user'] = data_rdd['owner']['id']
            else:
                result['id_user'] = None
                        
            if 'username' in data_rdd['owner'].keys():
                result['username'] = data_rdd['owner']['username']
            else:
                result['username'] = None
        else:
            result['full_name'] = None
            result['id_user'] = None
            result['username'] = None
        
        if 'hashtags' in data_rdd.keys():
            result['hashtags'] = ",".join(data_rdd['hashtags'])
        else:
            result['hashtags'] = None
                                    
        if 'post_like' in data_rdd.keys():
            result['post_like'] = data_rdd['post_like']
        else:
            result['post_like'] = None
                                        
        if 'post_desc' in data_rdd.keys():
            result['post_desc'] = data_rdd['post_desc']
        else:
            result['post_desc'] = None
                                    
        if 'crawling_date' in data_rdd.keys():
            result['crawling_date'] = data_rdd['crawling_date']
        else:
            result['crawling_date'] = None
                                        
        if 'post_id' in data_rdd.keys():
            result['post_id'] = data_rdd['post_id']
        else:
            result['post_id'] = None
                                        
        if 'post_date' in data_rdd.keys(): 
            result['post_date'] = data_rdd['post_date']
        else:
            result['post_date'] = None
                                        
        if 'post_code' in data_rdd.keys():
            result['post_code'] = data_rdd['post_code']
        else:
            result['post_code'] = None
                                        
        if 'post_comment' in data_rdd.keys():
            result['post_comment'] = data_rdd['post_comment']
        else:
            result['post_comment'] = None
                                        
        if 'edge_media_tp_comment' in data_rdd['raw_data'].keys():
            result['edge_media_tp_comment'] = data_rdd['raw_data']['edge_media_tp_comment']['count']
        else:
            result['edge_media_tp_comment'] = None
                                        
        if 'tracking_token' in data_rdd['raw_data'].keys():
            result['tracking_token'] = data_rdd['raw_data']['tracking_token']
        else:
            result['tracking_token'] = None
                                    
        if 'shortcode' in data_rdd['raw_data'].keys():
            result['shortcode'] = data_rdd['raw_data']['shortcode']
        else:
            result['shortcode'] = None
                                        
        if 'edge_media_preview_like' in data_rdd['raw_data'].keys():
            result['edge_media_preview_like'] = data_rdd['raw_data']['edge_media_preview_like']['count']
        else:
            result['edge_media_preview_like'] = None
                            
        if 'taken_at_timestamp' in data_rdd['raw_data'].keys():
            result['taken_at_timestamp'] = data_rdd['raw_data']['taken_at_timestamp']
        else:
            result['taken_at_timestamp'] = None
                            
        if 'keyword_id' in data_rdd['raw_data'].keys():
            result['keyword_id'] = data_rdd['raw_data']['keyword_id']
        else:
            result['keyword_id'] = None
                        
        if 'post_picture' in data_rdd.keys():
            result['post_picture'] = data_rdd['post_picture']
        else:
            result['post_picture'] = None
            
        result["sentiment"], result["total_score"] = predictSentiment(cleanSentences(_lookup_words(stemmer.stem(result["post_desc"]))))
        result["person"], result["company"], result["eventNegatif"], result["country"], result["city"], result["lat_city"], result["long_city"] = entity_analysis(result["post_desc"])
        
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
kafkaStream = KafkaUtils.createDirectStream(ssc, topics = ['enh-instagram-post'], kafkaParams = {"metadata.broker.list": broker1})
# Here to parse the inbound messages isn't valid JSON
parsed = kafkaStream.map(lambda v: json.loads(v[1]))
parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
#parsed.saveAsTextFiles("file:///D:/spark-kafka.txt")

rdd_tot = parsed.map(lambda data: collectElementJSON(data))
rdd_tot.saveAsTextFiles("/ebdesk_trans/instagram/{}/result_{}.json".format(date_time,timestr))

ssc.start()
ssc.awaitTermination()
  