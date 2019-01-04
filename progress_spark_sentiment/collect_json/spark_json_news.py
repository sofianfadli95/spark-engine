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
        if 'media_category' in data_rdd.keys():
            result['media_category'] = data_rdd['media_category']
        else:
            result['media_category'] = None
                                    
        if 'n_id' in data_rdd.keys():
            result['news_id'] = data_rdd['n_id']
        else:
            result['news_id'] = None
                                    
        if 'file_content' in data_rdd.keys():
            result['file_content'] = data_rdd['file_content']
        else:
            result['file_content'] = None
                                    
        if 'file_name' in data_rdd.keys():
            result['file_name'] = data_rdd['file_name']
        else:
            result['file_name'] = None
                                    
        if 'n_link' in data_rdd.keys():
            result['news_link'] = data_rdd['n_link']
        else:
            result['news_link'] = None
                                    
        if 'tablename' in data_rdd.keys():
            result['table_name'] = data_rdd['tablename']
        else:
            result['table_name'] = None
                                    
        if 'news_date' in data_rdd.keys():
            result['news_date'] = data_rdd['news_date']
        else:
            result['news_date'] = None
                                    
        if 'worker_id' in data_rdd.keys():
            result['worker_id'] = data_rdd['worker_id']
        else:
            result['worker_id'] = None
                                
        if 'result_content' in data_rdd.keys():
            result['result_content'] = data_rdd['result_content']
        else:
            result['result_content'] = None
            
        result["sentiment"], result["total_score"] = predictSentiment(cleanSentences(_lookup_words(stemmer.stem(result["result_content"]))))
        result["person"], result["company"], result["eventNegatif"], result["country"], result["city"], result["lat_city"], result["long_city"] = entity_analysis(result["result_content"])
        
        return result
    except Exception as e: # most generic exception you can catch
        logf.write(str(e))
    finally:
        # optional clean up code
        pass

sc = SparkContext(appName="PythonSparkStreamingKafka_News")  
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 2)

#kafkaStream = KafkaUtils.createStream(ssc, 'NLP:2181', 'spark-streaming', {'weblogs':1})
kafkaStream = KafkaUtils.createDirectStream(ssc, topics = ['raw-news'], kafkaParams = {"metadata.broker.list": broker1})
# Here to parse the inbound messages isn't valid JSON
parsed = kafkaStream.map(lambda v: json.loads(v[1]))
parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
#parsed.saveAsTextFiles("file:///D:/spark-kafka.txt")

rdd_tot = parsed.map(lambda data: collectElementJSON(data))
rdd_tot.saveAsTextFiles("/ebdesk_trans/news/{}/result_{}.json".format(date_time,timestr))

ssc.start()
ssc.awaitTermination()
  