# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 23:12:03 2018

@author: CS
"""

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

import time
import os

# Fungsi untuk menimpa kata-kata yang salah / alay dengan kata
# yang terdapat pada formalizationDict
# Contoh : gpp => tidak apa-apa
#          egp => emang saya pikirin

def utf8_decoder(s):
    """ Decode the unicode as UTF-8 """
    if s is None:
        return None
    return s.decode('ascii','ignore')

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

import io

sc = SparkContext(appName="PythonSparkStreamingKafka_Raw")  
sc.setLogLevel("WARN")

while(1):
    try:
        ssc = StreamingContext(sc, 10)
        
        #kafkaStream = KafkaUtils.createStream(ssc, 'NLP:2181', 'spark-streaming', {'weblogs':1})
        kafkaStream = KafkaUtils.createDirectStream(ssc, topics = ['enh-twitter-post'], kafkaParams = {"metadata.broker.list": broker1}, keyDecoder=utf8_decoder, valueDecoder=utf8_decoder,
                                   messageHandler=None)
        # Here to parse the inbound messages isn't valid JSON
        kafkaStream.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
        kafkaStream.saveAsTextFiles("/ebdesk_trans/twitter_raw/{}/result_{}.json".format(date_time,timestr))
        
        ssc.start()
        ssc.awaitTermination()
    except Exception as e:
            with open('error.txt', 'w') as f:
                f.write(unicode(str(e),"utf-8"))
                f.close()
                time.sleep(2000)