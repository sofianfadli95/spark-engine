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

from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
import re

from pyspark.sql import SQLContext
from pyspark import SparkContext
import pyspark

# Inisialisasi fungsi Stemmer bahasa Indonesia
# Stemmer untuk membuang semua imbuhan dan mendapatkan kata dasarnya
factory = StemmerFactory()
stemmer = factory.create_stemmer()

lookup_dict = {}
# Dictionary utk lemmatize
with io.open("formalizationDict.txt", 'r') as f:
    for line in f:
        items = line.split()
        key, values = items[0], items[1:]
        lookup_dict[key] = ' '.join(values)

# Fungsi untuk menimpa kata-kata yang salah / alay dengan kata
# yang terdapat pada formalizationDict
# Contoh : gpp => tidak apa-apa
#          egp => emang saya pikirin
        
def _lookup_words(input_text):
    words = input_text.split() 
    new_words = []
    new_text = ""
    for word in words:
        if word.lower() in lookup_dict:
            word = lookup_dict[word.lower()]
        new_words.append(word)
        new_text = " ".join(new_words) 
    return new_text

def utf8_decoder(s):
    """ Decode the unicode as UTF-8 """
    if s is None:
        return None
    return s.decode('ascii','ignore')

# Removes punctuation, parentheses, question marks, etc., and leaves only alphanumeric characters
strip_special_chars = re.compile("[^A-Za-z ]+")

def cleanSentences(string):
    string = string.lower().replace("<br />", " ")
    return re.sub(strip_special_chars, " ", string.lower())

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

def predictSentiment(data):
    pos_score = 0
    neg_score = 0
    words = word_tokenize(data.lower())
    
    for word in words:
        if word in neg_words:
            neg_score += 1
        elif word in pos_words:
            pos_score += 1
    if neg_score > pos_score:
        sentiment = "negatif"
    elif pos_score > neg_score:
        sentiment = "positif"
    else:
        sentiment = "netral"
        
    if (pos_score + neg_score) != 0 :
        total_score = (pos_score - neg_score) / (pos_score + neg_score)
    else:
        total_score = 0
    return sentiment, total_score

def entity_analysis(data):
    try:
        words = word_tokenize(data.lower())
        id_location = 0
        id_company = 0
        id_country = 0
        id_event = 0
        id_job = 0
        id_organization = 0
        id_person = 0
        id_product = 0
        id_event_neg = 0
        person = []
        company = []
        eventNegatif = []
        country = []
        location = []
        lat_city = []
        long_city = []
            
        """
        # Check apakah ada Entity City dalam kalimat
        for w in words:
            if w not in b_city:
                id_city+=1
            else:
                # Cek apakah jumlah id kata selanjutnya sudah melebihi panjang kalimat atau belum
                if (id_city+1) < len(words):
                    # Jika belum, kita dapat mengecek kata selanjutnya apakah termasuk bagian entity City atau tidak
                    if words[id_city+1] in i_city:
                        print(words[id_city]," ",words[id_city+1]," : "," city ")
                        #city.append(words[id_city]+" "+words[id_city+1])
                        geocode_result = gmaps.geocode(words[id_city]+" "+words[id_city+1])
                        dictCity[words[id_city]+" "+words[id_city+1]] = geocode_result[0]['geometry']['location']
                        id_city+=1
                    else:
                        print(words[id_city]," : "," city ")
                        #city.append(words[id_city])
                        geocode_result = gmaps.geocode(words[id_city])
                        dictCity[words[id_city]] = geocode_result[0]['geometry']['location']
                        id_city+=1
                else:
                    print(words[id_city]," : "," city ")
                    #city.append(words[id_city])
                    geocode_result = gmaps.geocode(words[id_city])
                    dictCity[words[id_city]] = geocode_result[0]['geometry']['location']
                    id_city+=1
        """                
        # Check apakah ada Entity Person dalam kalimat
        for w in words:
            if w not in b_person:
                id_person+=1
            else:
                if (id_person+1) < len(words):
                    if words[id_person+1] in i_person:
                        #print(words[id_person]," ",words[id_person+1]," : "," person ")
                        if (words[id_person]+" "+words[id_person+1]) not in person:
                            person.append(words[id_person]+" "+words[id_person+1])
                            id_person+=1
                        else:
                            id_person+=1
                            
                    else:
                        #print(words[id_person]," : "," person ")
                        if (words[id_person]) not in person:
                            person.append(words[id_person])
                            id_person+=1
                        else:    
                            id_person+=1
                else:
                    #print(words[id_person]," : "," person ")
                    if (words[id_person]) not in person:
                        person.append(words[id_person])
                        id_person+=1
                    else:
                        id_person+=1
                                
        # Check apakah ada Entity Company dalam kalimat
        for w in words:
            if w not in b_company:
                id_company+=1
            else:
                if (id_company+1) < len(words):
                    if words[id_company+1] in i_company:
                        #print(words[id_company]," ",words[id_company+1]," : "," company ")
                        if words[id_company]+" "+words[id_company+1] not in company:
                            company.append(words[id_company]+" "+words[id_company+1])
                            id_company+=1
                        else:
                            id_company+=1
                            
                    else:
                        #print(words[id_company]," : "," company ")
                        if words[id_company] not in company:
                            company.append(words[id_company])
                            id_company+=1
                        else:
                            id_company+=1
                else:
                    #print(words[id_company]," : "," company ")
                    if words[id_company] not in company:
                        company.append(words[id_company])
                        id_company+=1
                    else:
                        id_company+=1
                                
        # Check apakah ada Entity Country dalam kalimat
        for w in words:
            if w not in b_country:
                id_country+=1
            else:
                if (id_country+1) < len(words):
                    if words[id_country+1] in i_country:
                        #print(words[id_country]," ",words[id_country+1]," : "," country ")
                        # Jika seandainya kata belum ada di list country, maka tambahkan
                        # Untuk menghindari ada entity yang double
                        if (words[id_country]+" "+words[id_country+1]) not in country:
                            country.append(words[id_country]+" "+words[id_country+1])
                            id_country+=1
                        else:
                            id_country+=1
                    else:
                        #print(words[id_country]," : "," country ")
                        if words[id_country] not in country:
                            country.append(words[id_country])
                            id_country+=1
                        else:
                            id_country+=1
                else:
                    if words[id_country] not in country:
                        country.append(words[id_country])
                        id_country+=1
                    else:
                        id_country+=1
                            
        # Check apakah ada Entity Event Negatif dalam kalimat
        for w in words:
            if stemmer.stem(w) not in event_negatif:
                id_event_neg+=1
            else:
                #print(words[id_event_neg]," : "," eventNegatif ")
                # Jika seandainya kata belum ada di list eventNegatif, maka tambahkan
                # Untuk menghindari ada entity yang double
                if words[id_event_neg] not in eventNegatif:
                    eventNegatif.append(words[id_event_neg])
                    id_event_neg+=1
                else:
                    id_event_neg+=1
            
        # Check apakah ada Entity Location dalam kalimat
        for w in words:
            if w not in b_location:
                id_location+=1
            else:
                if (id_location+1) < len(words):
                    if words[id_location+1] in i_location:
                        #print(words[id_location]," ",words[id_location+1]," : "," location ")
                        if (words[id_location]+" "+words[id_location+1]) not in location:
                            location.append(words[id_location]+" "+words[id_location+1])
                            id_location+=1
                        else:
                            id_location+=1
                            
                    else:
                        #print(words[id_location]," : "," location ")
                        if (words[id_location]) not in location:
                            location.append(words[id_location])
                            id_location+=1
                        else:    
                            id_location+=1
                else:
                    #print(words[id_location]," : "," location ")
                    if (words[id_location]) not in location:
                        location.append(words[id_location])
                        id_location+=1
                    else:
                        id_location+=1
                    
        # Mapping location ke lattitude dan longitudenya
        for w in location:
            if w in loc_dict.keys():
                lat_city.append(loc_dict[w]["lattitude"])
                long_city.append(loc_dict[w]["longitude"])
            # Jika tidak ada, maka buang kata tersebut
            else:
                location.remove(w)
        
        return ",".join(person), ",".join(company), ",".join(eventNegatif), ",".join(country), ",".join(location), ",".join(lat_city), ",".join(long_city)
    except TypeError:
        raise

def collectElementJSON(data_rdd):
        result = {}
        #print(type(data_rdd))
        data_rdd = json.loads(data_rdd)
        #print(type(data_rdd))
        key = ["created_at", "id", "id_str", "text", "truncated", "source", "is_quote_status", 
               "quoted_status_id", "quoted_status_id_str", "retweet_count", "favorite_count", "favorited","retweeted","lang"]
        for element in key:
            try:
                if type(data_rdd[element]) == unicode:
                    result[element] = data_rdd[element].encode('ascii','ignore')
                else:
                    result[element] = data_rdd[element]
                print("Success loads the data")
                print(type(data_rdd[element]))
            except KeyError:
                result[element] = None
        result["sentiment"], result["total_score"] = predictSentiment(cleanSentences(_lookup_words(stemmer.stem(result["text"]))))
        result["person"], result["company"], result["eventNegatif"], result["country"], result["city"], result["lat_city"], result["long_city"] = entity_analysis(result["text"])
        result_json = json.dumps(result)
        return result_json
    
def getValueDictionary(data_rdd):
    data_rdd = json.loads(data_rdd)
    key = ["created_at", "id", "id_str", "text", "truncated", "source", "is_quote_status", 
               "quoted_status_id", "quoted_status_id_str", "retweet_count", "favorite_count", "favorited","retweeted","lang"]
    list_value = []
    for element in key:
        list_value.append(data_rdd[element])
    return list_value
    
from nltk.tokenize import word_tokenize
import io

neg_words = set(line.strip().lower() for line in io.open('./Dataset/neg.txt'))
pos_words = set(line.strip().lower() for line in io.open('./Dataset/pos.txt'))

b_location = set(line.strip().lower() for line in io.open('./dictionary_entity/b_location.txt', encoding="utf8"))
b_company = set(line.strip().lower() for line in io.open('./dictionary_entity/b_company.txt', encoding="utf8"))
b_country = set(line.strip().lower() for line in io.open('./dictionary_entity/b_country.txt', encoding="utf8"))
b_event = set(line.strip().lower() for line in io.open('./dictionary_entity/b_event.txt', encoding="utf8"))
b_job_title = set(line.strip().lower() for line in io.open('./dictionary_entity/b_job_title.txt', encoding="utf8"))
b_organization = set(line.strip().lower() for line in io.open('./dictionary_entity/b_organization.txt', encoding="utf8"))
b_person = set(line.strip().lower() for line in io.open('./dictionary_entity/b_person.txt', encoding="utf8"))
b_product = set(line.strip().lower() for line in io.open('./dictionary_entity/b_product.txt', encoding="utf8"))
event_negatif = set(line.strip().lower() for line in io.open('./dictionary_entity/eventNegatif.txt', encoding="utf8"))
i_location = set(line.strip().lower() for line in io.open('./dictionary_entity/i_location.txt', encoding="utf8"))
i_company = set(line.strip().lower() for line in io.open('./dictionary_entity/i_company.txt', encoding="utf8"))
i_country = set(line.strip().lower() for line in io.open('./dictionary_entity/i_country.txt', encoding="utf8"))
i_event = set(line.strip().lower() for line in io.open('./dictionary_entity/i_event.txt', encoding="utf8"))
i_job_title = set(line.strip().lower() for line in io.open('./dictionary_entity/i_job_title.txt', encoding="utf8"))
i_organization = set(line.strip().lower() for line in io.open('./dictionary_entity/i_organization.txt', encoding="utf8"))
i_person = set(line.strip().lower() for line in io.open('./dictionary_entity/i_person.txt', encoding="utf8"))
i_product = set(line.strip().lower() for line in io.open('./dictionary_entity/i_product.txt', encoding="utf8"))

# Inisialisasi data city beserta dengan koordinat lokasinya
with io.open("Location2.txt", 'r', encoding="ascii", errors = 'replace') as f:
    list_city = []
    loc_dict = {}
    for line in f:
        items = line.split("\t")
        key, values1, values2 = items[0].lower() , items[1], items[2]
        loc_dict[key.lower()] = { "lattitude" : values1.replace("\n",""), "longitude" : values2.replace("\n","") }
        list_city.append(key)
    list_city = set(list_city)


sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 10)

#kafkaStream = KafkaUtils.createStream(ssc, 'NLP:2181', 'spark-streaming', {'weblogs':1})
kafkaStream = KafkaUtils.createDirectStream(ssc, topics = ['enh-twitter-post'], kafkaParams = {"metadata.broker.list": "NLP:9092"}, keyDecoder=utf8_decoder, valueDecoder=utf8_decoder,
                           messageHandler=None)
# Here to parse the inbound messages isn't valid JSON
parsed = kafkaStream.map(lambda v: json.loads(v[1]))
parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()
#parsed.saveAsTextFiles("file:///D:/spark-kafka.txt")

rdd_tot = parsed.map(lambda data_new: collectElementJSON(data_new))
#rdd_tot.saveAsTextFiles("file:///D:/PROJECT_MABESPOLRI/progress_spark_sentiment/{}/result_{}.json".format(date_time,timestr))

__all__ = [
    "DataType", "NullType", "StringType", "BinaryType", "BooleanType", "DateType",
    "TimestampType", "DecimalType", "DoubleType", "FloatType", "ByteType", "IntegerType",
    "LongType", "ShortType", "ArrayType", "MapType", "StructField", "StructType"]

from pyspark.sql.types import DataType
from pyspark.sql.types import NullType
from pyspark.sql.types import StringType
from pyspark.sql.types import BinaryType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import DateType
from pyspark.sql.types import TimestampType
from pyspark.sql.types import DecimalType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import FloatType
from pyspark.sql.types import ByteType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import ShortType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import MapType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType    

schema = StructType([
    StructField("created_at", DateType(), True),
    StructField("id", StringType(), True),
    StructField("id_str", StringType(), True),
    StructField("text", StringType(), True),
    StructField("truncated", BooleanType(), True),
    StructField("source", StringType(), True),
    StructField("is_quote_status", BooleanType(), True),
    StructField("quoted_status_id", StringType(), True),
    StructField("qouted_status_id_str", StringType(), True),
    StructField("retweet_count", IntegerType(), True),
    StructField("favorite_count", IntegerType(), True),
    StructField("favorited", BooleanType(), True),
    StructField("retweeted", BooleanType(), True),
    StructField("lang", StringType(), True)
])

rdd_hive = rdd_tot.map(lambda x: getValueDictionary(x))
df = sqlContext.createDataFrame(rdd_hive, schema)
df.show()
df_writer = pyspark.sql.DataFrameWriter(df)
df_writer.saveAsTable('twitter_nlp', format='parquet', mode='overwrite'
                     ,path="file:///{}/{}/twitter_table_{}.parquet".format(file_path,date_time,timestr))

ssc.start()
ssc.awaitTermination()
  