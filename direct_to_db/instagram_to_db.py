# -*- coding: utf-8 -*-
"""
Created on Sat Nov 18 18:44:16 2017

@author: Sofian
"""

numDimensions = 300
maxSeqLength = 70
batchSize = 24
lstmUnits = 128
numClasses = 2
iterations = 100000

import numpy as np
import pickle
from nltk.tokenize import word_tokenize
import DataPreprocessing as proc
import time
#import spellCorrection as spell # Fungsi utk SpellCorrection jika ada mispell

"""
Ini codingan untuk menarik data yang berada di server Kafka,
kemudian menembakkan data tersebut ke API sentiment analysis dan pengenalan entitas
Dalam hal ini, data yang ditarik adalah data yang berada di topic "twitterstream"
Selanjutnya, hasil dari Call API sentiment akan ditembakkan ke topic "sentiment-result"
Sedangkan, hasil dari Call API pengenalan entitas akan ditembakkan ke topic "entity-result"

"""

from kafka import KafkaConsumer
# Connect to Kafka server and pass the topic we want to consume

import cx_Oracle # Library utk kita menggunakan API Database Oracle
import json


broker1 = "cks-svrdpw-02038:6667"
broker2 = "cks-svrdpw-02036:6667"
broker3 = "cks-svrdpw-02037:6667"

# Inisialisasi alamat untuk connect ke DB Oracle
con = cx_Oracle.connect('TESTUSER/TESTUSER@10.100.15.239:1521/DWHPROD')
cur = con.cursor()
consumer = KafkaConsumer(bootstrap_servers=broker1,
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))

consumer.subscribe(['enh-instagram-post'])

b_city = set(line.strip().lower() for line in open('./dictionary_entity/b_city.txt', encoding="utf8"))
b_company = set(line.strip().lower() for line in open('./dictionary_entity/b_company.txt', encoding="utf8"))
b_country = set(line.strip().lower() for line in open('./dictionary_entity/b_country.txt', encoding="utf8"))
b_event = set(line.strip().lower() for line in open('./dictionary_entity/b_event.txt', encoding="utf8"))
b_job_title = set(line.strip().lower() for line in open('./dictionary_entity/b_job_title.txt', encoding="utf8"))
b_organization = set(line.strip().lower() for line in open('./dictionary_entity/b_organization.txt', encoding="utf8"))
b_person = set(line.strip().lower() for line in open('./dictionary_entity/b_person.txt', encoding="utf8"))
b_product = set(line.strip().lower() for line in open('./dictionary_entity/b_product.txt', encoding="utf8"))
event_negatif = set(line.strip().lower() for line in open('./dictionary_entity/eventNegatif.txt', encoding="utf8"))
i_city = set(line.strip().lower() for line in open('./dictionary_entity/i_city.txt', encoding="utf8"))
i_company = set(line.strip().lower() for line in open('./dictionary_entity/i_company.txt', encoding="utf8"))
i_country = set(line.strip().lower() for line in open('./dictionary_entity/i_country.txt', encoding="utf8"))
i_event = set(line.strip().lower() for line in open('./dictionary_entity/i_event.txt', encoding="utf8"))
i_job_title = set(line.strip().lower() for line in open('./dictionary_entity/i_job_title.txt', encoding="utf8"))
i_organization = set(line.strip().lower() for line in open('./dictionary_entity/i_organization.txt', encoding="utf8"))
i_person = set(line.strip().lower() for line in open('./dictionary_entity/i_person.txt', encoding="utf8"))
i_product = set(line.strip().lower() for line in open('./dictionary_entity/i_product.txt', encoding="utf8"))
    

# import pickle file as dictionary words
"""

    Dictionary.pickle menyimpan setiap kode dari setiap kata unik
    Sementara final_embeddings.npy berisi Word2Vec yang telah kita train sebelumnya
    
"""
with open('dictionary.pickle', 'rb') as handle:
    wordsList = pickle.load(handle)
wordVectors = np.load('final_embeddings.npy')

loc_dict = {}
with open("Location.txt", 'r', encoding="utf-8", errors = 'ignore') as f:
    for line in f:
        items = line.split()
        key, values1, values2 = items[0], items[1], items[2]
        loc_dict[key.lower()] = { "lattitude" : values1, "longitude" : values2 }
        
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
        
def entity(data):
    try:
        entity_results = {}
        #sentences = data['sentences']
        words = word_tokenize(data.lower())
        id_city = 0
        id_company = 0
        id_country = 0
        id_event = 0
        id_job = 0
        id_organization = 0
        id_person = 0
        id_product = 0
        id_event_neg = 0
        id_location = 0
        person = []
        company = []
        eventNegatif = []
        country = []
        location = {}
            
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
                        print(words[id_person]," ",words[id_person+1]," : "," person ")
                        if (words[id_person]+" "+words[id_person+1]) not in person:
                            person.append(words[id_person]+" "+words[id_person+1])
                            id_person+=1
                        else:
                            id_person+=1
                            
                    else:
                        print(words[id_person]," : "," person ")
                        if (words[id_person]) not in person:
                            person.append(words[id_person])
                            id_person+=1
                        else:    
                            id_person+=1
                else:
                    print(words[id_person]," : "," person ")
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
                        print(words[id_company]," ",words[id_company+1]," : "," company ")
                        if words[id_company]+" "+words[id_company+1] not in company:
                            company.append(words[id_company]+" "+words[id_company+1])
                            id_company+=1
                        else:
                            id_company+=1
                            
                    else:
                        print(words[id_company]," : "," company ")
                        if words[id_company] not in company:
                            company.append(words[id_company])
                            id_company+=1
                        else:
                            id_company+=1
                else:
                    print(words[id_company]," : "," company ")
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
                        print(words[id_country]," ",words[id_country+1]," : "," country ")
                        # Jika seandainya kata belum ada di list country, maka tambahkan
                        if (words[id_country]+" "+words[id_country+1]) not in country:
                            country.append(words[id_country]+" "+words[id_country+1])
                            id_country+=1
                        else:
                            id_country+=1
                    else:
                        print(words[id_country]," : "," country ")
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
            if proc.stemmer.stem(w) not in event_negatif:
                id_event_neg+=1
            else:
                print(words[id_event_neg]," : "," eventNegatif ")
                # Jika seandainya kata belum ada di list eventNegatif, maka tambahkan
                if words[id_event_neg] not in eventNegatif:
                    eventNegatif.append(words[id_event_neg])
                    id_event_neg+=1
                else:
                    id_event_neg+=1
            
        # Check apakah ada Entity Location dalam kalimat
        for w in words:
            if w not in loc_dict.keys():
                id_location+=1
            else:
                print(words[id_location]," : "," Location ")
                # Jika seandainya kata belum ada di dictionary lokasi, maka tambahkan
                if words[id_location] not in location.keys():
                    location[words[id_location]] = loc_dict[words[id_location]]
                    id_location+=1
                else:
                    id_location+=1
        
        entity_results['City'] = location
        entity_results['Person'] = person
        entity_results['Company'] = company
        entity_results['eventNegatif'] = eventNegatif
        entity_results['Country'] = country
        
        return entity_results
    except TypeError:
        raise
    
# Start the web server
if __name__ == "__main__":
    # create a new cursor
    con = cx_Oracle.connect('TESTUSER/TESTUSER@10.100.15.239:1521/DWHPROD')
    cur = con.cursor()  
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))
        # Hasil tweet yang ditembakkan ke API
        # Hanya nilai tweet saja yang diambil dari consumer Kafka
        try:
            try:
                try:
                    try:
                        tweet = message.value['post_desc']
                        sentiment_results = sentimentPredict(tweet)
                        entity_results = entity(tweet)
                        # Simpan hasil ekstrak dari call API ke dalam dictionary
                        # Untuk selanjutnya nanti ditembakkan lagi ke Kafka dgn topic 'tweet-extraction'
                        if 'owner' in message.value.keys():
                            if 'full_name' in message.value['owner'].keys():
                                full_name = message.value['owner']['full_name']
                            else:
                                full_name = None
                        
                            if 'id' in message.value['owner'].keys():
                                id_user = message.value['owner']['id']
                            else:
                                id_user = None
                        
                            if 'username' in message.value['owner'].keys():
                                username = message.value['owner']['username']
                            else:
                                username = None
                        else:
                            full_name = None
                            id_user = None
                            username = None
        
                        if 'hashtags' in message.value.keys():
                            hashtags = " ".join(message.value['hashtags'])
                        else:
                            hashtags = None
                                    
                        if 'post_like' in message.value.keys():
                            post_like = message.value['post_like']
                        else:
                            post_like = None
                                        
                        if 'post_desc' in message.value.keys():
                            post_desc = message.value['post_desc']
                        else:
                            post_desc = None
                                    
                        if 'crawling_date' in message.value.keys():
                            crawling_date = message.value['crawling_date']
                        else:
                            crawling_date = None
                                        
                        if 'post_id' in message.value.keys():
                            post_id = message.value['post_id']
                        else:
                            post_id = None
                                        
                        if 'post_date' in message.value.keys(): 
                            post_date = message.value['post_date']
                        else:
                            post_date = None
                                        
                        if 'post_code' in message.value.keys():
                            post_code = message.value['post_code']
                        else:
                            post_code = None
                                        
                        if 'post_comment' in message.value.keys():
                            post_comment = message.value['post_comment']
                        else:
                            post_comment = None
                                        
                        if 'edge_media_tp_comment' in message.value['raw_data'].keys():
                            edge_media_tp_comment = message.value['raw_data']['edge_media_tp_comment']['count']
                        else:
                            edge_media_tp_comment = None
                                        
                        if 'tracking_token' in message.value['raw_data'].keys():
                            tracking_token = message.value['raw_data']['tracking_token']
                        else:
                            tracking_token = None
                                    
                        if 'shortcode' in message.value['raw_data'].keys():
                            shortcode = message.value['raw_data']['shortcode']
                        else:
                            shortcode = None
                                        
                        if 'edge_media_preview_like' in message.value['raw_data'].keys():
                            edge_media_preview_like = message.value['raw_data']['edge_media_preview_like']['count']
                        else:
                            edge_media_preview_like = None
                            
                        if 'taken_at_timestamp' in message.value['raw_data'].keys():
                            taken_at_timestamp = message.value['raw_data']['taken_at_timestamp']
                        else:
                            taken_at_timestamp = None
                            
                        if 'keyword_id' in message.value['raw_data'].keys():
                            keyword_id = message.value['raw_data']['keyword_id']
                        else:
                            keyword_id = None
                        
                        if 'post_picture' in message.value.keys():
                            post_picture = message.value['post_picture']
                        else:
                            post_picture = None
                                
                        positives_scores = sentiment_results['positiveScores']
                        negatives_scores = sentiment_results['negativeScores']
                        sentiment = sentiment_results['sentiment']
                        entitas_city = " ".join(entity_results["City"])
                        entitas_person = " ".join(entity_results["Person"])
                        entitas_company = " ".join(entity_results["Company"])
                        entitas_event_negatif = " ".join(entity_results["eventNegatif"])
                        entitas_country = " ".join(entity_results["Country"])
                        
                        cur.execute("""INSERT INTO INSTAGRAM_NLP(FULL_NAME,
                                                              ID_USER,
                                                              USERNAME,
                                                              HASHTAG,
                                                              POST_LIKE,
                                                              POST_DESC,
                                                              CRAWLING_DATE,
                                                              POST_ID,
                                                              POST_DATE,
                                                              POST_CODE,
                                                              POST_COMMENT,
                                                              EDGE_MEDIA_TP_COMMENT,
                                                              TRACKING_TOKEN,
                                                              SHORTCODE,
                                                              EDGE_MEDIA_PREVIEW_LIKE,
                                                              TAKEN_AT_TIMESTAMP,
                                                              KEYWORD_ID,
                                                              POST_PICTURE,
                                                              POSITIVE_SCORES,
                                                              NEGATIVE_SCORES,
                                                              SENTIMENT,
                                                              ENTITAS_CITY,
                                                              ENTITAS_PERSON,
                                                              ENTITAS_COMPANY,
                                                              ENTITAS_EVENT_NEGATIF,
                                                              ENTITAS_COUNTRY) VALUES (:1, :2, :3, :4, :5, :6, :7, :8,
                                                              :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20
                                                              , :21, :22, :23, :24, :25, :26)""", (full_name, id_user, username,
                                                              hashtags, post_like, post_desc,
                                                              crawling_date, post_id, post_date,
                                                              post_code, post_comment, edge_media_tp_comment,
                                                              tracking_token, shortcode, edge_media_preview_like,
                                                              taken_at_timestamp, keyword_id, post_picture, positives_scores, 
                                                              negatives_scores, sentiment, entitas_city, entitas_person,
                                                              entitas_company, entitas_event_negatif, entitas_country ))
                                # commit the changes to the database
                        con.commit()
                        time.sleep(0.5)
                    except IndexError:
                        print("IndexError happens...")
                except TypeError:
                    raise
            except ValueError:
                print ('Decoding JSON has failed')
        except KeyError:
            print ('Decoding JSON has failed')
