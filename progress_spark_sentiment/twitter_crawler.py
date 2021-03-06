import requests
from requests_oauthlib import OAuth1
import json
import config
import csv
from urllib.parse import urlparse
from kafka import KafkaProducer
import requests

#Endpoints
url_rest = "https://api.twitter.com/1.1/search/tweets.json"
url_streaming = "https://stream.twitter.com/1.1/statuses/sample.json"

# connect to Kafka
producer = KafkaProducer(bootstrap_servers='NLP:9092'
                                 , value_serializer=lambda v: json.dumps(v).encode('utf-8'))

csv_head = ['Post ID','Screen Name', 'ID', "sentence", "Sentiment"]
params = {
'app_key': config.consumer_key,
'app_secret': config.consumer_secret,
'oauth_token': config.access_token,
'oauth_token_secret':config.access_secret
}

def get_tweets(querry, max_id = 0, post = {}):
    try:
        try:
            url_rest = "https://api.twitter.com/1.1/search/tweets.json"
            params = {'q' : querry, 'count' : 100, 'lang' : 'id', 'max_id': max_id}
            auth = OAuth1(config.consumer_key,config.consumer_secret,config.access_token,config.access_secret)
            result = requests.get(url_rest,params=params,auth=auth)
            for tweet in result.json()['statuses']:
                with open("tweet_save8.csv","a") as csv_open:
                    writer = csv.DictWriter(csv_open, delimiter=',', lineterminator='\n', fieldnames=csv_head)
                    writer.writerow({
                            'Post ID' : tweet['id'], 
                            'Screen Name': tweet['user']['name'].encode("ascii",errors="replace").decode("utf8"),
                            'ID': tweet['user']['id_str'],
                            "sentence": tweet['text'].encode("ascii",errors="replace").decode("utf8"),
                            #"created_at" : tweet["created_at"].encode("ascii",errors="replace").decode("utf8")
                            })
                    
                post[tweet['id']] = {'id' : tweet['user']['id_str'],
                    'name':tweet['user']['name'],
                    'text':tweet['text'].encode("ascii",errors="replace").decode("utf8"),
                    #'created_at':tweet['created_at']
                    }
                user_json = json.dumps(tweet)
                producer.send('enh-twitter-post', user_json)
                print(tweet)
                print("\n")
                max_id = tweet['id']
            if('next_results' not in result.json()['search_metadata']):
                print('next_result invalid')
                return user_json
            else:
                return get_tweets(querry,max_id,post)
        except TypeError:
            print("TypeError happens...")
    except KeyError:
        print("KeyError happens...")
#    return post

dict_post = get_tweets('anies')

openfile = open("tweet_all5.txt","a")
 
try:
    if dict_post : openfile.write(dict_post.encode("ascii",errors="replace").decode("utf8"))
    openfile.close()
except KeyError:
    openfile.close()
    
"""
#auth = auth = OAuth1(config.consumer_key,config.consumer_secret,config.access_token,config.access_secret)
#
#q = 'adira AND finance'
#
#params = {'q' : q, 'count' : 200, 'lang' : 'id', 'since_id': '926349041629196288'}
#
#results = requests.get(url_rest,params=params, auth=auth)
#print(len(results.json()))
##print(results.json()['search_metadata'])
##dicti = {}
##dicti = results.json()['search_metadata']['next_results']
##print(dicti)
##print(dicti.find("max_id"))
##min_indx = dicti.find("max_id")+7
##max_indx = dicti.find("&q=")
##print(dicti[min_indx:max_indx])
##print(dicti.find("&q="))
##print(results.json()['statuses'])
#for tweet in results.json()['statuses']:
##    print(tweet['created_at'])
#    with open("tweet_save.csv","a") as csv_open:
#        writer = csv.DictWriter(csv_open, delimiter=',', lineterminator='\n', fieldnames=csv_head)
#        writer.writerow({
#                'Post ID' : tweet['id'], 
#                'Screen Name':tweet['user']['name'].encode("ascii",errors="replace").decode("utf8"),
#                'ID':tweet['user']['id_str'],
#                "sentence":tweet['text'].encode("ascii",errors="replace").decode("utf8") 
#                })
#    print(tweet['id'])
#    print(tweet['text'].encode("ascii",errors="replace").decode("utf8"))
#    print(tweet['user']['name'])
#    print(tweet['user']['id_str'])
#    print()
#    
#stream_results = requests.get(url_streaming, stream=True,auth=auth)
#
#for line in stream_results.iter_lines():
#    try:
#        decoded_line = line.decode('utf-8')
#        print(json.loads(decoded_line)['text'])
#    except:
#         pass
"""
