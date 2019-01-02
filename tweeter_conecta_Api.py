# -*- coding: utf-8 -*-
"""
Created on Sun Nov  4 13:52:47 2018

@author: Mariano
"""
import json
from textblob import TextBlob
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from elasticsearch import Elasticsearch
 
import twitter_credenciales



es = Elasticsearch(["http://lab-elasticsearch.dpsit.gba.gob.ar:9200"])
'''Nos conectamos a ElasticSearch'''
# Se conecta a Elastic via VPN ya que se encuentra en dominio de gobierno

# # # # TWITTER STREAMER # # # #
class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """
    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        
        # Uso las credenciales, cabe destacar que por seguridad siempre van en un archivo aparte
        # 
        listener = StdOutListener(fetched_tweets_filename)
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        stream = Stream(auth, listener)

        # This line filter Twitter Streams to capture data by the keywords: 
        stream.filter(languages=["en"],track=hash_tag_list)


# # # # TWITTER STREAM LISTENER # # # #
class StdOutListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            
            # decode json
            dict_data = json.loads(data)
            es.index(index="sentiment",
                 doc_type="test-type",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": dict_data["created_at"],
                       "message": dict_data["text"]})
            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True
          

    def on_error(self, status):
        print(status)

 
if __name__ == '__main__':
    
    

    hash_tag_list = ['like','friend','trump','politic','sun','snow','happy','funny']
    # Me guardo de respaldo los twits en un txt , luego tengo un proceso que lo borra
    fetched_tweets_filename = "tweets.txt"
    # Nos conectamos a la API y usamos los hashtag 
    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(fetched_tweets_filename, hash_tag_list)