import tweepy
import twitter_credentials
from urllib3.exceptions import ProtocolError
import numpy as np
import pandas as pd
import re
import matplotlib.pyplot as plt
from textblob import TextBlob
import time
import telegram_send
from db import DataBase

class TwitterClient():

    def __init__(self, twitter_user=None):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_client = tweepy.API(self.auth)
        self.twitter_user = twitter_user

    def get_twitter_client_api(self):

        api = tweepy.API(self.auth, wait_on_rate_limit=True)

        return api

    def get_user_timeline_tweets(self, num_tweets):
        tweets = []
        for tweet in tweepy.Cursor(self.twitter_client.user_timeline, id=self.twitter_user).items(num_tweets):
            tweets.append(tweet)
        return tweets

    def get_friend_list(self, num_friends):
        friend_list = []
        for friend in tweepy.Cursor(self.twitter_client.friends, id=self.twitter_user).items(num_friends):
            friend_list.append(friend)
        return friend_list

    def get_home_timeline_tweets(self, num_tweets):
        home_timeline_tweets = []
        for tweet in tweepy.Cursor(self.twitter_client.home_timeline, id=self.twitter_user).items(num_tweets):
            home_timeline_tweets.append(tweet)
        return home_timeline_tweets

class TwitterAuthenticator():

    def authenticate_twitter_app(self):
        auth = tweepy.OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)

        return auth

class TwitterStreamer():

    def stream_tweets(self, hash_tag_list):

        stream = TwitterListener(
            twitter_credentials.CONSUMER_KEY, 
            twitter_credentials.CONSUMER_SECRET, 
            twitter_credentials.ACCESS_TOKEN, 
            twitter_credentials.ACCESS_TOKEN_SECRET
        )

        while True:
            try:
                stream.filter(track=hash_tag_list, stall_warnings=True)
            except (ProtocolError, AttributeError):
                time.sleep(5)
                continue

class TwitterListener(tweepy.Stream):

    def on_connect(self):
        print("Corriendo sistema... 🌎")
        self.telegram_alert = TwitterClientAlerts()
        self.telegram_alert.telegram_alert("Corriendo sistema... 🌎")

    def on_status(self, status):
        tweet_analyzer = TweetAnalyzer()
        db = DataBase()

        if hasattr(status, "retweeted_status"):  # Revisar si es Retweet                
            try:
                print(status.retweeted_status.extended_tweet["full_text"])
                sentiment = tweet_analyzer.analyze_sentiment(status.retweeted_status.extended_tweet["full_text"])
                print(f"Sentiment: {sentiment}")
                print(f"User: {status.user.screen_name}")
                print(f"User_name: {status.user.name}")
                print(f"User_id: {status.user.id}")
                print(f"User_created_at: {status.user.created_at}")
                print(f"Created_at: {status.created_at}")
                print(f"Location: {status.user.location}")
                print(f"Coordinates: {status.coordinates}")
                print(f"Followers: {status.user.followers_count}")
                print(f"ID_tweet: {status.id_str}")
                print(f"Retweet_count: {status.retweet_count}")
                print("----------------------------------------------")
                
                tweets = [
                    (
                        status.retweeted_status.extended_tweet["full_text"],
                        sentiment, 
                        status.user.screen_name, 
                        status.user.name, 
                        status.user.id, 
                        status.user.created_at, 
                        status.created_at, 
                        status.user.location, 
                        status.user.followers_count, 
                        status.id_str
                    )
                ]
                
                db.insert_data("retweets", tweets)

                # telegram_send.send(messages=[message])
            except AttributeError:
                print(status.retweeted_status.text)
                sentiment = tweet_analyzer.analyze_sentiment(status.retweeted_status.text)
                print(f"Sentiment: {sentiment}")
                print(f"User: {status.user.screen_name}")
                print(f"User_name: {status.user.name}")
                print(f"User_id: {status.user.id}")
                print(f"User_created_at: {status.user.created_at}")
                print(f"Created_at: {status.created_at}")
                print(f"Location: {status.user.location}")
                print(f"Coordinates: {status.coordinates}")
                print(f"Followers: {status.user.followers_count}")
                print(f"ID_tweet: {status.id_str}")
                print(f"Retweet_count: {status.retweet_count}")
                print("----------------------------------------------")
                tweets = [
                    (
                        status.retweeted_status.text, 
                        sentiment, 
                        status.user.screen_name, 
                        status.user.name, 
                        status.user.id, 
                        status.user.created_at, 
                        status.created_at, 
                        status.user.location, 
                        status.user.followers_count, 
                        status.id_str
                    )
                ]

                db.insert_data("retweets", tweets)

        else:
            try:
                print(status.extended_tweet["full_text"])
                sentiment = tweet_analyzer.analyze_sentiment(status.extended_tweet["full_text"])
                print(f"Sentiment: {sentiment}")
                print(f"User: {status.user.screen_name}")
                print(f"User_name: {status.user.name}")
                print(f"User_id: {status.user.id}")
                print(f"User_created_at: {status.user.created_at}")
                print(f"Created_at: {status.created_at}")
                print(f"Location: {status.user.location}")
                print(f"Coordinates: {status.coordinates}")
                print(f"Followers: {status.user.followers_count}")
                print(f"ID_tweet: {status.id_str}")
                print(f"Retweet_count: {status.retweet_count}")
                print("----------------------------------------------")
                tweets = [
                    (
                        status.extended_tweet["full_text"], 
                        sentiment, 
                        status.user.screen_name, 
                        status.user.name, 
                        status.user.id, 
                        status.user.created_at, 
                        status.created_at, 
                        status.user.location, 
                        status.user.followers_count, 
                        status.id_str
                    )
                ]

                db.insert_data("tweets", tweets)


            except AttributeError:
                print(status.text)
                sentiment = tweet_analyzer.analyze_sentiment(status.text)
                print(f"Sentiment: {sentiment}")
                print(f"User: {status.user.screen_name}")
                print(f"User_name: {status.user.name}")
                print(f"User_id: {status.user.id}")
                print(f"User_created_at: {status.user.created_at}")
                print(f"Created_at: {status.created_at}")
                print(f"Location: {status.user.location}")
                print(f"Coordinates: {status.coordinates}")
                print(f"Followers: {status.user.followers_count}")
                print(f"ID_tweet: {status.id_str}")
                print(f"Retweet_count: {status.retweet_count}")
                print("----------------------------------------------")

                tweets = [
                    (
                        status.text,
                        sentiment, 
                        status.user.screen_name, 
                        status.user.name, 
                        status.user.id, 
                        status.user.created_at, 
                        status.created_at, 
                        status.user.location, 
                        status.user.followers_count, 
                        status.id_str
                    )
                ]

                db.insert_data("tweets", tweets)

          
    def on_error(self, status):
        if status == 420:
            # Returning False on_data method in case rate limit occurs.
            print('❌ Limite alcanzado, cerrando el stream({})'.format(self.lead_keyword))
            self.telegram_alert.telegram_alert('❌ Limite alcanzado, cerrando el stream({})'.format(self.lead_keyword))
            return False
        print('❌ Error en streaming (código de error {})'.format(status))
        self.telegram_alert.telegram_alert('❌ Error en streaming (código de error {})'.format(status))

    def on_disconnect(self, status):
        self.telegram_alert.telegram_alert('❌ Desconectado de Twitter '.format(status))

class TweetAnalyzer():

    def clean_tweet(self, tweet):
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", str(tweet)).split())

    def analyze_sentiment(self, tweet):
        analysis = TextBlob(self.clean_tweet(tweet))

        if analysis.sentiment.polarity > 0:
            return "Positivo"
        elif analysis.sentiment.polarity == 0:
            return "Neutral"
        else:
            return "Negativo"

    def tweets_to_data_frame(self, tweets):

        df = pd.DataFrame(data=[tweet.text for tweet in tweets], columns=['tweets'])

        df['id'] = np.array([tweet.id for tweet in tweets])
        df['len'] = np.array([len(tweet.text) for tweet in tweets])
        df['date'] = np.array([tweet.created_at for tweet in tweets])
        df['source'] = np.array([tweet.source for tweet in tweets])
        df['likes'] = np.array([tweet.favorite_count for tweet in tweets])
        df['retweets'] = np.array([tweet.retweet_count for tweet in tweets])

        return df

class TwitterClientAlerts():
    
    def telegram_alert(self, message):
        telegram_send.send(messages=[message])

if __name__ == '__main__':

    print(""" 
        a) Data completa
        b) Streaming de datos
        c) Análisis de publicaciones
        d) Análisis de likes en gráfico
        e) Análisis de sentimientos
    """)

    question = input("Elige una opción: ").lower()

    if question == "a":
        twitter_client = TwitterClient('MaruCampos_G')
        print(twitter_client.get_user_timeline_tweets(1))

    if question == "b":

        hash_tag_list = []
        arch = open('queries.txt', 'r')

        for linea in arch:
            text = linea=linea.strip().replace('\n', '')
            hash_tag_list.append(text)

        arch.close()

        twitter_streamer = TwitterStreamer()
        twitter_streamer.stream_tweets(hash_tag_list)

    if question == "c":
        twitter_user = input("Ingrese la cuenta del usuario: ")
        tweets_number = input("Ingrese la cantidad de tweets a obtener: ")
        tweet_analyzer = TweetAnalyzer()
        api = TwitterClient().get_twitter_client_api()
        tweets = api.user_timeline(screen_name=twitter_user, count=int(tweets_number))
        df = tweet_analyzer.tweets_to_data_frame(tweets)
        
        print(df)

    if question == "d":
        twitter_user = input("Ingrese la cuenta del usuario: ")
        tweets_number = input("Ingrese la cantidad de tweets a obtener: ")
        tweet_analyzer = TweetAnalyzer()
        api = TwitterClient().get_twitter_client_api()
        tweets = api.user_timeline(screen_name=twitter_user, count=int(tweets_number))
        df = tweet_analyzer.tweets_to_data_frame(tweets)

        # Get average length over all tweets:
        print(np.mean(df['len']))

        # Get the number of likes for the most liked tweet:
        print(np.max(df['likes']))

        # Get the number of retweets for the most retweeted tweet:
        print(np.max(df['retweets']))


        time_likes = pd.Series(data=df["likes"].values, index=df["date"])
        time_likes.plot(figsize=(16, 4), label="likes", legend=True)
        time_retweets = pd.Series(data=df["retweets"].values, index=df["date"])
        time_retweets.plot(figsize=(16, 4), label="retweets", legend=True)

        plt.show()

    if question == "e":
        twitter_user = input("Ingrese la cuenta del usuario: ")
        tweets_number = input("Ingrese la cantidad de tweets a obtener: ")
        twitter_client = TwitterClient()
        tweet_analyzer = TweetAnalyzer()

        api = twitter_client.get_twitter_client_api()

        tweets = api.user_timeline(screen_name=twitter_user, count=tweets_number)

        df = tweet_analyzer.tweets_to_data_frame(tweets)
        df['sentiment'] = np.array([tweet_analyzer.analyze_sentiment(tweet) for tweet in df['tweets']])

        print(df.head(10))


    else:
        print("Ha ingresado una respuesta incorrecta")

    
    
