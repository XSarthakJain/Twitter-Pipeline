import pandas as pd
import json
from datetime import datetime
import tweepy
import config
from datetime import timedelta
from Airflow import DAG
from pythonoperator import PythonOperator
from google.cloud import storage

default_args = {
    'owner':'sarthak Jain',
    'start_date':datetime('2023,06,06'),
    'retries':5,
    'retry-delay':timedelta(minutes=5)
}

Twitter_dag = DAG('twitterPipeline',default_args=default_args,description='twittet data',catch_up=False,schedule_interval='* * * * *',tags=['pipeline','Twitter'])

def twiiter_func():
    consumer_key = ""
    consumer_secret = ''
    access_token = ""
    access_token_secret = ""

    # Authentication
    auth = tweepy.OAuthHandler(access_token,access_token_secret)
    auth.set_access_token(consumer_key,consumer_secret)

    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@xsarthakjain',count=200,include_rts=False,tweet_mode='extended')
    
    twitter_list = []
    for tweet in tweets:
        text = tweet._json('full_text')

        refined_tweet = {
            'user':tweet.user.screen_name,
            'text':text,
            'favourite_count':tweet.favourite_count,
            'retweet_count':tweet.retweet_count,
            'create_date':tweet.created_at
        }
        twitter_list.append(refined_tweet)
    df = pd.DataFrame(twitter_list)
    save_to_Bucket(df)


def save_to_Bucket(data):
    client = storage.Client()
    bucket = client.getBucket('BucketNameHere')
    blob = bucket.blob('dagFile.csv')
    blob.upload_from_string(data)



first_task = PythonOperator(task_id='first_Process',python_callable=twiiter_func,dag=Twitter_dag)

first_task