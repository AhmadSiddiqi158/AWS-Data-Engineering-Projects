import tweepy
import pandas as pd
import s3fs
import json

def twitter_extract_transfer_load():

    CONSUMER_KEY = 'DifQjLJgOsao5n3IRLU8rHdZo'
    CONSUMER_SECRET = 'fI2ynhVOPUYQLkbk796st0QIk2olaTJlj9acoB7mY4k2O4RWg0'

    BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAMsnVwEAAAAANtYbznTle6ND1vCtAfgx7jQaOe4%3DyXxNbbBgLn0KaPtS3a45PWCUgat5BFCo0MNZRbzTU0NR7BV4Zl'
    ACCESS_TOKEN = '1459941043655397376-Ngzd6AJXujvjRy1paq99oGtDtBu61x'
    ACCESS_TOKEN_SECRET = 'ePTBgMYOUujnQY5T3PRunMG482tGZajgZxP0myjQGJE4b'

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)


    api = tweepy.API(auth)

    tweets = api.user_timeline(
        screen_name='@elonmusk',
        count='150',
        include_rts= False
    )

    tweet_list = []

    for tweet in tweets:
        cleaned_tweet = {
            "user": tweet.user.screen_name,
            'text': tweet.text,
            'favourite_count': tweet.favorite_count,
            'retweet_count' : tweet.retweet_count,
            'created_at' : tweet.created_at
        }
        
        tweet_list.append(cleaned_tweet)

    df = pd.DataFrame(tweet_list)

    df.to_csv('s3://airflow-twitter-bucket-1/elon_tweets.csv', index=False)

