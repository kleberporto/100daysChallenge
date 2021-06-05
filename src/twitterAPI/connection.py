import os
import tweepy as tw
import pandas as pd

consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

# Define the search term and the date_since date as variables
search_words = ""
date_since = "2021-05-04"

# Collect tweets
tweets = tw.Cursor(api.search,
                   q=search_words,
                   lang="pt-br",
                   since=date_since).items(10)

# Iterate and print tweets
for tweet in tweets:
    print('*' * 30)
    print(tweet.text)
