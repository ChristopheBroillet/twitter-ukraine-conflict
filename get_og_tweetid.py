import matplotlib.pyplot as plt
import networkx as nx
from pathlib import Path
import tweepy
import os
import pandas as pd
import time
from dotenv import load_dotenv
load_dotenv("./.env.local")
BEARER_TOKEN = os.getenv("BEARER_TOKEN1")
# https://docs.tweepy.org/en/stable/client.html
client = tweepy.Client(BEARER_TOKEN, wait_on_rate_limit=True)


def get_tweets(ids):
    tweets = client.get_tweets(
        ids=ids, expansions=['referenced_tweets.id', 'referenced_tweets.id.author_id'])
    return tweets


def get_original_tweets_ids(ids):
    # convert ids to list
    ids_list = ids["tweetid"].tolist()
    tweets = get_tweets(ids_list)
    # create df with columns original_tweet_id and author_id
    return_df = pd.DataFrame(columns=["author_id", "original_tweet_id"])
    if not tweets.data:
        return None, False
    for tweet in tweets.data:
        referenced_tweet = tweet.get('referenced_tweets')
        original_author = tweet.get('author_id')
        original_tweet_id = referenced_tweet[0].id if referenced_tweet else tweet.id
        # concat to df
        return_df = pd.concat([return_df, pd.DataFrame({"author_id": [
                              original_author], "original_tweet_id": [original_tweet_id]})], ignore_index=True)
    return_df['Unnamed: 0'] = ids['Unnamed: 0']
    return return_df, True


# By batch of 100 Tweets, get the original Tweet ID
for idx, file in enumerate(Path("csvdataframes").iterdir()):
    print(file)
    df = pd.read_csv(file)
    # temp df to store original tweet ids
    temp_df = pd.DataFrame()
    # split df by 100
    for i in range(0, len(df), 100):
        print(f"{file} Iteration {i}-{i+100} / {len(df)}")
        df_100 = df[i:i+100]
        original_tweet_ids_df, empty_check = get_original_tweets_ids(df_100)
        if not empty_check:
            continue
        # concat temp_df and original_tweet_ids_df
        temp_df = pd.concat(
            [temp_df, original_tweet_ids_df], ignore_index=True)
    # Merge temp_df with df
    if temp_df.empty:
        continue
    df = pd.merge(df, temp_df, on="Unnamed: 0")
    # save df to csv
    df.to_csv(Path(f"csvdataframes_wOgIds/day_{idx}.csv"), index=False)
