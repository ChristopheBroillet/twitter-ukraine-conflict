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


# get_retweeters from 1 id and 1 pagination token
def get_retweeters(id, pagination_token=None):
    print(".", end='', flush=True)
    retweeters = client.get_retweeters(id, pagination_token=pagination_token)
    next_token = retweeters.meta.get('next_token')

    if not retweeters.data:
        return None, None

    # return list of user ids
    retweeter_ids = [retweeter.id for retweeter in retweeters.data]
    return retweeter_ids, next_token


# get_all_retweeters from 1 id
def get_all_retweeters(id):
    retweeter_ids = []
    next_token = None
    while True:
        retweeter_ids_, next_token = get_retweeters(
            id, pagination_token=next_token)
        if retweeter_ids_ is None:
            break
        retweeter_ids += retweeter_ids_
    return retweeter_ids


def get_linkin_users(id, pagination_token=None):
    print(".", end='', flush=True)
    retweeters = client.get_liking_users(id, pagination_token=pagination_token)
    next_token = retweeters.meta.get('next_token')

    if not retweeters.data:
        return None, None

    # return list of user ids
    retweeter_ids = [retweeter.id for retweeter in retweeters.data]
    return retweeter_ids, next_token


def get_all_linkin_users(id):
    retweeter_ids = []
    next_token = None
    while True:
        retweeter_ids_, next_token = get_linkin_users(
            id, pagination_token=next_token)
        if retweeter_ids_ is None:
            break
        retweeter_ids += retweeter_ids_
    return retweeter_ids


def create_retweeters_edgelist():
    print("Creating edgelist for retweets")
    # read csv files from csvdataframes_wOgIds folder
    for idx, file in enumerate(Path("csvdataframes_wOgIds").iterdir()):
        # create empty edgelists df with column retweeter_id and tweet_id
        edgelists_df = pd.DataFrame(columns=["user_id", "author_id"])
        print(file)
        df = pd.read_csv(file)

        for tweetId, author_id in zip(df['original_tweet_id'], df['author_id']):
            print(tweetId, end='')
            retweeter_ids = get_all_retweeters(tweetId)
            # convert to df
            retweeter_ids_df = pd.DataFrame(retweeter_ids, columns=["user_id"])
            # add tweetId to df
            retweeter_ids_df.insert(1, "author_id", author_id)
            # concat edgelists_df and retweeter_ids_df
            edgelists_df = pd.concat(
                [edgelists_df, retweeter_ids_df], ignore_index=True)
            print("Done")
        # save df to csv
        edgelists_df.to_csv(
            Path(f"edgelists/retweeters_{idx}.csv"), index=False)


def create_liking_edgelist():
    print("Creating edgelist for likes")
    # read csv files from csvdataframes_wOgIds folder
    for idx, file in enumerate(Path("csvdataframes_wOgIds").iterdir()):
        # create empty edgelists df with column liker_id and tweet_id
        edgelists_df = pd.DataFrame(columns=["user_id", "author_id"])
        print(file)
        df = pd.read_csv(file)
        for tweetId, author_id in zip(df['original_tweet_id'], df['author_id']):
            print(tweetId, end='')
            liker_ids = get_all_linkin_users(tweetId)
            # convert to df
            liker_ids_df = pd.DataFrame(liker_ids, columns=["user_id"])
            # add tweetId to df
            liker_ids_df.insert(1, "author_id", author_id)
            # concat edgelists_df and liker_ids_df
            edgelists_df = pd.concat(
                [edgelists_df, liker_ids_df], ignore_index=True)
            print("Done")
        # save df to csv
        edgelists_df.to_csv(Path(f"edgelists/liking_{idx}.csv"), index=False)


# Preprocessing the datasets and save them in csv files under csvdataframes folder
print("Creating folders")
Path.mkdir(Path("csvdataframes"), exist_ok=True)
Path.mkdir(Path("edgelists"), exist_ok=True)
Path.mkdir(Path("csvdataframes_wOgIds"), exist_ok=True)
plots_path = Path("graphs")
Path.mkdir(plots_path, exist_ok=True)

# print("Preprocessing the data")
# for idx, day in enumerate(Path("dataset").iterdir()):
#     full_dataset = pd.read_csv(day, compression='gzip')
#     df_en = full_dataset[full_dataset['language'] == 'en']
#     df_en_filteted = df_en[["userid", "tweetid", "text", "hashtags"]]
#     df_no_duplicate = df_en_filteted.drop_duplicates(
#         subset='text', keep='first')
#     df_sampled = df_no_duplicate.sample(10000)
#     df_sampled.to_csv(Path("csvdataframes") / f"day_{idx}.csv")
#     print(".", end='', flush=True)

create_retweeters_edgelist()
# create_liking_edgelist()
