import pandas as pd
import json
from dateutil import parser

def extract_hydrated_retweets(file):
    '''
    file: filepath for jsonl from hydrator app
    output: list of dictionaries with minimal
    '''

    retweet_list = []

    with open(file) as f:

        for line in f:
            tweet = json.loads(line)
            try:
                status = tweet['retweeted_status']
                temp = {}
                temp['id'] = str(tweet['id_str'])
                temp['text'] = tweet['full_text']
                temp['influencer'] = status['user']['screen_name']
                temp['author_id'] = str(tweet['user']['id'])
                temp['author_name'] = tweet['user']['screen_name']
                temp['author_followers'] = tweet['user']['followers_count']
                temp['infl_id'] = str(status['user']['id'])
                temp['infl_followers'] = status['user']['followers_count']
                temp['infl_verified'] = status['user']['verified']
                temp['infl_total'] = status['user']['statuses_count']
                temp['infl_begin'] = parser.parse(status['user']['created_at'])
                retweet_list.append(temp)
            except:
                continue
        
    return retweet_list

def get_retweet_network(chunk_files):

    retweet_list = []
    for chunk in chunk_files:
        retweet_list = retweet_list + extract_hydrated_retweets(chunk)
    
    return pd.DataFrame(retweet_list)

def get_hydrated_tweets(file, user_dict):
    '''
    file: filepath for jsonl from hydrator app
    user_file: list of users whom you want to get tweets from
    output: list of dictionaries with minimal
    '''

    tweet_list = []

    with open(file) as f:

        for line in f:
            try:
                # checks that the tweet belongs to user per time period
                tweet = json.loads(line)
                author_id = str(tweet['user']['id'])
                date = parser.parse(tweet['created_at']).date()
                assert author_id in user_dict.keys()
                assert str(f"{date.year}-{date.month}") in user_dict[author_id]
                # checks to ensure the tweet is original
                status = tweet['retweeted_status']
                continue
            except KeyError as e:
                if not tweet['is_quote_status']:
                    temp = {}
                    temp['id'] = str(tweet['id_str'])
                    temp['text'] = tweet['full_text']
                    temp['author_id'] = author_id
                    temp['author_name'] = tweet['user']['screen_name']
                    temp['followers'] = tweet['user']['followers_count']
                    temp['verified'] = tweet['user']['verified']
                    temp['total'] = tweet['user']['statuses_count']
                    temp['begin'] = parser.parse(tweet['user']['created_at'])
                    tweet_list.append(temp)
            except AssertionError as e:
                continue
    
    return tweet_list

def get_tweets(chunk_files, user_dict):

    tweet_list = []
    for chunk in chunk_files:
        tweet_list = tweet_list + get_hydrated_tweets(chunk, user_dict)
    
    return pd.DataFrame(tweet_list)

