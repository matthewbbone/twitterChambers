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

def get_user_tweets(file, user_dict):
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
        tweet_list = tweet_list + get_user_tweets(chunk, user_dict)
    
    return pd.DataFrame(tweet_list)

def extract_tweets(file):
    '''
    file: filepath for jsonl from hydrator app
    user_file: list of users whom you want to get tweets from
    output: list of dictionaries with minimal
    '''

    tweet_list = []

    with open(file) as f:

        for line in f:
            tweet = json.loads(line)

            if not 'retweeted_status' in tweet and not tweet['is_quote_status']:
                temp = {}
                # tweet level data
                temp['id'] = tweet['id_str']
                temp['text'] = tweet['full_text']
                temp['hashtags'] = len(tweet['entities']['hashtags']) \
                    if 'hashtags' in tweet['entities'] else 0
                temp['mentions'] = len(tweet['entities']['user_mentions']) \
                    if 'user_mentions' in tweet['entities'] else 0
                temp['urls'] = len(tweet['entities']['urls']) \
                    if 'urls' in tweet['entities'] else 0
                temp['media'] = len(tweet['entities']['media']) \
                    if 'media' in tweet['entities'] else 0
                temp['symbols'] = len(tweet['entities']['symbols']) \
                    if 'symbols' in tweet['entities'] else 0
                temp['polls'] = len(tweet['entities']['polls']) \
                    if 'polls' in tweet['entities'] else 0
                temp['retweets'] = tweet['retweet_count']
                temp['favorites'] = tweet['favorite_count']
                temp['sensitive'] = tweet['possibly_sensitive'] \
                    if 'possibly_sensitive' in tweet else 'False'

                # user level data
                temp['user_id'] = tweet['user']['id_str']
                temp['user_name'] = tweet['user']['screen_name']
                temp['user_followers'] = tweet['user']['followers_count']
                temp['user_friends'] = tweet['user']['friends_count']
                temp['user_created_at'] = tweet['user']['created_at']
                temp['user_favorites'] = tweet['user']['favourites_count']
                temp['user_verified'] = tweet['user']['verified']
                temp['user_tweets'] = tweet['user']['statuses_count']

                tweet_list.append(temp)
    
    return tweet_list

def get_all_tweets(chunk_files):

    tweet_list = []
    for chunk in chunk_files:
        tweet_list = tweet_list + extract_tweets(chunk)
    
    return pd.DataFrame(tweet_list)

