import json
import datetime as dt
import pandas as pd
import numpy as np
import random as rnd
import matplotlib.pyplot as plt
from dateutil import parser
from dateutil.relativedelta import relativedelta, SU
from collections import Counter
import datetime as dt

# dictionary for abbreviating topics
topic_abb = {
    'Global stance': 'Global',
    'Significance of Pollution Awareness Events': 'Awareness',
    'Importance of Human Intervantion': 'Intervention',
    'Politics': 'Politics',
    'Undefined / One Word Hashtags': 'Undefined',
    'Seriousness of Gas Emissions': 'Emissions',
    'Donald Trump versus Science': 'Trump',
    'Weather Extremes': 'Weather',
    'Ideological Positions on Global Warming': 'Ideology',
    'Impact of Resource Overconsumption': 'Consumption'
}

def merge_preprocess_climate_tweets(climatepath, retweetpath, start=None, end=None, retweets=True):
    '''
    climatepath: path to a csv of the Effrosynidis et al.'s climate data
    retweetpath: path to pickle file of retweets
    start: datetime for start of range, take earliest if None
    end: datetime for end of range, take latest if None
    output: climate data and retweets merged on tweet id
    '''

    # loading and merging datasets
    climate_df = pd.read_csv(climatepath)
    retweet_df = pd.read_pickle(retweetpath)

    retweet_df['id'] = retweet_df['id'].map(lambda x: int(x))

    combined_df = retweet_df.merge(climate_df, on='id', how='left')

    # filtering combined data by time range
    combined_df['date'] = combined_df['created_at'].map(lambda x: parser.parse(x).date())

    if start is None and end is None:
        filtered_df = combined_df.copy()
    elif start is None:
        filtered_df = combined_df[combined_df['date'] <= end].copy()
    elif end is None:
        filtered_df = combined_df[combined_df['date'] >= start].copy()
    else:
         filtered_df = combined_df[(combined_df['date'] >= start) & (combined_df['date'] <= end)].copy()

    # preprocessing and creating useful columns
    filtered_df['topic'] = filtered_df['topic'].map(lambda x: topic_abb[x])
    filtered_df['week'] = filtered_df['date'].map(lambda x:  x + relativedelta(weekday=SU))
    filtered_df['month'] = filtered_df['date'].map(lambda x: x.replace(day=1))
    
    # binary grouping where aggressive, male, and denier are grouped in opposition
    filtered_df['aggressive'] = filtered_df['aggressiveness'].map(lambda x: 1 if x=='aggressive' else 0)
    filtered_df['male'] = filtered_df['gender'].map(lambda x: 1 if x=='male' else 0 if x == 'female' else None)
    filtered_df['denier'] = filtered_df['stance'].map(lambda x: 1 if x=='denier' else 0)

    if retweets:
        filtered_df['infl_verified'] = filtered_df['infl_verified'].map(lambda x: 1 if x=='True' else 0)
        filtered_df['infl_freq'] = filtered_df.apply(
            lambda r: r['infl_total'] / (1 + (r['date'] - r['infl_begin'].date()).days),
            axis = 1
        )

        return filtered_df[[
            'id', 'text', 'influencer', 'author_id', 'author_name', 'author_followers',
            'infl_id', 'infl_followers', 'infl_verified', 'infl_freq', 'topic',
            'sentiment', 'denier', 'male', 'aggressive', 'date', 'week', 'month'
        ]]
    else:
        if set(['verified', 'begin', 'total']).issubset(set(filtered_df.columns)):
            filtered_df['verified'] = filtered_df['verified'].map(lambda x: 1 if x=='True' else 0)
            filtered_df['freq'] = filtered_df.apply(
                lambda r: r['total'] / (1 + (dt.datetime(2022,11,30).date() - r['begin'].date()).days),
                axis = 1
            )
            return filtered_df[[
                'id', 'text', 'author_id', 'author_name', 'followers', 'topic', 'verified',
                'freq', 'sentiment', 'denier', 'male', 'aggressive', 'date', 'week', 'month'
            ]]
        else:
            #filtered_df['user_verified'] = filtered_df['user_verified'].map(lambda x: 1 if x=='True' else 0)
            #filtered_df['sensitive'] = filtered_df['sensitive'].map(lambda x: 1 if x=='True' else 0)
            filtered_df['user_freq'] = filtered_df.apply(
                lambda r: r['user_tweets'] / (1 + (dt.datetime(2022,11,30).date() - parser.parse(r['user_created_at']).date()).days),
                axis = 1
            )
            return filtered_df


        

def get_leading_users(retweet_df, timeunit, N, M):
    '''
    retweet_df: dataframe of retweets
    timeunit: the time unit to measure impact at (e.g. day, week, month)
    N: corresponds to Kolic et al., high impact users == top N retweeted per period
    M: corresponds to Kolic et al., leading users == top M persistance over periods
    output: dictionary of retweet count per user per period, and a dictionary of 
        leading users with their persistance 
    '''

    leading_users = []
    impact_history = {}

    for t in retweet_df[timeunit].dropna().unique():

        network_df = retweet_df[retweet_df[timeunit] == t].groupby(['influencer', 'author_name']).agg({'id':'count'})

        network_df = pd.DataFrame({
            'influencer': [row[0] for row in network_df.index],
            'audience': [row[1] for row in network_df.index],
            'n_retweets': network_df['id'].values
        })
        
        impact_df = network_df.groupby('influencer').agg({'n_retweets': 'sum'})
        impact_history[t] = impact_df.to_dict()['n_retweets']
        leading_users += list(impact_df.sort_values('n_retweets').tail(N).index)

    return impact_history, dict(Counter(leading_users).most_common(M))

def get_only_leading_user_retweets(retweet_df, leading_users, timeunit, perc_missing=None):
    '''
    retweet_df: dataframe of retweets
    leading_users: a dictionary of leading users with their persistance 
    timeunit: the time unit to measure impact at (e.g. day, week, month)
    perc_missing: this is the minimal percent missing to retain leading users
    output: a filtered retweet_df based on leading users and minimal persistance
    '''

    leading_filter = [infl in leading_users.keys() for infl in retweet_df['influencer']]
    leading_df = retweet_df[leading_filter].copy()

    if perc_missing is None:
        return leading_df

    n_periods = len(leading_df[timeunit].unique())
    keep_list = [k for k, v in leading_users.items() if v >= int(n_periods * perc_missing)]
    
    return leading_df[leading_df['influencer'].map(lambda x: x in keep_list)].copy()

def get_group_mean(df, col, timeunit, author):

    return df[[timeunit, author, col]] \
            .groupby([timeunit, author]) \
            .agg({f"{col}": np.nanmean})[col].values

def summarize_tweet_data(df, timeunit, type='retweets'):
    '''
    df: a tweet-level dataframe
    timeunit: the time unit to measure impact at (e.g. day, week, month)
    output: a summary dataset with rows for each unique user and time period
    '''

    assert type in ['retweets', 'tweets', 'audience', 'echoer']

    if type in ['retweets', 'audience']:
        author = 'influencer'
    else: author = 'author_name'

    output_df = df[[author, timeunit]] \
                    .drop_duplicates() \
                    .sort_values([timeunit, author]) \
                    .reset_index(drop=True)

    # summarize different columns
    try:
        output_df['capture'] = get_group_mean(df, 'capture', timeunit, author)
    except:
        counts = df.groupby([timeunit, author]).agg({'id': 'count'})
        totals = df.groupby(timeunit).agg({'id': 'count'})

        for row in counts.iterrows():
            counts.loc[row[0]] = counts.loc[row[0]] / totals.loc[row[0][0]]

        output_df['capture'] = list(counts['id'])

    output_df['denier'] = get_group_mean(df, 'denier', timeunit, author)
    output_df['sentiment'] = get_group_mean(df, 'sentiment', timeunit, author)
    output_df['male'] = get_group_mean(df, 'male', timeunit, author)
    output_df['aggressive'] = get_group_mean(df, 'aggressive', timeunit, author)
    
    # most popular topic
    # output_df['topic'] = df.groupby([timeunit, author])['topic'] \
    #                                 .agg(lambda x: rnd.choice(pd.Series.mode(x, dropna=False))).values
    
    if type=='retweets':
        output_df['audience_followers'] = get_group_mean(df, 'author_followers', timeunit, author)
        output_df['followers'] = get_group_mean(df, 'infl_followers', timeunit, author)
        output_df['verified'] = get_group_mean(df, 'infl_verified', timeunit, author)
        output_df['tweet_freq'] = get_group_mean(df, 'infl_freq', timeunit, author)
    else:
        output_df['followers'] = get_group_mean(df, 'followers', timeunit, author)
        output_df['verified'] = get_group_mean(df, 'verified', timeunit, author)
        output_df['freq'] = get_group_mean(df, 'freq', timeunit, author)

    return output_df


def get_audience_dict(leading_df, timeunit):
    '''
    leading_df: a filtered retweet_df based on leading users and minimal persistance
    timeunit: the time unit to measure impact at (e.g. day, week, month)
    output: a dictionary with author_id keys with lists of dates as values for
        quicker extraction of tweets
    '''

    audience_dict = {}
    for row in leading_df.iterrows():
        rowdat = row[1]
        time = f"{rowdat[timeunit].year}-{rowdat[timeunit].month}"
        author_id = str(rowdat['author_id'])
        if not author_id in audience_dict.keys():
            audience_dict[author_id] = [time]
        elif not time in audience_dict[author_id]:
            audience_dict[author_id].append(time)

    return audience_dict

def get_echoer_dict(retweet_df, audience_dict, timeunit):
    '''
    retweet_df: dataframe of retweets
    audience_dict: a dictionary with author_id keys with lists of dates as values
    timeunit: the time unit to measure impact at (e.g. day, week, month)
    output: a dictionary with infl_id keys with lists of dates as values for
        quicker extraction of tweets
    '''

    echoer_dict = {}
    for row in retweet_df.iterrows():
        rowdat = row[1]
        time = f"{rowdat[timeunit].year}-{rowdat[timeunit].month}"
        author_id = str(rowdat['author_id'])
        infl_id = str(rowdat['infl_id'])
        if author_id in audience_dict.keys() and time in audience_dict[author_id]:
            if not rowdat['infl_id'] in echoer_dict.keys():
                echoer_dict[infl_id] = [time]
            elif not time in echoer_dict[rowdat['infl_id']]:
                echoer_dict[infl_id].append(time)
    return echoer_dict

def merge_audience_echoer(audience_df, echoer_df, leading_df, retweet_df, timeunit):

    # summarize audience and echoer data
    audience_summary_df = summarize_tweet_data(
        audience_df,
        timeunit=timeunit,
        type='tweets'
    )

    echoer_summary_df = summarize_tweet_data(
        echoer_df,
        timeunit=timeunit,
        type='tweets'
    )

    # merge full retweet network with echoer summary to map echoer -> audience member 
    retweet_echoer_df = retweet_df[[timeunit, 'influencer', 'author_name']].drop_duplicates() \
        .merge(echoer_summary_df, left_on=[timeunit, 'influencer'], right_on=[timeunit, 'author_name'])
    retweet_echoer_df.rename(columns={'author_name_x': 'author_name'}, inplace=True) 
    retweet_echoer_df.drop('author_name_y', axis=1, inplace=True)

    # inner join merge of audience summary with retweet to summarize and map echoers/audience
    audience_echoer_df = audience_summary_df.merge(retweet_echoer_df, on=[timeunit, 'author_name'])
    audience_echoer_df.columns = [col[:-2] if '_y' in col else col for col in audience_echoer_df.columns]

    audience_echoer_summary_df = summarize_tweet_data(
        audience_echoer_df,
        timeunit=timeunit,
        type='echoer'
    )

    # merge audience and echoer summaries with leading_df for mapping to leading user
    leading_audience_df = leading_df[[timeunit, 'influencer', 'author_name']].drop_duplicates() \
        .merge(audience_summary_df, on=[timeunit, 'author_name'])

    leading_echoer_df = leading_df[[timeunit, 'influencer', 'author_name']].drop_duplicates() \
        .merge(audience_echoer_summary_df, on=[timeunit, 'author_name'])

    # summarize each audience and echoer to match leading user level
    leading_audience_summary_df = summarize_tweet_data(
        leading_audience_df,
        timeunit=timeunit,
        type='audience'
    )

    leading_echoer_summary_df = summarize_tweet_data(
        leading_echoer_df,
        timeunit=timeunit,
        type='audience'
    )

    # merge audience and echoer data on leading user
    meta_df = leading_audience_summary_df.merge(
        leading_echoer_summary_df, 
        on=[timeunit, 'influencer'], 
        how='outer'
    )

    col_map = {'_x': '_audience', '_y': '_echoer'}
    meta_df.columns = [col[:-2] + col_map[col[-2:]] if '_x' in col or '_y' in col else col for col in meta_df.columns]
    
    return meta_df

    





    


    


