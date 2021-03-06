__author__ = 'Darko'

import sys
import time
from urllib2 import URLError
from httplib import BadStatusLine
import twitter
import json
from functools import partial
from sys import maxsize
import pymongo
import re
import codecs
import TextProcesor
from gensim import corpora, models, similarities
import DataOperations
import MyCorpus
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


def oauth_login():
    CONSUMER_KEY = 'cmbXzHnndGrx7BPhSvk2IbhMm'
    CONSUMER_SECRET = 'u51FPq0GM7wDSPdjnzaWpYdo7qGKB7UDH7tg78GEMDQK9vc3rw'
    OAUTH_TOKEN = '101215787-RAEnVfND8jJeJlSOOtYBKhvLMZo1uKJOoZJg8FPH'
    OAUTH_TOKEN_SECRET = 'YS07Vh9FOMrLJIc8roF7ZYaS0khDI8Mxqq67aKgieTgBr'

    auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)

    twitter_api = twitter.Twitter(auth=auth)
    return twitter_api

twitter_api = oauth_login()
pp = partial(json.dumps, indent=1)


def twitter_search(twitter_api, q, max_results=200, **kw):
    search_results = twitter_api.search.tweets(q=q, count=100, **kw)

    statuses = search_results['statuses']

    # Iterate through batches of results by following the cursor until we
    # reach the desired number of results, keeping in mind that OAuth users
    # can "only" make 180 search queries per 15-minute interval. See
    # https://dev.twitter.com/docs/rate-limiting/1.1/limits
    # for details. A reasonable number of results is ~1000, although
    # that number of results may not exist for all queries.

    # reasonable limit
    max_results = min(1000, max_results)

    for _ in range(10):
        try:
            next_results = search_results['search_metadata']['next_results']
        except KeyError as e:
            break

        # creating a dictionary from next_results, which has the form:
        # ?max_id=313519052523986943&q=NCAA&include_entities=1

        kwargs = dict([kv.split('=') for kv in next_results[1:].split("&")])

        search_results = twitter_api.search.tweets(**kwargs)
        statuses += search_results['statuses']

        if len(statuses) > max_results:
            break

    return statuses

def extract_twitter_entities(statuses):
    if len(statuses) == 0:
        return [], [], [], [], []

    screen_names = [ user_mention['screen_name']
                        for status in statuses
                            for user_mention in status['entities']['user_mentions'] ]

    hashtags = [hashtag['text']
                    for status in statuses
                        for hashtag in status['entities']['hashtags']]

    urls = [url['expanded_url']
                for status in statuses
                    for url in status['entities']['urls']]

    symbols = [symbol['text']
                for status in statuses
                    for symbol in status['entities']['symbols']]

    #moze da ima i da nema media entry
    for status in statuses:
        if 'media' in status['entities']:
            media = [media['url']
                        for status in statuses
                            for media in status['entites']['media']]
        else:
            media=[]

        return screen_names, hashtags, urls, media, symbols

# Section 9.16
def make_twitter_request(twitter_api_func, max_errors=10, *args, **kw):
    def handle_twitter_http_error(e, wait_period=2, sleep_when_rate_limited=True):
        if wait_period > 3600: # Seconds
            print(sys.stderr, 'Too many retries. Quitting.')
            raise e
        # See https://dev.twitter.com/docs/error-codes-responses for common codes
        if e.e.code == 401:
            print(sys.stderr, 'Encountered 401 Error (Not Authorized)')
            return None
        elif e.e.code == 404:
            print(sys.stderr, 'Encountered 404 Error (Not Found)')
        elif e.e.code == 429:
            print(sys.stderr, 'Encountered 429 Error (Rate Limit Exceeded')
            if sleep_when_rate_limited:
                print(sys.stderr, "Retrying in 15 minutes...ZzZ...")
                sys.stderr.flush()
                time.sleep(60*15 + 5)
                print(sys.stderr, '...ZzZ...Awake now and trying again.')
                return 2
            else:
                raise e # Caller must handle the rate limiting issue
        elif e.e.code in (500, 502, 503, 504):
            print(sys.stderr, 'Encountered %iError. Retrying in %iseconds' %\
                (e.e.code, wait_period))
            time.sleep(wait_period)
            wait_period *= 1.5
            return wait_period
        else:
            raise e
    # end of helper error function

    wait_period = 2
    error_count = 0

    while True:
        try:
            return twitter_api_func(*args, **kw)
        except twitter.TwitterHTTPError as e:
            error_count = 0
            wait_period = handle_twitter_http_error(e, wait_period)
            if wait_period is None:
                return
        except URLError as e:
            error_count += 1
            print(sys.stderr, "URLError encountered. Continuing.")
            if error_count > max_errors:
                print(sys.stderr, "Too many consecutive errors...bailing out.")
                raise
        except BadStatusLine as e:
            error_count += 1
            print(sys.stderr, "BadStatusLine encountered. Continuing.")
            if error_count > max_errors:
                print(sys.stderr, "Too many consecutive errors...bailing out.")
                raise


# Section 9.19
def get_friends_followers_ids(twitter_api, screen_name=None, user_id=None,
                              friends_limit=maxsize, followers_limit=maxsize):
    assert (screen_name != None) != (user_id != None), \
    "Must have screen_name XOR user_id"

    # get_friends_ids= partial(make_twitter_request, twitter_api.friends.ids, count=5000)

    friends_ids = []
    # temp_results = make_twitter_request(twitter_api.friends.ids, friends_limit, friends_ids, "friends", count=500)
    # temp_results = get_friends_ids(friends_limit, friends_ids, "friends")
    # for limit, ids, label in [friends_limit, friends_ids, "friends"]:
    label = "friends"
    ids = friends_ids
    limit = friends_limit
    # for twitter_api_func, limit, ids, label in temp_results:
    cursor = -1
    while cursor != 0:
        if screen_name:
            response = make_twitter_request(twitter_api.friends.ids, count=5000, screen_name=screen_name,
                                            cursor=cursor)
        else:
            response = make_twitter_request(twitter_api.friends.ids, count=5000, user_id=user_id, cursor=cursor)

        if response is not None:
            ids += response['ids']
            cursor = response['next_cursor']
        print(sys.stderr, 'Fetched {0} total {1} ids for {2}'.format(len(ids),label,(user_id or screen_name)))

        if len(ids) >= limit or response is None:
            return ids[:friends_limit]
        return ids[:friends_limit]

def harvest_user_timeline(twitter_api, screen_name=None, user_id=None, max_results=1000):
    assert (screen_name!=None) != (user_id != None),\
    "Must have screen_name xor user_id"

    kw = {  # Keyword args for the Twitter API call
            'count': 200,
            'trim_user': 'true',
            'include_rts' : 'true',
            'since_id' : 1
            }
    if screen_name:
        kw['screen_name'] = screen_name
    else:
        kw['user_id']=user_id
    max_pages = 16
    results = []

    tweets = make_twitter_request(twitter_api.statuses.user_timeline, **kw)

    if tweets is None: # znaci nemame avtorizacija
        tweets = []
    results += tweets

    print(sys.stderr, 'Fetched %i tweets' % len(tweets))

    page_num = 1
    if max_results == kw['count']:
        page_num = max_pages # ne vleguvaj vo loop podolu

    while page_num < max_pages and len(tweets) > 0 and len(results) < max_results:
        kw['max_id'] = min([ tweet['id'] for tweet in tweets ]) - 1
        tweets = make_twitter_request(twitter_api.statuses.user_timeline, **kw)
        results += tweets
        print(sys.stderr, 'Fetched %i tweets' % (len(tweets)))
        page_num+=1
    print(sys.stderr, 'Done fetching')

    return results[:max_results]


#Saving json data in text files

def crawl_friends(twitter_api, screen_name, limit=1000, depth=2):
    # in storage
    seed_id = str(twitter_api.users.show(screen_name=screen_name)['id'])

    next_queue = get_friends_followers_ids(twitter_api, user_id=seed_id, friends_limit=limit, followers_limit=0)
    # Store a seed_id => _follower_ids mapping in MongoDB
    DataOperations.save_to_mongo({'followers' : [ _id for _id in next_queue ]}, 'users_crawl', 'users_ids'.format(seed_id))
    d = 1
    while d < depth:
        d += 1
        (queue, next_queue) = (next_queue, [])
        for fid in queue:
            follower_ids = get_friends_followers_ids(twitter_api, user_id=fid,friends_limit=limit,followers_limit=0)
            # Store a fid => follower_ids mapping in MongoDB
            DataOperations.save_to_mongo({'followers' : [ _id for _id in next_queue ]}, 'users_crawl', 'users_ids')
            next_queue += follower_ids

# function that stores users timeline in timelines collection
# argument is user_id
def get_users_friends_from_db():
    friends_ids = DataOperations.load_from_mongo('users_crawl', 'users_ids')
    return friends_ids

def get_users_timeline_from_db():
    uslov = { 'timeline': { '$not': { '$size': 0 } } }
    return DataOperations.load_from_mongo('users_crawl', 'users_timelines', True, criteria=uslov)

def get_urls_from_tweet(tweet):
    urls = []
    indices = []
    text = tweet['text']
    for url in tweet['entities']['urls']:
        indices = url['indices']
        urls.insert(0, text[indices[0]: indices[1]])
    return urls, indices

def remove_entities(text, pattern):
    entities = re.findall(pattern, text)
    original = text
    for url in entities:
        pos = original.find(url)
        res = original[0:pos-1] + original[pos+len(url):]
        original = res
    return original


def combine_users_tweets():
    timelines_cursor = get_users_timeline_from_db()
    urls_pattern = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    i=0
    for tl in timelines_cursor:
        i += 1
        if i < 10:
            continue
        DataOperations.append_to_file("temp", str(tl['user_id']))
        for tweet in tl['timeline']:
            first_text = tweet['text']
            lean_text = first_text

            lean_text.encode('ascii', 'ignore')
            lean_text = remove_entities(lean_text, urls_pattern)
            testObj = TextProcesor.TextProcesor()
            lean_text = testObj.removeSpecialChars(lean_text)

            lean_text_arr = testObj.splitToTokens(lean_text)
            lean_text_arr = testObj.removeStopwords(lean_text_arr)
            lean_text_arr = testObj.removeSingles(lean_text_arr)
            DataOperations.append_arr_to_file("temp", lean_text_arr)
        return
        #       Todo and figure out how to store them in db

def create_dictionary_from_file(filename=None):
    name = 'resources/{0}.txt'.format(filename)
    with codecs.open(name, "r", encoding="utf-8") as f:
        full_text = f.read()
        texts = full_text.split(" ")
        double_list = []
        double_list.append(texts)
        return corpora.Dictionary(double_list)

def save_users_timelines(twitter_api, users_ids):

    for str_id in users_ids:
        id = int(str_id)

        timeline = harvest_user_timeline(twitter_api, user_id=id)
        results = {'timeline': timeline, 'user_id': id}
        # print(json.dumps(results, indent=1))
        DataOperations.save_to_mongo(results, 'users_crawl', 'users_timelines')

def save_single_user_timeline(twitter_api, user_id):
    id = int(user_id)
    timeline = harvest_user_timeline(twitter_api, user_id=id)
    results = {'timeline': timeline, 'user_id': id}
    DataOperations.save_to_mongo(results, 'users_crawl', 'users_timelines')

# TODO: create function for transforming array of tweets into one file


def main():
    print("tester main")
    # append_to_file("temper", "Darko the awesomest")
    # append_to_file("temper", "As always")
    # combine_users_tweets()
    # print(create_dictionary_from_file("temp"))



    my_corpus = MyCorpus.MyCorpus()

    my_dictionary = corpora.Dictionary.load(fname='resources/master_dictionary_filtered.dict')


    # my_dictionary.filter_extremes(no_below=10)
    # my_dictionary.compactify()
    # my_dictionary.save('resources/master_dictionary2.dict')
    #
    # my_dictionary = corpora.Dictionary.load(fname='resources/master_dictionary.dict')
    # my_dictionary.filter_extremes(no_below=10)
    # my_dictionary.save('resources/master_dictionary_filtered.dict')

    # to execute
    my_lda = models.ldamodel.LdaModel(my_corpus, id2word=my_dictionary, num_topics=100,  update_every=1, chunksize=100, passes=20)
    print 'AAAAAALLLLL DONE'
    my_lda.print_topics(20)
    my_lda.save('resources/LdaModel2')


    # new_m = models.LdaModel.load('resources/LdaModel')
    # new_m.print_topics(10)


if __name__ == "__main__":
    main()

# my_user_id='101215787'
# res = get_users_friends_from_db(my_user_id)

# save_users_timelines(twitter_api, res[0]['followers'])
#<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
# print(res[0]['followers'])
# for stringce in res[0]['followers']:
#     print(type(stringce))

# crawl_friends(twitter_api, screen_name, depth=2, limit=10)





#Usage
# q='CrossFit'
#
# results = twitter_search(twitter_api, q, max_results=10)
#
# save_json(q, results)
# rez = read_json(q)
#
# print(pp(rez))

# auth_twitter_search = partial(twitter_search, twitter_api)

# friends_ids = get_friends_followers_ids(twitter_api,
#                                         screen_name="KolevD",
#                                         friends_limit=10,
#                                         followers_limit=10)
#
# first_friend = friends_ids[0]
#
# # print(friends_ids)
#
# tweets = harvest_user_timeline(twitter_api, user_id=first_friend
#                                , max_results=200)
# print(pp(tweets))

