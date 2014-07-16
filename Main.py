__author__ = 'Darko'

import sys
import time
from urllib.error import URLError
from http.client import BadStatusLine
import twitter
import json
from functools import partial
from sys import maxsize
import stopwords



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

        #Do something useful with the IDS,
        # return friends_ids[:friends_limit]
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


auth_twitter_search = partial(twitter_search, twitter_api)
# results = auth_twitter_search("Android")
# print(pp(results))

# sample usage

# q="CrossFit"


# statuses = twitter_search(twitter_api, q)
#
# (screen_names, hashtags, urls, media, symbols) = extract_twitter_entities(statuses)

#od site gi printame prvite pet
# print(pp(screen_names[0:5]))
# print(pp(hashtags[0:5]))
# print(pp(urls[0:5]))
# print(pp(media[0:5]))
# print(pp(symbols[0:5]))

friends_ids = get_friends_followers_ids(twitter_api,
                                        screen_name="KolevD",
                                        friends_limit=10,
                                        followers_limit=10)

first_friend = friends_ids[0]

# print(friends_ids)

tweets = harvest_user_timeline(twitter_api, user_id=first_friend
                               , max_results=200)
print(pp(tweets))

