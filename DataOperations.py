__author__ = 'darko'
import pymongo
import codecs
import json

def save_json(filename, data):
    name = 'resources/{0}.json'.format(filename)
    f = open(name, 'w+', encoding='utf-8')
    f.write((json.dumps(data, ensure_ascii=False)))
    return

def append_arr_to_file(filename, data):
    name = 'resources/{0}.txt'.format(filename)
    with codecs.open(name, "a+", encoding="utf-8") as f:
        for w in data:
            f.write(w + " ")
        f.write("\n")
    return

def append_to_file(filename, data):
    name = 'resources/{0}.txt'.format(filename)
    with codecs.open(name, "a+", encoding="utf-8") as f:
        f.write(data + "\n")
    return



def read_json(filename):
    name = 'resources/{0}.json'.format(filename)
    f = open(name, 'r', encoding='utf-8')
    return f.read()


def save_to_mongo(data, mongo_db, mongo_db_coll, **mongo_conn_kw):
    # Connects to the MongoDB server running on
    # localhost:27017 by default
    client = pymongo.MongoClient(**mongo_conn_kw)
    # Get a reference to a particular database
    db = client[mongo_db]
    # Reference a particular collection in the database
    coll = db[mongo_db_coll]
    # Perform a bulk insert and return the IDs
    return coll.insert(data)

def load_from_mongo(mongo_db, mongo_db_coll, return_cursor=False,
    criteria=None, projection=None, **mongo_conn_kw):

    client = pymongo.MongoClient(**mongo_conn_kw)
    db = client[mongo_db]
    coll = db[mongo_db_coll]
    if criteria is None:
        criteria = {}
    if projection is None:
        cursor = coll.find(criteria)
    else:
        cursor = coll.find(criteria, projection)
    # Returning a cursor is recommended for large amounts of data
    if return_cursor:
        return cursor
    else:
        return [ item for item in cursor ]

