__author__ = 'darko'
import DataOperations

class MyCorpus(object):
    def __iter__(self):
        for cursor in DataOperations.load_from_mongo('users_crawl', 'users_vectors', True):
        # assume there's one document per line, tokens separated by whitespace
            yield cursor['vector']

