__author__ = 'darko'
from nltk.corpus import stopwords
from nltk import word_tokenize
import re

class TextProcesor:

    name = "Darko"
    specialChars = [".",",","!","?","@","#",":"]

    def removeStopwords(self, word_list):
        """
        Accepts array of words
        and filters out english stopwords
        :return:
        """
        filtered_words = [w for w in word_list if not w in stopwords.words('english')]
        return filtered_words

    def splitToTokens(self, text):
        return word_tokenize(text)

    def removeSpecialChars(self, text):
        return re.sub('[^a-zA-Z0-9\n]', ' ', text)

    def removeSingles(self, text_arr):
        result = []
        for token in text_arr:
            if len(token) > 1 and str.lower(str(token)) != "rt" and isinstance(token, basestring):
                result.append(str(token))
        return result