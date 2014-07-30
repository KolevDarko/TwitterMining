

__author__ = 'Darko'

import json
from gensim import corpora, models, similarities
from functools import partial

pp = partial(json.dumps, indent=1)

documents = ["Human machine interface for lab abc computer applications",
              "A survey of user opinion of computer system response time",
              "The EPS user interface management system",
              "System and human system engineering testing of EPS",
              "Relation of user perceived response time to error measurement",
              "The generation of random binary unordered trees",
              "The intersection graph of paths in trees",
              "Graph minors IV Widths of trees and well quasi ordering",
              "Graph minors A survey"]

def createCorpus():
    stoplist = set('for a of the and to in'.split())
    texts = [[word for word in document.lower().split() if word not in stoplist]
                for document in documents]
    all_tokens = sum(texts, [])
    tokens_once = set(word for word in set(all_tokens) if all_tokens.count(word) == 1)
    texts = [[word for word in text if word not in tokens_once]
          for text in texts]
    dictionary = corpora.Dictionary(texts)
    dictionary.save('resources/deerwester.dict')
    # print(dictionary.token2id)
    new_doc = "Human computer interface"
    new_vec = dictionary.doc2bow(new_doc.lower().split())
    print(new_vec)


def main():
    print("Corpus")
    createCorpus()

if __name__ == "__main__":
    main()
