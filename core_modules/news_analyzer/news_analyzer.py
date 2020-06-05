import networkx as nx
import numpy as np
from pymongo import MongoClient
from nltk.tokenize.punkt import PunktSentenceTokenizer
from sklearn.feature_extraction.text import TfidfTransformer, CountVectorizer
import yaml
import logging
import os
from bert_serving.client import BertClient
from pprint import pprint
import math


class NewsAnalyzer:
    def __init__(self, CONFIG=None):
        if CONFIG is not None:
            self.CONFIG = CONFIG
        else:
            with open("../../configuration/configuration.yaml") as f:
                self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.CLIENT = MongoClient(self.CONFIG["mongourl"])
        self.BC = BertClient(port=5555, port_out=5556, check_version=False)
        self.LOGGER = self.__get_logger()
        print("Bert client ready")

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("NewsAnalyzer")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "../log/news_analyzer.log"
        if not os.path.isdir("../log/"):
            os.mkdir("../log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger

    def text_rank(self, document):
        sentence_tokenizer = PunktSentenceTokenizer()
        sentences = sentence_tokenizer.tokenize(document)
        bow_matrix = CountVectorizer().fit_transform(sentences)
        normalized = TfidfTransformer().fit_transform(bow_matrix)
        similarity_graph = normalized * normalized.T
        nx_graph = nx.from_scipy_sparse_matrix(similarity_graph)
        scores = nx.pagerank(nx_graph)
        return sorted(((scores[i], s) for i, s in enumerate(sentences)), reverse=True)

    def scoring(self, first_doc, second_doc):
        # cosine similarity between two encodings
        cosine = np.dot(first_doc, second_doc) / (
            np.linalg.norm(first_doc) * np.linalg.norm(second_doc)
        )
        return 1 / (1 + math.exp(-100 * (cosine - 0.95)))

    def similarity_pair(self, pair):
        # first, extract two not analyzed sentences from db (their encodings)
        # then, return their similarity
        first_doc = None
        second_doc = None
        similarity = self.scoring(first_doc, second_doc)
        # now add this similarity to db (need to think about what db collection to use)

    def encode_news(self, news):
        ###
        # TODO:
        # extract not processed news and compute the encoding
        # of their main paragraph
        ###
        text_rank = self.text_rank(news)
        main_phrase = " ".join((text_rank[0][1], text_rank[1][1], text_rank[2][1]))
        res = self.BC.encode([main_phrase])
        return list(res)
        ###
        # TODO:
        # update the db, adding the encoded paragraph


if __name__ == "__main__":
    news_analyzer = NewsAnalyzer()
    doc = """With changes taking effect on Wednesday, all 50 states have begun to reopen in at least some way, more than two months after the coronavirus thrust the country into lockdown. But there remain vast discrepancies in how states are deciding to open up, with some forging far ahead of others.
			 Connecticut will be among the last states to take a plunge back to business on Wednesday, when its stay-at-home order lifts and stores, museums and offices are allowed to reopen. But not far away in New Jersey, the reopening has been more limited, with only curbside pickup at retail stores and allowances for certain industries."""
    enc = news_analyzer.encode_news(doc)
    print(type(enc))
    print(enc)
    enc = list(enc)
    print("=" * 100)
    print(type(enc))
    print(enc)
