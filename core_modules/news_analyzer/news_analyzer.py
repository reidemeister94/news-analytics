import networkx as nx
import numpy as np
from pymongo import MongoClient
from nltk.tokenize.punkt import PunktSentenceTokenizer
from sklearn.feature_extraction.text import TfidfTransformer, CountVectorizer
from transformers import TFBertModel, BertTokenizer
import tensorflow as tf
import yaml
import logging
import os
from pprint import pprint
import math
import sys
import time


class NewsAnalyzer:
    def __init__(self, CONFIG=None):
        if CONFIG is not None:
            self.CONFIG = CONFIG
        else:
            with open("configuration/configuration.yaml") as f:
                self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.CLIENT = MongoClient(self.CONFIG["mongourl"])
        self.LOGGER = self.__get_logger()
        self.BERT_TOKENIZER = BertTokenizer.from_pretrained("bert-base-multilingual-cased")
        self.BERT_MODEL = TFBertModel.from_pretrained("bert-base-multilingual-cased")
        self.LOGGER.info("=" * 120)
        self.LOGGER.info("Bert client ready")

    def text_rank(self, document):
        sentence_tokenizer = PunktSentenceTokenizer()
        sentences = sentence_tokenizer.tokenize(document)
        bow_matrix = CountVectorizer().fit_transform(sentences)
        normalized = TfidfTransformer().fit_transform(bow_matrix)
        similarity_graph = normalized * normalized.T
        nx_graph = nx.from_scipy_sparse_matrix(similarity_graph)
        scores = nx.pagerank(nx_graph)
        return sorted(((scores[i], s) for i, s in enumerate(sentences)), reverse=True)

    # def scoring(self, first_doc, second_doc):
    #     # cosine similarity between two encodings
    #     cosine = np.dot(first_doc, second_doc) / (
    #         np.linalg.norm(first_doc) * np.linalg.norm(second_doc)
    #     )
    #     return 1 / (1 + math.exp(-100 * (cosine - 0.95)))

    # def similarity_pair(self, pair):
    #     # first, extract two not analyzed sentences from db (their encodings)
    #     # then, return their similarity
    #     first_doc = None
    #     second_doc = None
    #     similarity = self.scoring(first_doc, second_doc)

    def encode_news(self, doc):
        # print("encode news started")
        try:
            text_rank = self.text_rank(doc["text"])
            # print("text rank finished")
            if len(text_rank) > 0:
                main_phrase = ""
                for i in range(min(3, len(text_rank))):
                    main_phrase += text_rank[i][1] + " "

                input_ids = tf.convert_to_tensor(
                    [self.BERT_TOKENIZER.encode(main_phrase, add_special_tokens=True)]
                )
                last_hidden_states = self.BERT_MODEL(input_ids)[0]
                res = tf.reduce_mean(last_hidden_states, axis=1)

                final_res = []
                for elem in res.numpy()[0]:
                    final_res.append(float(elem))
                return final_res

            else:
                return []
        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            # print("{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno))
            self.LOGGER.error(
                "{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno)
            )

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("NewsAnalyzer")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "core_modules/log/news_analyzer.log"
        if not os.path.isdir("core_modules/log/"):
            os.mkdir("core_modules/log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger


if __name__ == "__main__":
    news_analyzer = NewsAnalyzer()
    doc = "text"
    enc = news_analyzer.encode_news(doc)
    print(type(enc))
