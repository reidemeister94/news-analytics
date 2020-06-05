import sys

sys.path.append("../")
import dateparser
from pymongo import MongoClient
from core_modules.topic_extraction.nlp_utils import NLPUtils
from core_modules.topic_extraction.lda_module import LdaModule
from core_modules.named_entity_recognition.named_entity_recognition import (
    NamedEntityRecognition,
)
from core_modules.news_analyzer.news_analyzer import NewsAnalyzer
from scraping.news_scraper import NewsScraper
import os
import time
import json
from pprint import pprint
import requests
import logging
import yaml


class NewsPostProcess:
    def __init__(self):
        with open("../configuration/configuration.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.LOGGER = self.__get_logger()
        mongourl = "mongodb://localhost:27017/"
        self.MONGO_CLIENT = MongoClient(mongourl)
        self.news_json = None
        self.news_analyzer = None
        self.named_entity_recognition = None
        self.lda_module = None
        self.nlp_utils = None
        self.batch_size = 0
        self.batch_docs = []

    def db_news_extraction(self):
        # news extraction from db: there will be extracted
        # all the articles that aren't processed yet
        self.news_analyzer = NewsAnalyzer(self.CONFIG)
        self.named_entity_recognition = NamedEntityRecognition()
        for lang in self.CONFIG["collections_lang"]:
            self.lda_module = LdaModule(lang=lang, trained=True)
            self.nlp_utils = NLPUtils(lang=lang)
            if lang != "it":
                name_coll = "article_" + lang
                last_processed_param = "last_processed_" + lang
            else:
                name_coll = "article"
                last_processed_param = "last_processed"
            collection = self.MONGO_CLIENT["news"][name_coll]
            not_processed_docs = collection.find(
                {
                    "$or": [
                        {"processedEncoding": False},
                        {"processedEncoding": {"$exists": False}},
                    ]
                }
            )
        return collection, not_processed_docs

    def process_doc(self, doc, update_model=False):
        # named entity recognition phase
        doc = self.named_entity_recognition(doc)

        # topic extraction phase
        doc = self.topic_extraction(doc, update_model)

        # bert enconding phase

        return doc

    def topic_extraction(self, doc, update_model):
        parsed_text = self.nlp_utils.parse_text(doc["text"])
        self.batch_docs.append(parsed_text)
        topics = self.lda_module.model.show_topics(
            formatted=False,
            num_topics=self.CONFIG["topic_extraction"]["num_topics"],
            num_words=self.CONFIG["topic_extraction"]["num_words"],
        )
        document_topic_info = {}
        for el in self.lda_module.model[
            self.lda_module.dictionary.doc2bow(parsed_text)
        ]:
            document_topic_info[el[0]] = [round(el[1], 2), topics[el[0]][1]]
        doc["topic_extraction"] = document_topic_info

        if update_model:
            self.lda_module.update_lda_model(self.batch_docs)
        return doc

    def named_entity_recognition(self, doc):
        (
            elem_pos_type,
            labels,
            items,
            most_common_items,
        ) = self.named_entity_recognition.named_entity_recognition_process(doc["text"])
        ner_data = [elem_pos_type, labels, items, most_common_items]
        doc["named_entity_recognition"] = ner_data
        return doc

    def news_analysis(self):
        pass

    def db_news_update(self, collection, doc):
        query = {"id": doc["id"]}
        newvalues = {
            "$set": {
                "topic_extraction": doc["topic_extraction"],
                "named_entity_recognition": doc["named_entity_recognition"],
                "bert_encoding": doc["bert_encoding"],
            }
        }
        collection.update_one(query, newvalues)
        time.sleep(10)

    def main(self):
        # this is the main workflow: here the extraction and processing
        # phases are looped until no other news has to be analyzed
        collection, not_processed_docs = self.db_news_extraction()
        # print(len(list(not_processed_docs)))
        for doc in not_processed_docs:
            if self.batch_size == self.CONFIG["topic_extraction"]["batch_size"]:
                updated_doc = self.process_doc(doc, update_model=True)
                self.batch_size = 0
                self.batch_docs.clear()
            else:
                updated_doc = self.process_doc(doc, update_model=False)
            self.batch_size += 1

    def __stop(self, p, collection):
        is_old_post = collection.find_one({"id_post": p["id_post"]})
        if is_old_post is None and p["timestamp"] >= self.min_date_post:
            return False
        # if p['timestamp'] >= self.min_date_post:
        #     return False
        else:
            return True

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("NewsPostProcess")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "../log/news_post_process.log"
        if not os.path.isdir("../log/"):
            os.mkdir("../log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger


if __name__ == "__main__":
    news_post_process = NewsPostProcess()
    news_post_process.main()
