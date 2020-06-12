import bson
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
import subprocess
import yaml
import sys


class NewsPostProcess:
    def __init__(self):
        # init
        with open("configuration/configuration.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.LOGGER = self.__get_logger()
        mongourl = self.CONFIG["mongourl"]
        self.MONGO_CLIENT = MongoClient(mongourl)
        self.news_json = None
        self.news_analyzer = None
        self.named_entity_recognition = None
        self.lda_module = None
        self.nlp_utils = None
        self.batch_size = 0
        self.batch_docs = []

    def db_news_extraction(self, lang):
        if lang != "it":
            name_coll = "article_" + lang
        else:
            name_coll = "article"
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
        # topic extraction phase
        try:
            # print("topic extraction started")
            doc = self.topic_extraction(doc, update_model)
            # print("topic extraction completed")
        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error(
                "{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno)
            )
            return None, "error"

        # bert enconding phase
        try:
            # print("bert encoding started")
            doc = self.news_analysis(doc)
            # print("bert encoding completed")
        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error(
                "{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno)
            )
            return None, "error"

        # named entity recognition phase
        try:
            # print("ner started")
            doc = self.ner_analysis(doc)
            # print("ner completed")
        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error(
                "{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno)
            )
            return None, "error"

        return doc, None

    def news_analysis(self, doc):
        doc["bert_encoding"] = self.news_analyzer.encode_news(doc)
        return doc

    def format_topic_list(self, topics):
        return [(word, float(weight)) for word, weight in topics]

    def topic_extraction(self, doc, update_model):
        parsed_text = self.nlp_utils.parse_text(doc)
        self.batch_docs.append(parsed_text)
        try:
            topics = self.lda_module.model.show_topics(
                formatted=False,
                num_topics=self.CONFIG["topic_extraction"]["num_topics"],
                num_words=self.CONFIG["topic_extraction"]["num_words"],
            )
        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            # print("{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno))
            self.LOGGER.error(
                "{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno)
            )
            raise Exception("Error on lda module show topics")
        document_topic_info = {}
        for el in self.lda_module.model[
            self.lda_module.dictionary.doc2bow(parsed_text)
        ]:
            document_topic_info[str(el[0])] = [
                float(round(el[1], 2)),
                self.format_topic_list(topics[el[0]][1]),
            ]
        doc["topic_extraction"] = document_topic_info
        doc["parsed_text"] = " ".join(word for word in parsed_text)

        if update_model:
            self.lda_module.update_lda_model(self.batch_docs)
        return doc

    def ner_analysis(self, doc):
        ner_data = self.named_entity_recognition.named_entity_recognition_process(doc)
        doc["named_entity_recognition"] = ner_data
        return doc

    def db_news_update(self, collection, doc):
        query = {"_id": doc["_id"]}
        newvalues = {
            "$set": {
                "parsedText": doc["parsed_text"],
                "topicExtraction": doc["topic_extraction"],
                "namedEntityRecognition": doc["named_entity_recognition"],
                "bertEncoding": doc["bert_encoding"],
                "processedEncoding": True,
            }
        }
        collection.update_one(query, newvalues)

    def init_core_modules(self, lang):
        for _ in range(3):
            # print("SPEGNITI")
            subprocess.run(["bert-serving-terminate", "-port", "5555"])
        subprocess.Popen(
            [
                "bert-serving-start",
                "-model_dir",
                self.CONFIG["news_analyzer"]["bert_model_path"],
                "-num_worker=1",
                "-max_seq_len=40",
            ],
            stdout=subprocess.DEVNULL,
        )
        self.news_analyzer = NewsAnalyzer(self.CONFIG)
        self.lda_module = LdaModule(lang=lang, trained=True)
        self.nlp_utils = NLPUtils(lang=lang)
        self.named_entity_recognition = NamedEntityRecognition(self.nlp_utils.nlp)

    def main(self):
        # this is the main workflow: here the extraction and processing
        # phases are looped until no other news has to be analyzed
        self.LOGGER.info("=" * 120)
        self.LOGGER.info("STARTED POST PROCESSING")
        for lang in self.CONFIG["collections_lang"]:
            self.LOGGER.info("CURRENT COLLECTION: ARTICLE {}".format(lang.upper()))
            self.LOGGER.info("Initializing core modules and extract news from db...")
            self.init_core_modules(lang)
            collection, not_processed_docs = self.db_news_extraction(lang)
            self.LOGGER.info("Core modules initialized and news from db extracted...")
            # print(len(list(not_processed_docs)))
            i = 0
            self.LOGGER.info("Starting processing docs from db...")
            for doc in not_processed_docs:
                if i % 10000 == 0:
                    self.LOGGER.info("10k Docs processed...")
                if len(doc["text"]) > 0:
                    if self.batch_size == self.CONFIG["topic_extraction"]["batch_size"]:
                        updated_doc, error = self.process_doc(doc, update_model=True)
                        self.batch_size = 0
                        self.batch_docs.clear()
                    else:
                        updated_doc, error = self.process_doc(doc, update_model=False)
                    if error is None:
                        self.batch_size += 1
                        self.db_news_update(collection, updated_doc)
                        # print("DOC UPDATED TO DB!")
                        i += 1
            subprocess.run(["bert-serving-terminate", "-port=5555"])

    def __stop(self, p, collection):
        # is_old_post = collection.find_one({"id_post": p["id_post"]})
        # if is_old_post is None and p["timestamp"] >= self.min_date_post:
        #    return False
        # if p['timestamp'] >= self.min_date_post:
        #     return False
        # else:
        #    return True
        pass

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("NewsPostProcess")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "log/news_post_process.log"
        if not os.path.isdir("log/"):
            os.mkdir("log/")
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
