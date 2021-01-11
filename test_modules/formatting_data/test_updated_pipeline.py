import bson
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import CursorNotFound, ServerSelectionTimeoutError
from core_modules.topic_extraction.nlp_utils import NLPUtils
from core_modules.topic_extraction.lda_module import LdaModule
from core_modules.named_entity_recognition.named_entity_recognition import (
    NamedEntityRecognition,
)
import os
import logging
import yaml
import sys
import gc

"""
4 Sept 2020
FATTA ANDARE COME TEST IN LOCALE SULLA COLLEZIONE TEDESCA
**NON** FAR ANDARE ALTROVE
"""


class SimplePipeline:
    def __init__(self):
        # init
        with open("configuration/configuration.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.LOGGER = self.__get_logger()
        mongourl = self.CONFIG["mongourl"]
        self.MONGO_CLIENT = MongoClient(mongourl)
        self.named_entity_recognition = None
        self.lda_module = None
        self.nlp_utils = None
        self.batch_size = 0
        self.batch_docs = []
        self.QUERY = {
            "$or": [{"processedEncoding": False}, {"processedEncoding": {"$exists": False}}]
        }

    def db_news_extraction(self, lang):
        if lang != "it":
            name_coll = "article_" + lang
        else:
            name_coll = "article"
        collection = self.MONGO_CLIENT["news"][name_coll]
        not_processed_docs = collection.find(self.QUERY, no_cursor_timeout=True)
        # not_processed_docs = collection.find(
        #     {"_id": ObjectId("5e7ceeb9dab3970e51e924a8")}, no_cursor_timeout=True,
        # )
        return collection, not_processed_docs

    def process_doc(self, doc, current_lang, update_model=False):
        doc["text"] = doc["text"][:10000]  # temp fix ^ 2

        # topic extraction phase
        try:
            doc = self.topic_extraction(doc, update_model)
        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error(
                "{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno)
            )
            return None, "error"
        gc.collect()

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
        gc.collect()

        return doc, None

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
        document_topic_info = []  # old: {}
        for el in self.lda_module.model[self.lda_module.dictionary.doc2bow(parsed_text)]:
            # str(el[0]) -> key -> topic number
            # float(round(el[1], 2)) -> topic probability
            # self.format_topic_list(topics[el[0]][1]) -> tokens list and contibutions
            new_entry = {}
            new_entry["topic_number"] = str(el[0])
            new_entry["topic_prob"] = float(round(el[1], 2))
            temp_topic_list = self.format_topic_list(topics[el[0]][1])
            tokens_list = []
            for token in temp_topic_list:
                new_token = {}
                new_token["token"] = token[0]
                new_token["contrib"] = token[1]
                tokens_list.append(new_token)
            new_entry["topic_tokens"] = tokens_list
            document_topic_info.append(new_entry)

            # document_topic_info[str(el[0])] = [
            #     float(round(el[1], 2)),
            #     self.format_topic_list(topics[el[0]][1]),
            # ]
        doc["topic_extraction"] = document_topic_info
        doc["parsed_text"] = " ".join(word for word in parsed_text)
        if update_model:
            self.lda_module.update_lda_model(self.batch_docs)
        return doc

    def ner_analysis(self, doc):
        ner_data = self.named_entity_recognition.named_entity_recognition_process(doc)
        doc["named_entity_recognition"] = ner_data
        return doc

    def db_news_update(self, collection, doc, empty=False):
        query = {"_id": doc["_id"]}
        if empty:
            newvalues = {
                "$set": {
                    "parsedText": "",
                    "topicExtraction": {},
                    "namedEntityRecognition": {},
                    "bertEncoding": [],
                    "processedEncoding": True,
                    "triplesExtraction": [],
                }
            }
        else:
            newvalues = {
                "$set": {
                    "parsedText": doc["parsed_text"],
                    "topicExtraction": doc["topic_extraction"],
                    "namedEntityRecognition": doc["named_entity_recognition"],
                    "bertEncoding": [],
                    "processedEncoding": True,
                    "triplesExtraction": [],
                }
            }
        collection.update_one(query, newvalues)

    def init_core_modules(self, lang):
        self.lda_module = None
        self.nlp_utils = None
        self.named_entity_recognition = None
        self.lda_module = LdaModule(lang=lang, trained=True)
        self.nlp_utils = NLPUtils(lang=lang)
        self.named_entity_recognition = NamedEntityRecognition(self.nlp_utils.nlp)

    def main(self):
        self.LOGGER.info("=" * 120)
        self.LOGGER.info("STARTED POST PROCESSING")
        # for lang in self.CONFIG["collections_lang"]:
        for lang in self.CONFIG["collections_lang"]:
            self.LOGGER.info("CURRENT COLLECTION: ARTICLE {}".format(lang.upper()))
            self.LOGGER.info("Initializing core modules and extract news from db...")
            self.init_core_modules("de")
            stop = False
            while not stop:
                collection, not_processed_docs = self.db_news_extraction("de")
                not_processed_docs_count = collection.count_documents(self.QUERY)
                self.LOGGER.info("{} Articles to analyze...".format(not_processed_docs_count))
                if not_processed_docs_count < 100:
                    stop = True
                    not_processed_docs.close()
                    self.LOGGER.info(
                        "Less than 100 articles to analyze ({} to be precise), \
                            proceeding to next collection...".format(
                            not_processed_docs_count
                        )
                    )
                    break
                self.LOGGER.info("Core modules initialized and news from db extracted...")
                # print(len(list(not_processed_docs)))
                i = 0
                self.LOGGER.info("Starting processing docs from db...")
                try:
                    for doc in not_processed_docs:
                        if i % 10 == 0 and i > 0:
                            print(i)
                        if i % 1000 == 0 and i > 0:
                            gc.collect()
                        if i % 10000 == 0 and i > 0:
                            self.LOGGER.info("10k Docs processed...")
                        if len(doc["text"]) > 0 and not doc["text"].isspace():
                            if (
                                self.batch_size
                                == self.CONFIG["topic_extraction"]["batch_size"]
                            ):
                                updated_doc, error = self.process_doc(
                                    doc, "de", update_model=True
                                )
                                self.batch_size = 0
                                self.batch_docs.clear()
                            else:
                                updated_doc, error = self.process_doc(
                                    doc, "de", update_model=False
                                )
                            if error is None:
                                self.batch_size += 1
                                self.db_news_update(collection, updated_doc, empty=False)
                                # print("DOC UPDATED TO DB!")
                                i += 1
                        else:
                            self.db_news_update(collection, doc, empty=True)
                except (CursorNotFound, ServerSelectionTimeoutError) as e:
                    print(type(e))
                    self.LOGGER.error("Lost cursor. Retry...")
                not_processed_docs.close()

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("SimplePipeline")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "log/simple_pipeline.log"
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
    simple_pipeline = SimplePipeline()
    simple_pipeline.main()