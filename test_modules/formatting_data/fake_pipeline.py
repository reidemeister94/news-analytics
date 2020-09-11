import bson
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import CursorNotFound, ServerSelectionTimeoutError
from core_modules.topic_extraction.nlp_utils import NLPUtils
from core_modules.topic_extraction.lda_module import LdaModule
from core_modules.named_entity_recognition.named_entity_recognition import (
    NamedEntityRecognition,
)
from core_modules.news_analyzer.news_analyzer import NewsAnalyzer
from core_modules.triple_extraction.triples_extraction import TripleExtraction
import os
import time
import logging
import yaml
import sys
import gc


class NewsPostProcess:
    def __init__(self):
        # init
        with open("configuration/configuration.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        mongourl = self.CONFIG["mongourl"]
        self.MONGO_CLIENT = MongoClient(mongourl)
        self.news_json = None
        self.news_analyzer = None
        self.nlp_utils = None
        self.triples_extractor = None
        self.batch_size = 0
        self.batch_docs = []
        self.QUERY = {
            "$or": [{"processedEncoding": False}, {"processedEncoding": {"$exists": False}}]
        }
        # self.QUERY = {"_id": ObjectId("5e7ceeb9dab3970e51e924a8")}

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
        triples_extraction_container = []

        # bert enconding phase
        # start_time = time.time()
        try:
            # print("bert encoding started")
            doc, triples_extraction_container = self.news_analysis(doc)
            # print("bert encoding completed")
        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print("{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno))
            return None, "error"
        gc.collect()
        # print("--- %s seconds for bert encoding ---" % (time.time() - start_time))

        # triples extraction phase
        if current_lang == "en":
            # start_time = time.time()
            try:
                # print("triples extraction started")
                triples = self.triples_extraction(triples_extraction_container, doc["_id"])
                doc["triples_extraction"] = triples
                # print("triples extraction completed")
            except Exception as e:
                exc_type, _, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(
                    "{}, {}, {}, {}, {}".format(
                        doc["_id"], exc_type, fname, exc_tb.tb_lineno, str(e)
                    )
                )
                return None, "error"
            gc.collect()
            # print("--- %s seconds for triples extraction ---" % (time.time() - start_time))
        else:
            doc["triples_extraction"] = []
        return doc, None

    def news_analysis(self, doc):
        doc["bert_encoding"], triples_extraction_container = self.news_analyzer.encode_news(
            doc
        )
        return doc, triples_extraction_container

    def triples_extraction(self, triples_extraction_container, doc_id):
        triples = self.triples_extractor.perform_triples_extraction(
            doc_id, triples_extraction_container
        )
        triples_formatted = []
        for t in triples[0]:
            new_entry = {}
            new_entry["subject"] = t[0]
            new_entry["verb"] = t[1]
            new_entry["complement"] = t[2]
            triples_formatted.append(new_entry)
        return triples_formatted

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
                    "parsedText": "",
                    "topicExtraction": [],
                    "namedEntityRecognition": [],
                    "bertEncoding": doc["bert_encoding"],
                    "processedEncoding": True,
                    "triplesExtraction": doc["triples_extraction"],
                }
            }
        collection.update_one(query, newvalues)

    def init_core_modules(self, lang):
        self.news_analyzer = None
        self.nlp_utils = None
        self.triples_extractor = None
        self.news_analyzer = NewsAnalyzer(self.CONFIG, self.MONGO_CLIENT)
        self.nlp_utils = NLPUtils(lang=lang)
        self.triples_extractor = TripleExtraction(self.nlp_utils.nlp)

    def main(self):
        for lang in self.CONFIG["collections_lang"]:
            self.init_core_modules("en")
            stop = False
            while not stop:
                collection, not_processed_docs = self.db_news_extraction("en")
                not_processed_docs_count = collection.count_documents(self.QUERY)
                if not_processed_docs_count < 100:
                    stop = True
                    not_processed_docs.close()
                    break
                i = 0
                try:
                    for doc in not_processed_docs:
                        if i % 10 == 0 and i > 0:
                            print(i)
                        if i % 1000 == 0 and i > 0:
                            gc.collect()

                        if len(doc["text"]) > 0 and not doc["text"].isspace():
                            if (
                                self.batch_size
                                == self.CONFIG["topic_extraction"]["batch_size"]
                            ):
                                updated_doc, error = self.process_doc(
                                    doc, "en", update_model=True
                                )
                                self.batch_size = 0
                                self.batch_docs.clear()
                            else:
                                updated_doc, error = self.process_doc(
                                    doc, "en", update_model=False
                                )
                            if error is None:
                                self.batch_size += 1
                                self.db_news_update(collection, updated_doc, empty=False)
                                # print("DOC UPDATED TO DB!")
                                i += 1
                        # print("--- %s seconds ---" % (time.time() - start_time))
                        else:
                            self.db_news_update(collection, doc, empty=True)
                except (CursorNotFound, ServerSelectionTimeoutError) as e:
                    print(type(e))
                not_processed_docs.close()


if __name__ == "__main__":
    news_post_process = NewsPostProcess()
    news_post_process.main()
