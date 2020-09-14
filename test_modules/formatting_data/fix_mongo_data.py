import bson
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import CursorNotFound, ServerSelectionTimeoutError
import os
import yaml
import sys
import gc


class FixDocuments:
    def __init__(self):
        # init
        with open("configuration/configuration.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        mongourl = self.CONFIG["mongourl"]
        self.MONGO_CLIENT = MongoClient(mongourl)
        self.QUERY = {
            "$and": [
                {"namedEntityRecognition": {"$exists": True}},
                {"topicExtraction": {"$exists": True}},
            ]
        }

    def db_news_extraction(self):
        name_coll = "article"
        collection = self.MONGO_CLIENT["news"][name_coll]
        docs_to_fix = collection.find(self.QUERY, no_cursor_timeout=True)
        return collection, docs_to_fix

    def process_doc(self, doc):
        # Fixing NER data
        if doc["namedEntityRecognition"] != {}:
            try:
                doc = self.fix_ner(doc)
            except Exception:
                exc_type, _, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print("{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno))
                return None, "error"
            gc.collect()

        # Fixing Topic Extraction data
        if doc["topicExtraction"] != {}:
            try:
                doc = self.fix_topics(doc)
            except Exception:
                exc_type, _, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print("{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno))
                return None, "error"
            gc.collect()

        return doc, None

    def fix_ner(self, doc):
        ner_data = doc["namedEntityRecognition"]
        ner_data_fixed = []
        for entity in ner_data.keys():
            new_entry = {}
            new_entry["entity_name"] = entity
            new_entry["label"] = ner_data[entity]["label"]
            new_entry["freq"] = ner_data[entity]["freq"]
            ner_data_fixed.append(new_entry)
        doc["namedEntityRecognition"] = ner_data_fixed
        return doc

    def fix_topics(self, doc):
        old_topics = doc["topicExtraction"]
        new_topics = []
        for topic in old_topics.keys():
            new_entry = {}
            new_entry["topic_number"] = topic
            new_entry["topic_prob"] = old_topics[topic][0]
            tokens_list = []
            for token in old_topics[topic][1]:
                new_token = {}
                new_token["token"] = token[0]
                new_token["contrib"] = token[1]
                tokens_list.append(new_token)
            new_entry["topic_tokens"] = tokens_list
            new_topics.append(new_entry)
        doc["topicExtraction"] = new_topics
        return doc

    def db_news_update(self, collection, doc):
        query = {"_id": doc["_id"]}
        new_values = {
            "$set": {
                "topicExtraction": doc["topicExtraction"],
                "namedEntityRecognition": doc["namedEntityRecognition"],
            }
        }
        collection.update_one(query, new_values)

    def main(self):
        stop = False
        while not stop:
            collection, docs_to_fix = self.db_news_extraction()
            # docs_to_fix_count = collection.count_documents(self.QUERY)
            # if docs_to_fix_count < 100:
            #     stop = True
            #     docs_to_fix.close()
            #     self.LOGGER.info(
            #         "Less than 100 articles to analyze ({} to be precise). \
            #         Stopping...".format(
            #             docs_to_fix_count
            #         )
            #     )
            #     break

            i = 0
            try:
                for doc in docs_to_fix:
                    if i % 10 == 0:
                        print(i)
                    if i % 1000 == 0 and i > 0:
                        gc.collect()
                    updated_doc, error = self.process_doc(doc)
                    if error is None:
                        self.db_news_update(collection, updated_doc)
                    i += 1
                stop = True
            except (CursorNotFound, ServerSelectionTimeoutError) as e:
                print(type(e))
                print("Lost cursor. Retry...")
            docs_to_fix.close()


if __name__ == "__main__":
    fix_documents = FixDocuments()
    fix_documents.main()
