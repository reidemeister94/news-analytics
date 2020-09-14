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
        self.QUERY = {"$and": [{"triplesExtraction": {"$exists": True}}]}

    def db_news_extraction(self):
        name_coll = "article_en"
        collection = self.MONGO_CLIENT["news"][name_coll]
        docs_to_fix = collection.find(self.QUERY, no_cursor_timeout=True)
        return collection, docs_to_fix

    def process_doc(self, doc):
        # Fixing Triples Extraction data
        # print(doc["triplesExtraction"])

        if (
            doc["triplesExtraction"] != {}
            and doc["triplesExtraction"] != []
            and doc["triplesExtraction"][0] is not None
        ):
            try:
                doc = self.fix_triples(doc)
                # print(doc["triplesExtraction"])
            except Exception:
                exc_type, _, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print("{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno))
                return None, "error"
            gc.collect()

        elif (
            doc["triplesExtraction"] != {}
            and doc["triplesExtraction"] != []
            and doc["triplesExtraction"][0] is None
        ):

            doc["triplesExtraction"] = []

        return doc, None

    def fix_triples(self, doc):

        triples_data = doc["triplesExtraction"][0]
        triples_data_fixed = []

        for triple in triples_data:
            new_entry = {}
            new_entry["subject"] = triple[0]
            new_entry["verb"] = triple[1]
            new_entry["complement"] = triple[2]
            triples_data_fixed.append(new_entry)

        doc["triplesExtraction"] = triples_data_fixed
        return doc

    def db_news_update(self, collection, doc):
        query = {"_id": doc["_id"]}
        new_values = {"$set": {"triplesExtraction": doc["triplesExtraction"]}}
        collection.update_one(query, new_values)

    def main(self):
        stop = False
        while not stop:
            collection, docs_to_fix = self.db_news_extraction()

            i = 0
            try:
                for doc in docs_to_fix:
                    if i % 10 == 0:
                        print(i)
                    if i % 1000 == 0 and i > 0:
                        gc.collect()
                    updated_doc, error = self.process_doc(doc)
                    if error is None:
                        # print("No Error")
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
