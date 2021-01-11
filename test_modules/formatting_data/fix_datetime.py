import bson
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import CursorNotFound, ServerSelectionTimeoutError
import os
import sys
import gc
from datetime import datetime


class FixDateTime:
    def __init__(self):
        mongourl = "mongodb://admin:adminpassword@localhost:27017"
        self.MONGO_CLIENT = MongoClient(mongourl)
        self.QUERY = {"discoverDate": {"$type": 2}}

    def db_news_extraction(self):
        name_coll = "article_en"
        collection = self.MONGO_CLIENT["news"][name_coll]
        docs_to_fix = collection.find(self.QUERY, no_cursor_timeout=True)
        return collection, docs_to_fix

    def process_doc(self, doc):
        # Funziona SOLO con il formato di date che abbiamo sul database
        doc["discoverDate"] = datetime.strptime(doc["discoverDate"], "%Y-%m-%dT%H:%M:%S.%f%z")
        return doc, None

    def db_news_update(self, collection, doc):
        query = {"_id": doc["_id"]}
        new_values = {"$set": {"discoverDate": doc["discoverDate"]}}
        collection.update_one(query, new_values)

    def main(self):
        stop = False
        while not stop:
            collection, docs_to_fix = self.db_news_extraction()
            i = 0
            try:
                for doc in docs_to_fix:
                    if i % 100 == 0:
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
    fix_documents = FixDateTime()
    fix_documents.main()
