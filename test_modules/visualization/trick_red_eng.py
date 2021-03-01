from pymongo import MongoClient
from datetime import datetime
from pymongo.errors import CursorNotFound

import numpy as np
import os


class DimReductionProcess:
    def __init__(self):
        # mongourl = "mongodb://localhost:27017"
        mongourl = "mongodb://admin:adminpassword@localhost:27017"
        self.MONGO_CLIENT = MongoClient(mongourl)
        self.START_YEAR = 2020
        self.START_MONTH = 1
        self.END_YEAR = 2020
        self.END_MONTH = 2
        self.START = datetime(self.START_YEAR, self.START_MONTH, 1, 0, 0)
        self.END = datetime(self.END_YEAR, self.END_MONTH, 1, 0, 0)

    def db_news_extraction(self, coll, lang, query, chunk_size, limit=0):
        not_processed_docs = coll.find(
            query, no_cursor_timeout=True, batch_size=chunk_size
        ).limit(limit)
        return not_processed_docs

    def yield_rows(self, cursor, chunk_size):
        """
        Generator to yield chunks from cursor
        :param cursor:
        :param chunk_size:
        :return:
        """
        chunk = []
        for i, row in enumerate(cursor):
            if i % chunk_size == 0 and i > 0:
                yield chunk
                del chunk[:]
            chunk.append(row)
        yield chunk

    def build_query(self):
        q = {
            "$and": [
                {"discoverDate": {"$gte": self.START, "$lt": self.END}},
                {"reducedEmbedding": {"$exists": True}},
                {"parsedText": {"$ne": ""}},
            ]
        }
        return q

    def update_docs(self, collection, doc):
        query = {"_id": doc["_id"]}
        newvalues = {"$set": {"reducedEmbedding": doc["reducedEmbedding"]}}
        collection.update_one(query, newvalues)

    def update_dates(self):
        self.START_MONTH += 1
        self.END_MONTH += 1
        if self.START_MONTH == 13:
            self.START_MONTH = 1
            self.START_YEAR += 1
        if self.END_MONTH == 13:
            self.END_MONTH = 1
            self.END_YEAR += 1
        self.START = datetime(self.START_YEAR, self.START_MONTH, 1, 0, 0)
        self.END = datetime(self.END_YEAR, self.END_MONTH, 1, 0, 0)

    def main(self):
        print("=" * 120)
        print("STARTED DIMENSIONALITY REDUCTION")

        lang = "en"

        chunk_size = 5000

        name_coll = "article_" + lang
        collection = self.MONGO_CLIENT["news"][name_coll]

        load_save = False

        print("CURRENT COLLECTION: ARTICLE {}".format(lang.upper()))
        while self.END.year <= 2020 or (self.END.year <= 2021 and self.END.month <= 1):
            try:
                folder = "reduced_embeddings"
                file_name = "{}.npy".format(self.START.strftime("%b_%Y"))
                complete_path = "{}/{}".format(folder, file_name)
                print("Starting parsing docs from {}".format(self.START.strftime("%b_%Y")))
                if load_save:
                    query = self.build_query()
                    not_processed_docs = self.db_news_extraction(
                        collection, lang, query, chunk_size
                    )
                    count_per_month = collection.count_documents(query)
                    print("Found {} articles".format(count_per_month))

                    chunks = self.yield_rows(not_processed_docs, chunk_size)

                    docs = []
                    for chunk in chunks:
                        for doc in chunk:
                            elem = {}
                            elem["_id"] = doc["_id"]
                            elem["reducedEmbedding"] = doc["reducedEmbedding"]
                            docs = np.append(docs, elem)

                    try:
                        os.mkdir(folder)
                        print("Created {}".format(folder))
                    except Exception:
                        print("{} already exists".format(folder))

                    np.save(complete_path, docs)
                    not_processed_docs.close()
                else:
                    data = np.load(complete_path, allow_pickle=True)
                    print("Found {} articles".format(data.shape[0]))
                    for d in data:
                        self.update_docs(collection, d)

                self.update_dates()
            except CursorNotFound:
                print("Lost cursor for {}".format(self.START.strftime("%b_%Y")))
                print("Try alternative way...")


if __name__ == "__main__":
    dim_red_process = DimReductionProcess()
    dim_red_process.main()
