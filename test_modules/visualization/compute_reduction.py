from pymongo import MongoClient
from datetime import datetime
from sklearn.decomposition import PCA
from pymongo.errors import CursorNotFound

import numpy as np
import umap


class DimReductionProcess:
    def __init__(self):
        mongourl = "mongodb://admin:adminpassword@localhost:27017"
        # mongourl = "mongodb://localhost:27017"
        self.MONGO_CLIENT = MongoClient(mongourl)
        self.START_YEAR = 2019
        self.START_MONTH = 12
        self.END_YEAR = 2020
        self.END_MONTH = 1
        self.START = datetime(self.START_YEAR, self.START_MONTH, 1, 0, 0)
        self.END = datetime(self.END_YEAR, self.END_MONTH, 1, 0, 0)

    def db_news_extraction(self, lang, query, chunk_size, limit=0):
        if lang != "it":
            name_coll = "article_" + lang
        else:
            name_coll = "article"
        collection = self.MONGO_CLIENT["news"][name_coll]
        # Limit = 0 => No limit
        not_processed_docs = collection.find(
            query, no_cursor_timeout=True, batch_size=chunk_size
        ).limit(limit)
        return collection, not_processed_docs

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

    def reduce_dim(self, docs, n_dims):
        pca = PCA(n_components=n_dims, random_state=7)
        res_pca = pca.fit_transform(docs)
        # print("Shape after PCA: ", res_pca.shape)
        reducer = umap.UMAP(n_neighbors=n_dims, min_dist=0.5)
        res_umap = reducer.fit_transform(res_pca)
        # print("Shape after UMAP: ", res_umap.shape)
        return res_umap

    def build_query(self):
        q = {
            "$and": [
                {"discoverDate": {"$gte": self.START, "$lt": self.END}},
                {"bertEncoding": {"$exists": True}},
                {"reducedEmbedding": {"$exists": False}},
                {"$where": "this.bertEncoding.length > 0"},
            ]
        }
        return q

    def update_docs(self, collection, doc):
        query = {"_id": doc["_id"]}
        emb = doc["embedding"].astype("float")
        emb = emb.tolist()
        newvalues = {"$set": {"reducedEmbedding": emb}}
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
        lang = "it"
        n_dims = 50
        bert_embedding_size = 768

        chunk_size = 5000

        while self.END.year <= 2020:
            try:
                print(
                    "Looking from {} to {}".format(
                        self.START.strftime("%Y/%m"), self.END.strftime("%Y/%m")
                    )
                )
                query = self.build_query()
                coll, not_processed_docs = self.db_news_extraction(lang, query, chunk_size)
                chunks = self.yield_rows(not_processed_docs, chunk_size)
                chunk_idx = 0

                embeddings = []
                for chunk in chunks:
                    print("Processing chunk {}".format(chunk_idx))
                    for doc in chunk:
                        elem = {}
                        elem["_id"] = doc["_id"]
                        elem["embedding"] = doc["bertEncoding"]
                        embeddings = np.append(embeddings, elem)
                    chunk_idx = chunk_idx + 1
                if len(embeddings) > 0:
                    # print("Found some articles")
                    to_reduce = np.reshape(
                        [e["embedding"] for e in embeddings], (-1, bert_embedding_size)
                    )
                    # print(to_reduce.shape)
                    print("Reducing dimensions of {}".format(to_reduce.shape))
                    results = self.reduce_dim(to_reduce, n_dims)
                    # print(results.shape)
                    # Rebuilt documents' <_id, embedding> pairs
                    for r in range(results.shape[0]):
                        embeddings[r]["embedding"] = results[r]
                        self.update_docs(coll, embeddings[r])
                self.update_dates()
                not_processed_docs.close()
            except CursorNotFound:
                print("Lost cursor, retry")


if __name__ == "__main__":
    dim_red_process = DimReductionProcess()
    dim_red_process.main()
