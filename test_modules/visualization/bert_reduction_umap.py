from pymongo import MongoClient
from datetime import datetime
from os import path

import numpy as np
import matplotlib.pyplot as plt
import pickle5 as pickle
import yaml
import umap


class NewsPostProcess:
    def __init__(self):
        # init
        #  with open("configuration/configuration.yaml") as f:
        #     self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        mongourl = "mongodb://admin:adminpassword@localhost:27017"
        self.MONGO_CLIENT = MongoClient(mongourl)
        # Articoli da agosto in poi
        start = datetime(2020, 6, 1, 0, 0)
        end = datetime(2020, 7, 1, 0, 0)
        self.QUERY = {"discoverDate": {"$gte": start, "$lt": end}}
        # self.QUERY = {}

    def db_news_extraction(self, lang, limit=10):
        if lang != "it":
            name_coll = "article_" + lang
        else:
            name_coll = "article"
        collection = self.MONGO_CLIENT["news"][name_coll]
        not_processed_docs = collection.find(self.QUERY, no_cursor_timeout=True)
        return collection, not_processed_docs

    def reduce_dim(self, docs, limit):
        reducer = umap.UMAP()
        res_umap = reducer.fit_transform(docs)
        print("Shape after UMAP: ", res_umap.shape)
        return res_umap

    def main(self):
        lang = "en"
        limit = 200
        bert_embedding_size = 768
        collection, not_processed_docs = self.db_news_extraction(lang)
        # i = 0
        embeddings = []
        for doc in not_processed_docs:
            embeddings = np.append(embeddings, np.array(doc["bertEncoding"]))
        try:
            print(embeddings.shape)
        except Exception:
            print(len(embeddings))
        embeddings = np.reshape(embeddings, (-1, bert_embedding_size))
        print(embeddings.shape)
        results = self.reduce_dim(embeddings, limit)
        colors = np.random.rand(embeddings.shape[0])
        plt.figure(figsize=(8, 8))
        plt.scatter(results[:, 0], results[:, 1], s=100, c=colors, alpha=0.5)
        plt.show()
        # for doc in not_processed_docs:
        # i += 1
        # if i % 10 == 0:
        # self.save_pictures(bert_reduced)
        not_processed_docs.close()


if __name__ == "__main__":
    news_post_process = NewsPostProcess()
    news_post_process.main()
