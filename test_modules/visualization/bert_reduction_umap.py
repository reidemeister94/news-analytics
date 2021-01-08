from pymongo import MongoClient
from datetime import datetime
from os import path

from bokeh.plotting import figure, output_file, show
from bokeh.models import Label

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
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
        self.QUERY = {
            "$and": [
                {"discoverDate": {"$gte": start, "$lt": end}},
                {"$where": "this.bertEncoding.length > 0"},
            ]
        }
        # self.QUERY = {}

    def db_news_extraction(self, lang, limit=10):
        if lang != "it":
            name_coll = "article_" + lang
        else:
            name_coll = "article"
        collection = self.MONGO_CLIENT["news"][name_coll]
        not_processed_docs = collection.find(self.QUERY, no_cursor_timeout=True).limit(1000)
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
        num_topics = 20
        collection, not_processed_docs = self.db_news_extraction(lang)
        # i = 0
        embeddings = []
        topic_numbers = []

        for doc in not_processed_docs:
            embeddings = np.append(embeddings, np.array(doc["bertEncoding"]))
            topic_probs = [el["topic_prob"] for el in doc["topicExtraction"]]
            topic_max_prob = np.argmax(topic_probs)
            topic_numbers = np.append(
                topic_numbers, doc["topicExtraction"][topic_max_prob]["topic_number"]
            )
        topic_numbers = topic_numbers.astype("int32")
        try:
            print(embeddings.shape)
        except Exception:
            print(len(embeddings))
        embeddings = np.reshape(embeddings, (-1, bert_embedding_size))
        print(embeddings.shape)
        results = self.reduce_dim(embeddings, limit)
        color_map = np.sort(np.random.rand(num_topics))
        colors = []
        for t in topic_numbers:
            colors.append(color_map[t])
        plt.figure(figsize=(8, 8))
        plt.scatter(results[:, 0], results[:, 1], s=100, c=colors, alpha=0.5)
        plt.show()
        not_processed_docs.close()


if __name__ == "__main__":
    news_post_process = NewsPostProcess()
    news_post_process.main()
