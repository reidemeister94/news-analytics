from pymongo import MongoClient
from datetime import datetime
from os import path
from sklearn.decomposition import PCA

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import pickle5 as pickle
import yaml
import os
import umap


class NewsPostProcess:
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
        self.DIR_PLOT = "dim_red_plots_umap"
        self.setup_folder()

    def db_news_extraction(self, lang, query, limit=10):
        if lang != "it":
            name_coll = "article_" + lang
        else:
            name_coll = "article"
        collection = self.MONGO_CLIENT["news"][name_coll]
        not_processed_docs = collection.find(query, no_cursor_timeout=True).limit(limit)
        return collection, not_processed_docs

    def reduce_dim(self, docs, n_dims):
        pca = PCA(n_components=n_dims, random_state=7)
        res_pca = pca.fit_transform(docs)
        print("Shape after PCA: ", res_pca.shape)
        reducer = umap.UMAP(n_neighbors=n_dims, min_dist=0.5)
        res_umap = reducer.fit_transform(docs)
        print("Shape after UMAP: ", res_umap.shape)
        return res_umap

    def build_query(self):
        q = {
            "$and": [
                {"discoverDate": {"$gte": self.START, "$lt": self.END}},
                {"bertEncoding": {"$exists": True}},
                {"$where": "this.bertEncoding.length > 0"},
                # {"testTopicExtraction": {"$exists": True}},
            ]
        }
        return q

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

    def plot_dim_reduction(self, data, num_topics, topic_numbers, file_name):
        color_map = np.sort(np.random.rand(num_topics))
        colors = []
        for t in topic_numbers:
            colors.append(color_map[t])
        fig = plt.figure(figsize=(8, 8))
        fig.suptitle(file_name, fontsize=20)
        plt.scatter(data[:, 0], data[:, 1], s=100, c=colors, alpha=0.5)
        plt.savefig("{}/{}.pdf".format(self.DIR_PLOT, file_name), dpi=150)

    def setup_folder(self):
        if not os.path.exists(self.DIR_PLOT):
            os.mkdir(self.DIR_PLOT)
        else:
            print("plot directory already exists")

    def create_file_path(self):
        s = self.START.strftime("%Y_%m")
        e = self.END.strftime("%Y_%m")
        return "{}_to_{}".format(s, e)

    def main(self):
        lang = "en"
        n_dims = 50
        bert_embedding_size = 768
        num_topics = 20

        while self.END_YEAR <= 2020:
            print(
                "Looking from {} to {}".format(
                    self.START.strftime("%Y/%m"), self.END.strftime("%Y/%m")
                )
            )
            query = self.build_query()
            collection, not_processed_docs = self.db_news_extraction(lang, query, limit=1000)

            embeddings = []
            topic_numbers = []
            for doc in not_processed_docs:
                embeddings = np.append(embeddings, np.array(doc["bertEncoding"]))
                # topic_probs = [el["topic_prob"] for el in doc["testTopicExtraction"]]
                topic_probs = [el["topic_prob"] for el in doc["topicExtraction"]]
                topic_max_prob = np.argmax(topic_probs)
                # doc_topic_max_prob = doc["testTopicExtraction"][topic_max_prob]
                doc_topic_max_prob = doc["topicExtraction"][topic_max_prob]
                topic_numbers = np.append(topic_numbers, doc_topic_max_prob["topic_number"])
            if len(topic_numbers) > 0:
                print("Found some articles")
                topic_numbers = np.asarray(topic_numbers)
                topic_numbers = topic_numbers.astype("int32")
                embeddings = np.reshape(embeddings, (-1, bert_embedding_size))
                print(embeddings.shape)
                print("Reducing dimensions")
                results = self.reduce_dim(embeddings, n_dims)
                print(results.shape)
                self.plot_dim_reduction(
                    results, num_topics, topic_numbers, self.create_file_path()
                )
            self.update_dates()

        not_processed_docs.close()


if __name__ == "__main__":
    news_post_process = NewsPostProcess()
    news_post_process.main()
