from pymongo import MongoClient
from datetime import datetime
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import pickle5 as pickle
import yaml
import os


class NewsPostProcess:
    def __init__(self):
        mongourl = "mongodb://admin:adminpassword@localhost:27017"
        self.MONGO_CLIENT = MongoClient(mongourl)
        self.START_YEAR = 2020
        self.START_MONTH = 4
        self.END_YEAR = 2020
        self.END_MONTH = 5
        self.START = datetime(self.START_YEAR, self.START_MONTH, 1, 0, 0)
        self.END = datetime(self.END_YEAR, self.END_MONTH, 1, 0, 0)
        # Articoli da agosto in poi
        # start = datetime(2020, 6, 1, 0, 0)
        # end = datetime(2020, 7, 1, 0, 0)
        # self.QUERY = {
        #     "$and": [
        #         {"discoverDate": {"$gte": start, "$lt": end}},
        #         {"$where": "this.bertEncoding.length > 0"},
        #     ]
        # }
        # self.QUERY = {}
        self.DIR_PLOT = "dim_red_plots"
        self.setup_folder()

    def db_news_extraction(self, lang, query, limit=10):
        if lang != "it":
            name_coll = "article_" + lang
        else:
            name_coll = "article"
        collection = self.MONGO_CLIENT["news"][name_coll]
        not_processed_docs = collection.find(query, no_cursor_timeout=True).limit(1000)
        return collection, not_processed_docs

    def reduce_dim(self, docs, limit):
        # n_components must be between 0 and min(n_samples, n_features)
        # file_name = "./gianni.pickle"
        # if path.exists(file_name):
        #     with open(file_name, "rb") as f:
        #         pca = pickle.load(f)
        # else:
        pca = PCA(n_components=limit, random_state=7)
        res_pca = pca.fit_transform(docs)
        print("Shape after PCA: ", res_pca.shape)
        # with open(file_name, "wb") as f:
        #     pickle.dump(pca, f)
        tsne = TSNE(
            n_components=2, perplexity=10, random_state=6, learning_rate=1000, n_iter=1500
        )
        res_tsne = tsne.fit_transform(res_pca)
        print("Shape after t-SNE: ", res_tsne.shape)
        return res_tsne

    def build_query(self):
        q = {
            "$and": [
                {"discoverDate": {"$gte": self.START, "$lt": self.END}},
                {"$where": "this.bertEncoding.length > 0"},
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
        limit = 200
        bert_embedding_size = 768
        num_topics = 20

        while self.END_YEAR <= 2020:
            print(
                "Looking from {} to {}".format(
                    self.START.strftime("%Y/%m"), self.END.strftime("%Y/%m")
                )
            )
            query = self.build_query()
            collection, not_processed_docs = self.db_news_extraction(lang, query)

            embeddings = []
            topic_numbers = []
            for doc in not_processed_docs:
                embeddings = np.append(embeddings, np.array(doc["bertEncoding"]))
                topic_probs = [el["topic_prob"] for el in doc["topicExtraction"]]
                topic_max_prob = np.argmax(topic_probs)
                topic_numbers = np.append(
                    topic_numbers, doc["topicExtraction"][topic_max_prob]["topic_number"]
                )
            if len(topic_numbers) > 0:
                print("Found some articles")
                topic_numbers = np.asarray(topic_numbers)
                topic_numbers = topic_numbers.astype("int32")
                # try:
                #     print(embeddings.shape)
                # except Exception:
                #     print(len(embeddings))
                embeddings = np.reshape(embeddings, (-1, bert_embedding_size))
                print(embeddings.shape)
                # print(topic_numbers)
                # print(type(topic_numbers[0]))
                print("Reducing dimensions")
                results = self.reduce_dim(embeddings, limit)
                print(results.shape)
                self.plot_dim_reduction(
                    results, num_topics, topic_numbers, self.create_file_path()
                )
                # color_map = np.sort(np.random.rand(num_topics))
                # colors = []
                # for t in topic_numbers:
                #     colors.append(color_map[t])
                # plt.figure(figsize=(8, 8))
                # plt.scatter(results[:, 0], results[:, 1], s=100, c=colors, alpha=0.5)
                # plt.show()
            self.update_dates()

        not_processed_docs.close()


if __name__ == "__main__":
    news_post_process = NewsPostProcess()
    news_post_process.main()
