from pymongo import MongoClient
from datetime import datetime
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from os import path

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import pickle5 as pickle
import yaml


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
        not_processed_docs = collection.find(self.QUERY, no_cursor_timeout=True).limit(1000)
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
        print(topic_numbers)
        print(type(topic_numbers[0]))
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
