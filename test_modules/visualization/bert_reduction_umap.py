from pymongo import MongoClient
from datetime import datetime
from sklearn.decomposition import PCA
from pymongo.errors import CursorNotFound

import numpy as np
import matplotlib.pyplot as plt
import os
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
        self.DIR_PLOT = "dim_red_plots_umap"
        self.setup_folder()

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
        print("Shape after PCA: ", res_pca.shape)
        reducer = umap.UMAP(n_neighbors=n_dims, min_dist=0.5)
        res_umap = reducer.fit_transform(res_pca)
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
        lang = "it"
        n_dims = 50
        bert_embedding_size = 768
        num_topics = 3

        chunk_size = 5000

        while self.END.year <= 2020 or (self.END.year <= 2021 and self.END.month <= 1):
            try:
                print(
                    "Looking from {} to {}".format(
                        self.START.strftime("%Y/%m"), self.END.strftime("%Y/%m")
                    )
                )
                query = self.build_query()
                _, not_processed_docs = self.db_news_extraction(lang, query, chunk_size)
                chunks = self.yield_rows(not_processed_docs, chunk_size)
                chunk_idx = 0

                embeddings = []
                topic_numbers = []
                for chunk in chunks:
                    print("Processing chunk {}".format(chunk_idx))
                    for doc in chunk:
                        embeddings = np.append(embeddings, np.array(doc["bertEncoding"]))
                        # topic_probs = [el["topic_prob"] for el in doc["testTopicExtraction"]]
                        topic_probs = [el["topic_prob"] for el in doc["topicExtraction"]]
                        topic_max_prob = np.argmax(topic_probs)
                        # doc_topic_max_prob = doc["testTopicExtraction"][topic_max_prob]
                        doc_topic_max_prob = doc["topicExtraction"][topic_max_prob]
                        topic_numbers = np.append(
                            topic_numbers, doc_topic_max_prob["topic_number"]
                        )
                    chunk_idx = chunk_idx + 1
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
            except CursorNotFound:
                print("Lost cursor, retry")


if __name__ == "__main__":
    dim_red_process = DimReductionProcess()
    dim_red_process.main()
