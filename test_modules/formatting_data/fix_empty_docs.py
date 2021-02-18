from pymongo import MongoClient
from pymongo.errors import CursorNotFound


class DimReductionProcess:
    def __init__(self):
        mongourl = "mongodb://localhost:27017"
        # mongourl = "mongodb://admin:adminpassword@localhost:27017"
        self.MONGO_CLIENT = MongoClient(mongourl)

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

    def build_query(self):
        q = {"parsedText": ""}
        return q

    def update_reduced_embedding(self, collection, doc_id):
        query = {"_id": doc_id}
        new_values = {"$set": {"reducedEmbedding": []}}
        collection.update_one(query, new_values)

    def main(self):
        chunk_size = 5000

        for lang in ["it", "en", "fr", "de", "nl", "es"]:
            print("CURRENT COLLECTION: ARTICLE {}".format(lang.upper()))
            try:
                query = {"parsedText": ""}
                coll, not_processed_docs = self.db_news_extraction(lang, query, chunk_size)

                chunks = self.yield_rows(not_processed_docs, chunk_size)

                i = 0

                for chunk in chunks:
                    # print("Processing chunk {}".format(chunk_idx))
                    for doc in chunk:
                        self.update_reduced_embedding(coll, doc["_id"])
                        i = i + 1

                print("Fixed {} {} articles".format(i, lang))

                not_processed_docs.close()
            except CursorNotFound:
                print("Lost cursor")


if __name__ == "__main__":
    dim_red_process = DimReductionProcess()
    dim_red_process.main()
