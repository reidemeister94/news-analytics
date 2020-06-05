import json
import numpy as np

from lda_module import LdaModule
from lda_utils import LdaUtils

# Assuming a json file coming from mongoDB
file = "data.json"
with open(file, "r") as texts:
    data = json.load(texts)

doc_collection = []

# Define LDA model
num_docs = len(doc_collection)
num_topics = 20
lda = LdaModule(
    lang="en",
    num_docs=num_docs,
    doc_collection=doc_collection,
    num_topics=num_topics,
    trained=False,
)

# Some preparation before running LDA
print("Parsing articles...")
for doc in data["articles"]:
    # tokens = text_utils.parse_text(doc['text'])
    tokens = lda.parse_text(doc["text"])
    doc_collection.append(tokens)

print("Completed parsing articles")

lda.runLDA()

docs_topics_dict = lda.get_docs_topics_dict()

# == Saving to file ==
with open("doc_topic.json", "w") as fp:
    json.dump(docs_topics_dict, fp)

# == Saving model checkpoint ==
lda.save_LDA_model()
