import json
import numpy as np
import datetime

from lda_module import LdaModule
from lda_utils import LdaUtils

from nlp_utils import NLPUtils

# Assuming a json file coming from mongoDB
file = 'data.json'
with open(file, 'r') as texts:
    data = json.load(texts)

doc_collection = []

# Some preparation before running LDA
text_utils = NLPUtils('en')
print("Parsing articles...")
for doc in data['articles']:
    tokens = text_utils.parse_text(doc['text'])
    doc_collection.append(tokens)

print("Completed parsing articles")
num_docs = len(doc_collection)
num_topics = 20

lda = LdaModule(num_docs = num_docs, doc_collection = doc_collection, num_topics = num_topics, trained = False)

lda.runLDA()

docs_topics_dict = lda.get_docs_topics_dict()

# == Saving to file ==
with open('doc_topic.json', 'w') as fp:
    json.dump(docs_topics_dict, fp)

# == Saving model checkpoint ==
now = datetime.datetime.now()
timestamp = now.strftime("%m-%d-%Y_%H-%M-%S")
lda.save_LDA_model("./lda_checkpoint/lda_{}".format(timestamp))