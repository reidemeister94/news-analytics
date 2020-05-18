import json
#import pandas as pd
import numpy as np

from lda_module import LdaModule

#data = pd.read_csv("./data_test.csv")

# Assuming a json file coming from mongoDB
file = 'data.json'
with open(file, 'r') as texts:
    data = json.load(texts)

num_docs = len(data['articles'])
num_topics = 20

# Some preparation before running LDA
#text_utils = NLPUtils(data)
#tokens = text_utils.parse_text('en')

lang = 'en'
lda = LdaModule(num_docs, data, num_topics, lang)
lda.runLDA()

#dictionary = lda.build_dictionary()

#corpus = lda.build_corpus()

#model = lda.build_lda_model()

#topics = lda.get_topics()

#flat_topics = [topic for sublist in topics for topic in sublist]

#print("I've assigned ", len(flat_topics), "topics.")
# print(flat_topics)

docs_topics_dict = lda.get_docs_topics_dict()

# == Saving to file ==
with open('doc_topic.json', 'w') as fp:
    json.dump(docs_topics_dict, fp)
