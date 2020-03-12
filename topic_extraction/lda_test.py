import json
#import pandas as pd

from lda_module import LdaModule
from lda_utils import LdaUtils

from nlp_utils import NLPUtils

#data = pd.read_csv("./data_test.csv")

# Assuming a json file coming from mongoDB
file = './old/data.json'
with open(file, 'r') as texts:
    data = json.load(texts)

num_docs = len(data['articles'])

# Some preparation before running LDA
text_utils = NLPUtils(data)
tokens = text_utils.parse_text('en')

lda = LdaModule(num_docs, tokens)

dictionary = lda.build_dictionary()

corpus = lda.build_corpus()

model = lda.build_lda_model()

topics = lda.get_topics()

flat_topics = [topic for sublist in topics for topic in sublist]

print("I've assigned ", len(flat_topics), "topics.")
# print(topics)

'''
dictionary, tokens = lda_module.build_dictionary(tokens, use_collocations = False)

corpus = lda_module.build_corpus(dictionary, tokens)

model = lda_module.build_lda_model(dictionary, corpus)

topics = lda_module.get_topics(model, corpus, len(tokens))

print(topics)
'''
