import json
#import pandas as pd
import numpy as np

from lda_module import LdaModule
from lda_utils import LdaUtils

from nlp_utils import NLPUtils

#data = pd.read_csv("./data_test.csv")

# Assuming a json file coming from mongoDB
file = './old/data.json'
with open(file, 'r') as texts:
    data = json.load(texts)

num_docs = len(data['articles'])
num_topics = 20

# Some preparation before running LDA
text_utils = NLPUtils(data)
tokens = text_utils.parse_text('en')

lda = LdaModule(num_docs, tokens, num_topics)

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

# == Saving to file ==
topics_doc_dict = {}
for i in range(num_docs):
    #print('---- Documento ',i,' ----')
    current_doc_topics = topics[i]
    for j in range(len(current_doc_topics)):
        topic = current_doc_topics[j]
        if len(topic) == 1:
            topic = topic[0]
        # print(topic)
        topic = (topic[0], str(topic[1]))
        current_doc_topics[j] = topic

    topics_doc_dict[str(i)] = {'topic': current_doc_topics, 'words': model.show_topics(
        formatted=True, num_topics=lda.num_topics, num_words=20)[topics[i][0][0]]}

with open('doc_topic.json', 'w') as fp:
    json.dump(topics_doc_dict, fp)
