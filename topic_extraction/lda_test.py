import pandas as pd

from lda_utils import LdaUtils
from lda_module import LdaModule


data = pd.read_csv("./data_test.csv")

tokens = data['tokens'].tolist()

print('ciaone')

lda = LdaModule(len(data), tokens)

dictionary = lda.build_dictionary()

corpus = lda.build_corpus()

model = lda.build_lda_model()

topics = lda.get_topics()

print(topics[:4])

'''
dictionary, tokens = lda_module.build_dictionary(tokens, use_collocations = False)

corpus = lda_module.build_corpus(dictionary, tokens)

model = lda_module.build_lda_model(dictionary, corpus)

topics = lda_module.get_topics(model, corpus, len(tokens))

print(topics)
'''
