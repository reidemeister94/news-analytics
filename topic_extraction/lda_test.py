import pandas as pd

import utils
import lda_module


data = pd.read_csv("./data_test.csv")

tokens = data['tokens'].tolist()

dictionary, tokens = lda_module.build_dictionary(tokens, use_collocations = False)

corpus = lda_module.build_corpus(dictionary, tokens)

model = lda_module.build_lda_model(dictionary, corpus)

topics = lda_module.get_topics(model, corpus, len(tokens))

print(len(topics))