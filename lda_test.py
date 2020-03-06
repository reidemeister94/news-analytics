import pandas as pd

from utils import lda_utils
from topic_extraction import lda_model


data = pd.read_csv("./topic-lda/data_test.csv")

lda_mod = lda_model(20)

tokens = data['tokens'].tolist()
print("tokens length", len(tokens))

corpus = lda_mod.build_corpus(tokens)

model = lda_mod.build_lda_model(corpus)

print(model.topics)