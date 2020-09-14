from gensim import corpora, models

list_of_list_of_tokens = [
    ["ciao", "bello", "questo", "film"],
    ["ciao", "brutto", "libro"],
]
# ["a","b","c"] are the tokens of document 1, ["d","e","f"] are the tokens of document 2...
dictionary_LDA = corpora.Dictionary(list_of_list_of_tokens)
dictionary_LDA.filter_extremes(no_below=3)
corpus = [dictionary_LDA.doc2bow(list_of_tokens) for list_of_tokens in list_of_list_of_tokens]

num_topics = 20
lda_model = models.LdaModel(
    corpus,
    num_topics=num_topics,
    id2word=dictionary_LDA,
    passes=4,
    alpha=[0.01] * num_topics,
    eta=[0.01] * len(dictionary_LDA.keys()),
)
for i, topic in lda_model.show_topics(formatted=True, num_topics=num_topics, num_words=10):
    print(str(i) + ": " + topic)
    print()
