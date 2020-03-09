import pandas as pd

from gensim import corpora, models

import utils

def build_dictionary(tokens, use_collocations = True, doc_threshold = 3):
	assert len(tokens) != 0, "Missing input tokens."

	print("... Building dictionary ...")

	if(use_collocations):
		print("... Finding collocations ...")
		tokens = utils.get_word_collocations(tokens)
	else:
		tokens = [utils.string_to_list(t) for t in tokens]

	# Build dictionary
	dictionary = corpora.Dictionary(tokens)

	# Keep tokens that appear at least in 3 documents
	if(doc_threshold > 0):
		dictionary.filter_extremes(no_below = doc_threshold)

	return dictionary, tokens

def build_corpus(dictionary, tokens):
	
	print("... Building corpus ...")

	# Build corpus as list of bags of words from the documents
	corpus = [dictionary.doc2bow(list_of_tokens) for list_of_tokens in tokens]

	return corpus


def build_lda_model(dictionary, corpus, num_topics = 20, passes = 4, alpha = 0.01, eta = 0.01):
	assert len(dictionary) != 0, "Empty dictionary."

	print("... Building LDA model ...")

	model = models.LdaModel(corpus, num_topics = num_topics,
							id2word = dictionary, passes = passes,
	                        alpha = [alpha] * num_topics,
	                        eta = [eta] * len(dictionary.keys()))

	return model


def get_topics(model, corpus, num_docs):

	print("... Retrieving topics ...")

	return [model[corpus[i]] for i in range(num_docs)]

'''
Return the topic(s) for a given document
'''
def get_document_topic(model, topics, doc_tokens, num_words):
	# word_tokenize da importare

	assert len(topics != 0), "LDA model not present."

	document_info = pd.DataFrame([(el[0], round(el[1],2), topics[el[0]][1]) for el in model[dictionary_LDA.doc2bow(doc_tokens)]], \
								columns = ['topic #', 'weight', 'words in topic'])

	return document_info


def get_top2doc_matrix(topics):

	assert len(topics != 0), "LDA model not present."
	num_topics = len(topics)

	t2d_matrix = pd.concat([topics_document_to_dataframe(topics_document, num_topics) for topics_document in topics]) \
							.reset_index(drop = True).fillna(0)
	return t2d_matrix
