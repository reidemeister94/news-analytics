import pandas as pd

from gensim import corpora, models

from utils import lda_utils

class lda_model():

	def __init__(self, num_topics):
		self.num_topics = num_topics # old value = 20
		self.dictionary = corpora.Dictionary()
		self.topics = []


	def build_corpus(tokens, use_collocations = True, doc_threshold = 3):
		print("... Building corpus ...")
		if(use_collocations):
			print("... Finding collocations ...")
			tokens = lda_utils.get_word_collocations(tokens)

		assert len(tokens) != 0, "Missing input tokens."

		# Build dictionary
		self.dictionary = corpora.Dictionary(tokens)

		# Keep tokens that appear at least in 3 documents
		if(doc_threshold > 0):
			self.dictionary.filter_extremes(no_below = doc_threshold)

		# Build corpus as list of bags of words from the documents
		corpus = [self.dictionary.doc2bow(list_of_tokens) for list_of_tokens in tokens]

		return corpus


	def build_lda_model(corpus, passes = 4, alpha = 0.01, eta = 0.01):
		assert len(self.dictionary) != 0, "Empty dictionary."

		model = models.LdaModel(corpus, num_topics = self.num_topics,
								id2word = self.dictionary, passes = passes,
		                        alpha = [alpha] * self.num_topics,
		                        eta = [eta] * len(self.dictionary.keys()))

		self.topics = [model[corpus[i]] for i in range(len(data))]

		return model


	'''
	Return the topic(s) for a given document
	'''
	def get_document_topic(model, document, num_words):
		# word_tokenize da importare

		assert len(self.topics != 0), "LDA model not present."

		document_info = pd.DataFrame([(el[0], round(el[1],2), topics[el[0]][1]) for el in model[dictionary_LDA.doc2bow(tokens)]], \
									columns = ['topic #', 'weight', 'words in topic'])

		return document_info


	def get_top2doc_matrix():

		assert len(self.topics != 0), "LDA model not present."

		t2d_matrix = pd.concat([topics_document_to_dataframe(topics_document, self.num_topics) for topics_document in self.topics]) \
								.reset_index(drop = True).fillna(0)
		return t2d_matrix









		