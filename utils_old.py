from gensim.models import Phrases

class lda_utils:

	def get_word_collocations(tokens):
		print("... trying ...")
		bigrams = Phrases(tokens)
		print("... survived the first one ...")
		trigrams = Phrases(bigrams[tokens], min_count = 1)

		return list(trigrams[bigrams[tokens]])

	def topics_document_to_dataframe(topics_document, num_topics):
	    res = pd.DataFrame(columns = range(num_topics))
	    for topic_weight in topics_document:
	        res.loc[0, topic_weight[0]] = topic_weight[1]
	    return res