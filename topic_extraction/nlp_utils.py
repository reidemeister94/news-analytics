import pandas as pd

from langdetect import detect
from itertools import chain

from nltk.tokenize import sent_tokenize
from nltk.corpus import wordnet
from nltk.corpus import stopwords
from nltk import pos_tag
from nltk.stem.wordnet import WordNetLemmatizer

def filter_language(data, lang):
	'''
	- data: Pandas dataframe
	- lang: string representing the language to keep
	'''
	return data.loc[data.lang == lang]

def sentence_tokenize(data):
	'''
	For each document in data, create a list of strings
	with each one being a sentence from that document.

	The result is appended to data in the 'sentences' column
	'''
	data['sentences'] = data.articles.map(sent_tokenize)
	return data

def word_tokenize(data):
	'''
	For each document, tokenize the sentences in it.

	The result is appended to data in the 'tokens_sentences' column
	'''
	data['tokens_sentences'] = data['sentences'].map(lambda sentences: [word_tokenize(sentence) for sentence in sentences])
	return data


def pos_tagging(data):
	data['POS_tokens'] = data['tokens_sentences'].map(lambda tokens_sentences: [pos_tag(tokens) for tokens in tokens_sentences])
	return data

def get_wordnet_pos(treebank_tag):
	'''
	Utility function to map from the Treebank corpus tag system
	to the wordnet one
	'''

    if treebank_tag.startswith('J'):
        return wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return wordnet.VERB
    elif treebank_tag.startswith('N'):
        return wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return wordnet.ADV
    else:
        return ''

def lemmatize(data):
	# Lemmatizing each word with its POS tag, in each sentence
	lemmatizer = WordNetLemmatizer()

	data['tokens_sentences_lemmatized'] = data['POS_tokens'].map(
	    lambda list_tokens_POS: [
	        [
	            lemmatizer.lemmatize(el[0], get_wordnet_pos(el[1])) 
	            if get_wordnet_pos(el[1]) != '' else el[0] for el in tokens_POS
	        ] 
	        for tokens_POS in list_tokens_POS
	    ]
	)

	return data

def remove_stopwords(data, lang, extra_stopwords = None):
	to_remove = stopwords.words(lang)

	if extra_stopwords is not None:
		if len(extra_stopwords) > 0:
			to_remove = to_remove + extra_stopwords

	data['tokens'] = data['tokens_sentences_lemmatized'].map(lambda sentences: list(chain.from_iterable(sentences)))
	data['tokens'] = data['tokens'].map(lambda tokens: [token.lower() for token in tokens if token.isalpha() 
	                                                    and token.lower() not in to_remove and len(token)>1])

	return data
