import pandas as pd
import spacy

from langdetect import detect
from itertools import chain

from nltk.tokenize import word_tokenize
from nltk.tokenize import sent_tokenize
from nltk.corpus import wordnet
from nltk.corpus import stopwords
from nltk import pos_tag
from nltk.stem.wordnet import WordNetLemmatizer


class NLPUtils:

    def __init__(self, data):
        self.data = data
        self.nlp = spacy.load("en_core_web_md")

    def parse_text(self, lang):
        '''
        General function to parse a set of texts

        To-Do: add the option to 'toggle' only some of them
        '''
        self.data = self.convert_to_df(self.data)
        self.data = self.filter_language(self.data, lang)
        self.data = self.sentence_tokenize(self.data)
        self.data = self.get_word_tokens(self.data)
        self.data = self.pos_tagging(self.data)
        self.data = self.lemmatize_tokens(self.data)
        if(lang == 'en'):
            self.data = self.remove_stopwords(self.data, 'english')

        return self.data['tokens'].tolist()

    def convert_to_df(self, data):
        '''
        Convert the JSON data into a Pandas dataframe.
        Makes it easier to work with models from nltk and gensim.
        '''
        return pd.DataFrame.from_dict(data)

    def filter_language(self, data, lang=None):
        '''
        Keep texts only from a given language
        - data: Pandas dataframe
        - lang: string representing the language to keep (like 'en')
        '''
        ''' COMMENTED ONLY FOR TESTING
        if lang is not None:
            return data.loc[data.lang == lang]
        else:
            return data'''
        return data

    def sentence_tokenize(self, data):
        '''
        For each document in data, create a list of strings
        with each one being a sentence from that document.
        The result is appended to data in the 'sentences' column
        '''
        data['sentences'] = data.articles.map(sent_tokenize)
        return data

    def get_word_tokens(self, data):
        '''
        For each document, tokenize the sentences in it.
        The result is appended to data in the 'tokens_sentences' column
        '''
        data['tokens_sentences'] = data['sentences'].map(
            lambda sentences: [word_tokenize(sentence) for sentence in sentences])
        return data

    def pos_tagging(self, data):
        '''
        Performs POS tagging on a list of tokens representing a sentence
        '''
        data['POS_tokens'] = data['tokens_sentences'].map(
            lambda tokens_sentences: [pos_tag(tokens) for tokens in tokens_sentences])
        return data

    def _get_wordnet_pos(self, treebank_tag):
        '''
        Utility function to map from the Treebank corpus tag set (used to
        train the nltk POS tagger) to the WordNet tag set. 
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

    def lemmatize_tokens(self, data):
        '''
        Lemmatizing each word with its POS tag, in each sentence
        '''
        lemmatizer = WordNetLemmatizer()

        data['tokens_sentences_lemmatized'] = data['POS_tokens'].map(
            lambda list_tokens_POS: [
                [
                    lemmatizer.lemmatize(el[0], self._get_wordnet_pos(el[1]))
                    if self._get_wordnet_pos(el[1]) != '' else el[0] for el in tokens_POS
                ]
                for tokens_POS in list_tokens_POS
            ]
        )
        return data

    def remove_stopwords(self, data, lang, extra_stopwords=None):
        '''
        Remove very common words
        '''
        to_remove = stopwords.words(lang)

        if extra_stopwords is not None:
            if len(extra_stopwords) > 0:
                to_remove = to_remove + extra_stopwords

        data['tokens'] = data['tokens_sentences_lemmatized'].map(lambda sentences: list(chain.from_iterable(sentences)))
        data['tokens'] = data['tokens'].map(lambda tokens: [token.lower() for token in tokens if token.isalpha()
                                                            and token.lower() not in to_remove and len(token) > 1])

        return data


if __name__ == '__main__':
    nlp_utils = NLPUtils()
