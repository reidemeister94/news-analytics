import pandas as pd

import spacy
import json
import logging
import os

from itertools import chain


# Utility functions to parse text
class NLPUtils:
    def __init__(self, lang="en"):
        self.lang = lang
        self.doc = None
        if self.lang == "es":
            self.nlp = spacy.load("es_core_news_md")
        elif self.lang == "de":
            self.nlp = spacy.load("de_core_news_md")
        elif self.lang == "fr":
            self.nlp = spacy.load("fr_core_news_md")
        elif self.lang == "it":
            self.nlp = spacy.load("it_core_news_sm")
        elif self.lang == "nl":
            self.nlp = spacy.load("nl_core_news_sm")
        else:
            # English is default
            self.nlp = spacy.load("en_core_web_md")
        self.fix_stop_words()
        self.load_custom_stop_words()

    def parse_text(self, raw_data):
        """
        General function to parse a set of texts
        """
        # Check parsing before this point (should be good with pymongo)

        # Build spaCy's doc object
        self.doc = self.nlp(raw_data)
        # Retrieve sentences
        sentences = self.sentence_tokenize(self.doc)
        # print(len(sentences))
        # Lemmatize + remove stop words
        lemmas = self.lemmatize_tokens(sentences)
        # print(len(lemmas))
        # Flatten results into a single list
        parsed_text = self.flatten_list(lemmas)

        return parsed_text

    def fix_stop_words(self):
        """
        Despite being present in spaCy's models, sometimes stop words aren't picked up.
        This workaround forces them to be removed.
        """
        for word in self.nlp.Defaults.stop_words:
            self.nlp.vocab[word].is_stop = True
        return

    def load_custom_stop_words(self):
        with open("core_modules/topic_extraction/custom_stop_words.json", "r") as sw:
            custom_stop_words = json.load(sw)["s_words"]
        # custom_stop_words = custom_stop_words['s_words']
        self.add_custom_stop_words(custom_stop_words)

    def add_custom_stop_words(self, custom_stop_words):
        """
        If any, custom words are added by flagging them in the language model (nlp)
        """
        for cw in custom_stop_words:
            self.nlp.vocab[cw].is_stop = True
        return

    def sentence_tokenize(self, data):
        """
        Creates a list of strings with each one being a sentence from that document.
        """
        return [sent for sent in data.sents]

    def lemmatize_tokens(self, data):
        """
        Lemmatizing each word + remove stop words in each sentence
        """
        lemmas = []
        for sent in data:
            lemmas.append(
                [
                    token.lemma_
                    for token in sent
                    if (
                        not self.nlp.vocab[token.lower_].is_stop
                        and not token.is_punct
                        and len(token.text) > 1
                    )
                ]
            )
        return lemmas

    def flatten_list(self, data):
        """
        Flattens the lemmatized sentences into a single list,
        ready for gensim's LDA implementation
        """
        return list(chain.from_iterable(data))

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("LdaModule")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "core_modules/log/NLPUtils.log"
        if not os.path.isdir("core_modules/log"):
            os.mkdir("core_modules/log")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger


if __name__ == "__main__":
    nlp_utils = NLPUtils()
