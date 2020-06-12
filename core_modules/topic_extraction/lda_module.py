import pandas as pd
import datetime
import os
import logging

from gensim import corpora, models

from core_modules.topic_extraction.lda_utils import LdaUtils
from core_modules.topic_extraction.nlp_utils import NLPUtils
import yaml


class LdaModule:
    def __init__(
        self, lang="en", num_docs=0, doc_collection=[], num_topics=0, trained=False
    ):
        with open("configuration/configuration.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.lang = lang
        self.num_docs = num_docs
        self.doc_collection = doc_collection
        self.num_topics = num_topics
        self.dictionary = None
        self.corpus = None
        self.topics = None
        # load it form config file?
        self.location = self.CONFIG["topic_extraction"]["lda_models_path"]
        self.utils = LdaUtils()
        # If the model has already been trained we restore it
        if trained:
            self.load_lda_model()
        else:
            self.model = None

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("LdaModule")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "core_modules/log/LdaModule.log"
        if not os.path.isdir("core_modules/log"):
            os.mkdir("core_modules/log")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger

    def parse_text(self, raw_data):
        text_utils = NLPUtils(self.lang)
        return text_utils.parse_text(raw_data)

    def build_dictionary(self, use_collocations=True, doc_threshold=3):
        assert len(self.doc_collection) != 0, "Missing input tokens."

        print("... Building dictionary ...")

        if use_collocations:
            print("... Finding collocations ...")
            self.doc_collection = self.utils.get_word_collocations(self.doc_collection)
        else:
            self.doc_collection = [
                self.utils.string_to_list(t) for t in self.doc_collection
            ]

        # Build dictionary
        dictionary = corpora.Dictionary(self.doc_collection)

        # Keep tokens that appear at least in 3 documents
        if doc_threshold > 0:
            dictionary.filter_extremes(no_below=doc_threshold)

        self.dictionary = dictionary

    def build_corpus(self):

        print("... Building corpus ...")

        # Build corpus as list of bags of words from the documents
        self.corpus = [
            self.dictionary.doc2bow(list_of_tokens)
            for list_of_tokens in self.doc_collection
        ]

    def build_lda_model(self, num_topics=20, passes=4, alpha=0.01, eta=0.01):
        assert len(self.dictionary) != 0, "Empty dictionary."

        print("... Building LDA model ...")

        self.model = models.LdaModel(
            self.corpus,
            num_topics=self.num_topics,
            id2word=self.dictionary,
            passes=passes,
            alpha=[alpha] * self.num_topics,
            eta=[eta] * len(self.dictionary.keys()),
        )

    def get_topics(self):
        print("... Retrieving topics ...")
        self.topics = [self.model[self.corpus[i]] for i in range(self.num_docs)]

    def get_topics_flat(self):
        """
		Format self.topics object into a list
		"""
        return [topic for sublist in self.topics for topic in sublist]

    def get_document_topic(self, doc_tokens):
        """
		Return the topic(s) for a given document.
		Future: now it's unused, maybe to remove since this info is made persistent on mongo
		"""
        assert len(self.topics) != 0, "LDA model not present."
        document_info = pd.DataFrame(
            [
                (el[0], round(el[1], 2), self.topics[el[0]][1])
                for el in self.model[self.dictionary.doc2bow(doc_tokens)]
            ],
            columns=["topic #", "weight", "words in topic"],
        )
        return document_info

    def get_top2doc_matrix(self):
        """
		Future: now it's unused, maybe to remove...
		"""
        assert len(self.topics != 0), "LDA model not present."
        num_topics = len(self.topics)

        t2d_matrix = (
            pd.concat(
                [
                    self.utils.topics_document_to_dataframe(topics_document, num_topics)
                    for topics_document in self.topics
                ]
            )
            .reset_index(drop=True)
            .fillna(0)
        )
        return t2d_matrix

    def get_docs_topics_dict(self):
        docs_topics_dict = {}
        for i in range(self.num_docs):
            # print('---- Documento ',i,' ----')
            current_doc_topics = self.topics[i]
            for j in range(len(current_doc_topics)):
                topic = current_doc_topics[j]
                if len(topic) == 1:
                    topic = topic[0]
                # print(topic)
                topic = (topic[0], str(topic[1]))
                current_doc_topics[j] = topic

            docs_topics_dict[str(i)] = {
                "topic": current_doc_topics,
                "words": self.model.show_topics(
                    formatted=True, num_topics=self.model.num_topics, num_words=20
                )[self.topics[i][0][0]],
            }
        return docs_topics_dict

    def runLDA(self):
        self.build_dictionary()
        self.build_corpus()
        self.build_lda_model()
        self.get_topics()

    def save_LDA_model(self):
        # now = datetime.datetime.now()
        # timestamp = now.strftime("%m-%d-%Y_%H-%M-%S")
        path = self.location + "lda_model_" + self.lang
        if not os.path.isdir(self.location):
            os.mkdir(self.location)
        self.utils.save_lda_model(self, path)

    def load_lda_model(self):
        print("LOCAZIONE: {}".format(os.getcwd()))
        module = self.utils.load_lda_model(self.location + "lda_model_" + self.lang)
        self.dictionary = module.dictionary
        self.corpus = module.corpus
        self.model = module.model
        self.topics = module.topics
        self.lang = module.lang

    def update_lda_model(self, parsed_docs):
        # text_utils = NLPUtils(self.lang)
        if self.model is None:
            self.load_lda_model()
        # parsed_doc = text_utils.parse_text(doc)
        new_corpus = self.corpus + [self.dictionary.doc2bow(doc) for doc in parsed_docs]
        self.model.update(new_corpus)
        self.save_LDA_model()


if __name__ == "__main__":
    lda = LdaModule()
