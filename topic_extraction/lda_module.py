import pandas as pd

from gensim import corpora, models

from lda_utils import LdaUtils


class LdaModule:

    def __init__(self, num_docs, tokens, num_topics):
        self.num_docs = num_docs
        self.tokens = tokens
        self.dictionary = None
        self.corpus = None
        self.topics = None
        self.num_topics = num_topics
        self.utils = LdaUtils()

    def __get_logger(self):
        # create logger
        logger = logging.getLogger('LdaModule')
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = '../log/LdaModule.log'
        if not os.path.isdir('../log/'):
            os.mkdir('../log/')
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger

    def build_dictionary(self, use_collocations=True, doc_threshold=3):
        assert len(self.tokens) != 0, "Missing input tokens."

        print("... Building dictionary ...")

        if(use_collocations):
            print("... Finding collocations ...")
            self.tokens = self.utils.get_word_collocations(self.tokens)
        else:
            self.tokens = [self.utils.string_to_list(t) for t in self.tokens]

        # Build dictionary
        dictionary = corpora.Dictionary(self.tokens)

        # Keep tokens that appear at least in 3 documents
        if(doc_threshold > 0):
            dictionary.filter_extremes(no_below=doc_threshold)

        self.dictionary = dictionary

        return dictionary

    def build_corpus(self):

        print("... Building corpus ...")

        # Build corpus as list of bags of words from the documents
        self.corpus = [self.dictionary.doc2bow(
            list_of_tokens) for list_of_tokens in self.tokens]

        return self.corpus

    def build_lda_model(self, num_topics=20, passes=4, alpha=0.01, eta=0.01):
        assert len(self.dictionary) != 0, "Empty dictionary."

        print("... Building LDA model ...")

        self.model = models.LdaModel(self.corpus, num_topics=self.num_topics,
                                     id2word=self.dictionary, passes=passes,
                                     alpha=[alpha] * self.num_topics,
                                     eta=[eta] * len(self.dictionary.keys()))

        return self.model

    def get_topics(self):
        print("... Retrieving topics ...")
        self.topics = [self.model[self.corpus[i]]
                       for i in range(self.num_docs)]
        return self.topics

    def get_document_topic(self, doc_tokens):
        '''
        Return the topic(s) for a given document
        '''
        assert len(self.topics != 0), "LDA model not present."
        document_info = pd.DataFrame([(el[0], round(el[1], 2), topics[el[0]][1])
                                      for el in self.model[self.dictionary.doc2bow(doc_tokens)]],
                                     columns=['topic #', 'weight', 'words in topic'])
        return document_info

    def get_top2doc_matrix(self):

        assert len(self.topics != 0), "LDA model not present."
        num_topics = len(self.topics)

        t2d_matrix = pd.concat([self.utils.topics_document_to_dataframe(topics_document, num_topics)
                                for topics_document in self.topics]).reset_index(drop=True).fillna(0)
        return t2d_matrix


if __name__ == '__main__':
    lda = LdaModule()
