import dateparser
from pymongo import MongoClient
from core_modules.topic_extraction.nlp_utils import NLPUtils
from core_modules.topic_extraction.lda_module import LdaModule
from core_modules.named_entity_recognition.named_entity_recognition import NamedEntityRecognition
from core_modules.news_analyzer.news_analyzer import NewsAnalyzer
from scraping.news_scraper import NewsScraper
import os
import time
import json
from pprint import pprint
import requests
import logging
import yaml
import sys
sys.path.append('../')


class NewsPostProcess:
    def __init__(self):
        with open('../configuration/configuration.yaml') as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.LOGGER = self.__get_logger()
        mongourl = 'mongodb://localhost:27017/'
        self.MONGO_CLIENT = MongoClient(mongourl)
        self.news_json = None
        self.news_analyzer = None
        self.named_entity_recognition = None
        self.lda_module = None
        self.nlp_utils = None

    def db_news_extraction(self):
        # news extraction from db: there will be extracted
        # all the articles that aren't processed yet
        pass

    def process_doc(self, doc):
        # named entity recognition phase
        elem_pos_type, labels, items, most_common_items = self.named_entity_recognition.named_entity_recognition_process(
            doc['text'])
        ner_data = [elem_pos_type, labels, items, most_common_items]

        # topic extraction phase
        parsed_text = self.nlp_utils.parse_text(doc['text'])
        topics = self.lda_module.model.show_topics(formatted=False,
                                                   num_topics=self.CONFIG['topic_extraction']['num_topics'],
                                                   num_words=self.CONFIG['topic_extraction']['num_words'])
        document_topic_info = {}
        for el in self.lda_module.model[lda_module.dictionary.doc2bow(parsed_text)]:
            document_topic_info[el[0]] = [round(el[1], 2), topics[el[0]][1]]

        # bert enconding phase

    def topic_extraction(self, num_topics):
        # from db: data, lang, num_docs
        # lda = LdaModule(num_docs, data, num_topics, lang)
        # lda.runLDA()
        # docs_topics_dict = lda.get_docs_topics_dict()
        pass

    def cited_news_extraction(self):
        pass

    def named_entity_recognition(self):
        pass

    def news_analysis(self):
        pass

    def main(self):
        # this is the main workflow: here the extraction and processing
        # phases are looped until no other news has to be analyzed
        i = 0  # DEBUG
        self.news_analyzer = NewsAnalyzer(self.CONFIG)
        self.named_entity_recognition = NamedEntityRecognition()
        for lang in self.CONFIG['collections_lang']:
            self.lda_module = LdaModule(lang=lang, trained=True)
            self.nlp_utils = NLPUtils(lang=lang)
            if lang == 'en':  # DEBUG - only english news
                if lang != 'it':
                    name_coll = 'article_' + lang
                    last_processed_param = 'last_processed_' + lang
                else:
                    name_coll = 'article'
                    last_processed_param = 'last_processed'
                collection = self.MONGO_CLIENT['news'][name_coll]
                if i == 0:  # DEBUG
                    not_processed_docs = collection.find({'$or': [
                        {'processedEncoding': False},
                        {'processedEncoding': {'$exists': False}}]})
                    # print(len(list(not_processed_docs)))
                    for doc in not_processed_docs:
                        # processing every document in db
                        # print(doc)
                        # print('='*75)
                        # time.sleep(5)
                        updated_doc = self.process_doc(doc)
                        time.sleep(10)
                    i += 1  # DEBUG

    def __stop(self, p, collection):
        is_old_post = collection.find_one({'id_post': p['id_post']})
        if is_old_post is None and p['timestamp'] >= self.min_date_post:
            return False
        # if p['timestamp'] >= self.min_date_post:
        #     return False
        else:
            return True

    def __get_logger(self):
        # create logger
        logger = logging.getLogger('NewsPostProcess')
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = '../log/news_post_process.log'
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


if __name__ == '__main__':
    news_post_process = NewsPostProcess()
    news_post_process.main()
