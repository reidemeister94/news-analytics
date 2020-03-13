import sys
sys.path.append('../')
import yaml
import logging
import requests
from pprint import pprint
import json
import os
from scraping.news_scraper import NewsScraper
from pymongo import MongoClient
import spacy
from spacy import displacy
from collections import Counter


class NewsPostProcess:
	def __init__(self, news_json):
		with open('../configuration/configuration.yaml') as f:
			self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
		self.LOGGER = self.__get_logger()
		mongourl = 'mongodb://localhost:27017/'
		self.MONGO_CLIENT = MongoClient(mongourl)
		self.news_json = news_json
		self.nlp_it = spacy.load('it_core_news_sm')
		self.nlp_en = spacy.load('en_core_web_sm')

	def topic_extraction(self):
		pass

	def cited_news_extraction(self):
		pass

	def named_entity_recognition(self):
		collection = self.MONGO_CLIENT['news']['article']
		# collection.insert_one(self.news_json[0])
		doc = self.nlp_en(self.news_json[0]['text'])
		print('News Text:')
		print(doc)
		print('"' * 75)
		print('Elem, where is element (begin, inside, outside), Named entity type:')
		pprint([(X, X.ent_iob_, X.ent_type_) for X in doc])
		print('"' * 75)
		labels = [x.label_ for x in doc.ents]
		print('Labels:')
		pprint(Counter(labels))
		print('"' * 75)
		items = [x.text for x in doc.ents]
		print('{} Most common items in doc'.format(3))
		pprint(Counter(items).most_common(3))
		print('"' * 75)

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
	news_scraper = NewsScraper()
	query = 'language:EN AND title:coronavirus'
	news_json = news_scraper.get_news_by_query(query, 'discover_date', 'desc')
	news_post_process = NewsPostProcess(news_json)
	news_post_process.named_entity_recognition()
