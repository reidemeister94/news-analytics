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
		self.nlp = spacy.load('it_core_news_sm')

	def topic_extraction(self):
		pass

	def cited_news_extraction(self):
		pass

	def named_entity_recognition(self):
		collection = self.MONGO_CLIENT['news']['article']
		# collection.insert_one(self.news_json[0])
		doc = self.nlp(self.news_json[0]['text'])
		pprint([(X, X.ent_iob_, X.ent_type_) for X in doc])
		labels = [x.label_ for x in doc.ents]
		pprint(Counter(labels))
		items = [x.text for x in doc.ents]
		pprint(Counter(items).most_common(3))
		sentences = [x for x in doc.sents]
		displacy.render(self.nlp(str(sentences[20])),
		                style='dep', options={'distance': 120})

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
	query = 'language:IT AND title:coronavirus'
	news_json = news_scraper.get_news_by_query(query, 'discover_date', 'desc')
	# pprint(news_json[0])
	news_post_process = NewsPostProcess(news_json)
	news_post_process.named_entity_recognition()
