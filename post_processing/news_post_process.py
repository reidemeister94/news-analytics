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


class NewsPostProcess:
	def __init__(self):
		with open('../configuration/configuration.yaml') as f:
			self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
		self.LOGGER = self.__get_logger()
		mongourl = 'mongodb://localhost:27017/'
		self.MONGO_CLIENT = MongoClient(mongourl)
		self.news_json = None

	def db_news_extraction(self):
		# news extraction from db: there will be extracted
		# all the articles that aren't processed yet
		pass

	def topic_extraction(self):
		pass

	def cited_news_extraction(self):
		pass

	def named_entity_recognition(self):
		pass

	def news_similarity(self):
		# this is a task that can be performed only 
		# after all the previous tasks because we have 
		# to work not only on the text but also on features like topic and n.e.r. 
		pass

	def main(self):
		# this is the main workflow: here the extraction and processing 
		# phases are looped until no other news has to be analyzed
		pass

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
