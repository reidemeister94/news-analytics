import sys
sys.path.append('../')
import yaml
import logging
import requests
from pprint import pprint
import json
import os
from scraping.news_scraper import NewsScraper


class NewsPostProcess:
	def __init__(self, news_json):
		with open('../configuration/configuration.yaml') as f:
			self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
		self.LOGGER = self.__get_logger()
		self.news_json = news_json

	def topic_extraction(self):
		pass

	def cited_news_extraction(self):
		pass

	def named_entity_recognition(self):
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
	news_scraper = NewsScraper()
	query = 'language:IT AND title:coronavirus'
	json_news = news_scraper.get_news_by_query(query, 'discover_date', 'desc')
