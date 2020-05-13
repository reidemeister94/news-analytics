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
import dateparser


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

	def news_analysis(self): 
		pass

	def main(self):
		# this is the main workflow: here the extraction and processing 
		# phases are looped until no other news has to be analyzed
		i = 0
		for lang in self.CONFIG['collections_lang']:
			if lang == 'it': # Just for testing. Remove it then.
				if lang != 'it':
					collection = self.MONGO_CLIENT['news']['article_' + lang] 
				else:
					collection = self.MONGO_CLIENT['news']['article']
			
			#print(self.CONFIG['post_process']['last_processed_id_en'] is None)
			if i == 0:
				for doc in collection.find({"id":self.CONFIG['post_process']['last_processed_id']}):
					discover_date = doc['discoverDate']
					discover_date = dateparser.parse(discover_date)#.strftime('%d/%m/%y, %H:%M')
					res = collection.update_one({'id':doc['id']}, {"$set": { "discoverDate": discover_date }})
					print(res)
					i += 1
				# print(doc)
				# print('='*75)
				# i += 1
				# if i == 10:
				# 	break
		

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
