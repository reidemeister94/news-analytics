import yaml
import logging
import requests
from pprint import pprint
import json
import os
import sys
from datetime import datetime


class NewsScraper:
    def __init__(self):
        with open('../configuration/configuration.yaml','r') as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.NEWS_API = self.CONFIG['scraper']['newsriver_api'][2]
        self.LOGGER = self.__get_logger()
        self.sort_by = self.CONFIG['scraper']['sort_by']
        self.sort_order = self.CONFIG['scraper']['sort_order']

    def __get_logger(self):
        # create logger
        logger = logging.getLogger('NewsScraper')
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = '../log/news_scraper.log'
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

    def get_news_by_query(self, query, sort_by=None, sort_order=None, limit=None):
        # query is composed used Lucene syntax:
        # see https://lucene.apache.org/core/2_9_4/queryparsersyntax.html
        # and https://console.newsriver.io/code-book for details
        url = "https://api.newsriver.io/v2/search?query={}".format(query)
        if sort_by is not None:
            url += '&sortBy={}'.format(self.sort_by[sort_by])
        if sort_order is not None:
            url += '&sortOrder={}'.format(self.sort_order[sort_order])
        if limit is not None:
            url += '&limit={}'.format(limit)
        try:
            response = requests.get(
                url, headers={"Authorization": self.NEWS_API})
            response_json = response.json()
            return response_json
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(
                exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error('Response code: {}, Response text: {}, {}, {}, {}'.format(response.status_code, response.text,
                                                                                        exc_type, fname, exc_tb.tb_lineno))
            return None
    
    def save_news_to_db(self):
        print(self.NEWS_API)
        current_date = self.CONFIG['scraper']['start_date']
        stop = False
        while not stop:
            query = "language:IT AND title:coronavirus AND discoverDate:[{} TO *] ".format(current_date)
            json_news = self.get_news_by_query(query, 'discover_date', 'asc', 100)
            for article in json_news.items():
                pprint(article)
            stop = True
                #stop = self.stop_condition(article)
        # print(type(start_date))
        # start_date = datetime.strptime(start_date, '%Y-%m-%d')
        # start_date_new = '2020-02-01'
        # self.CONFIG['scraper']['start_date'] = start_date_new
        # with open('../configuration/configuration.yaml', 'w') as f:
        #     yaml.dump(self.CONFIG, f)


    def stop_condition(self, article):
        if article['timestamp'] >= datetime.now():
            return True
        return False

if __name__ == '__main__':
    news_scraper = NewsScraper()
    # query = 'language:IT AND title:coronavirus'
    # json_news = news_scraper.get_news_by_query(query, 'discover_date', 'desc')
    # pprint(json_news)
    news_scraper.save_news_to_db()
