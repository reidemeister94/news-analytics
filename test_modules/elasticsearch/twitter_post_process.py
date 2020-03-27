import yaml
import logging
import os
import requests


class TwitterPostProcess:

    def __init__(self):
        with open('./configuration.yaml', 'r') as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        #self.LOGGER = self.__get_logger()
        self.HOST = self.CONFIG['twitter_es']['host']
        self.INDEX = self.CONFIG['twitter_es']['index']

    def __get_logger(self):
        # create logger
        logger = logging.getLogger('TwitterPostProcess')
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = '../log/twitter_post_process.log'
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

    def query_es(self, query, num_results):
        req = self.HOST + self.INDEX + '/' + query
        if num_results > 0:
            req = req + '&size=' + str(num_results)
        r = requests.get(req)
        return r.json()

    def format_results(self, res):
        # Sistemare
        print(res['hits']['hits'][0]['_source']['text'])

    # Può essere superfluo per ora dato che abbiamo solo roba sul Corona
    def topic_extraction(self):
        pass


if __name__ == '__main__':
    twitter_post_process = TwitterPostProcess()
    query = '_search?q=favorited:false'
    num_results = 5
    twitter_json = twitter_post_process.query_es(query, num_results)
    results = twitter_post_process.format_results(twitter_json)
