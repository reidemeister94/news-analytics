import yaml
import logging
import os
import requests
import json
import dateutil
import pytz
from datetime import datetime

import schedule
import time


class TwitterPostProcess:

    def __init__(self):
        with open('./configuration.yaml', 'r') as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        #self.LOGGER = self.__get_logger()
        self.HOST = self.CONFIG['twitter_es']['host']
        self.INDEX = self.CONFIG['twitter_es']['index']
        self.DATE = self.CONFIG['twitter_es']['last_time']

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

    def query_es(self, query):
        url = self.HOST + self.INDEX + "/_search"
        r = requests.post(url, json=query)
        return r.json()

    def format_results(self, res):
        # == Sistemare ==
        hits = res['hits']['hits']
        for h in hits:
            print(h['_source']['text'])
            print('='*10)
        return hits

    def get_last_post_date(self, data):
        l = len(data)
        if l > 0:
            # Save the last tweet timestamp to resume
            day = data[-1]["_source"]["created_at"]
            print("Saving last timestamp...")
            print(day)
            return day
        else:
            # If nothing is found save the current time
            # Adding timezone to match Elasticsearch date format
            day = datetime.now()
            timezone = pytz.timezone("Europe/Rome")
            day_tz = timezone.localize(day)
            day_tz_str = day_tz.strftime("%a %b %d %H:%M:%S %z %Y")
            print("Saving last timestamp...")
            print(day_tz_str)
            return day_tz_str

    def scheduled_test(self, query_args, num_results):
        query_body = {}
        if num_results > 0:
            query_body["size"] = num_results
        query_body["query"] = query_args
        query_body["sort"] = {"created_at": "asc"}

        # while not stop:
        twitter_json = self.query_es(query_body)

        twitter_hits = self.format_results(twitter_json)

        self.DATE = self.get_last_post_date(twitter_hits)
        self.CONFIG['twitter_es']['last_time'] = self.DATE
        with open('./configuration.yaml', 'w') as f:
            yaml.dump(self.CONFIG, f)

    # Può essere superfluo per ora dato che abbiamo solo roba sul Corona

    def topic_extraction(self):
        pass


if __name__ == '__main__':
    twitter_post_process = TwitterPostProcess()

    query_simple = {
        "range": {
            "created_at": {
                "format": "EEE MMM dd HH:mm:ss Z yyyy",
                "gte": "now-14d",
                "lt": "now"
            }
        }
    }

    query_not_fave = {
        "bool": {
            "must": [{
                "term": {
                    "favorited": "false"
                }
            }, {
                "range": {
                    "created_at": {
                        "format": "EEE MMM dd HH:mm:ss Z yyyy",
                        "gte": "now-14d",
                        "lt": "now"
                    }
                }
            }
            ]
        }
    }

    num_results = 0  # if 0 -> no 'size' in the request, elasticsearch defaults to 10 results
    twitter_post_process.scheduled_test(query_simple, num_results)

    schedule.every(2).minutes.do(twitter_post_process.scheduled_test, query_simple, num_results)

    while True:
        schedule.run_pending()
        time.sleep(1)
    #twitter_json = twitter_post_process.query_es(query, num_results)
    #results = twitter_post_process.format_results(twitter_json)
