import pymongo
from pymongo import MongoClient
from pymongo.errors import CursorNotFound, ServerSelectionTimeoutError
import os
import time
import logging
import yaml
import sys
import gc
import pprint
import datetime
from dateutil.relativedelta import relativedelta
import re


class DBHandler:
    def __init__(self):
        with open("configuration/configuration.yaml") as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.LOGGER = self.__get_logger()
        mongourl = self.CONFIG["mongourl"]
        self.MONGO_CLIENT = MongoClient(mongourl)

    def get_common_words(self, start_date, lang):
        if type(start_date) is str:
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = start_date + relativedelta(months=1) - datetime.timedelta(days=1)
        collection = self.MONGO_CLIENT["statistics"]["month_" + lang]
        ts = int(datetime.datetime.timestamp(end_date))
        query = {"ts": ts}
        # self.LOGGER.info(query)
        documents = collection.find(query)
        if documents is not None:
            res = {"data": []}
            for doc in documents:
                date_range = doc["dateRange"]
                date_range = date_range.split("00:00:00-")
                for i in range(len(date_range)):
                    date_range[i] = date_range[i].replace("00:00:00", "").strip()
                res["data"].append(
                    {
                        "date_range": "{}__{}".format(date_range[0], date_range[1]),
                        "most_frequent_words": doc["most_frequent_words"],
                    }
                )
            return res
        return None

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("DBHandler")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "log/db_handler.log"
        if not os.path.isdir("log/"):
            os.mkdir("log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger
