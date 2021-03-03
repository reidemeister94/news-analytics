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
import dateutil.parser as parser


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

    def get_reduced_articles(self, start_date, lang):
        try:
            if type(start_date) is str:
                start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            self.LOGGER.info(start_date)
            end_date = start_date + relativedelta(months=1) - datetime.timedelta(days=1)
            if lang == "it":
                collection = self.MONGO_CLIENT["news"]["article"]
            else:
                collection = self.MONGO_CLIENT["news"]["article_" + lang]

            query = {"discoverDate": {"$gte": start_date, "$lte": end_date}}

            documents = collection.find(query, no_cursor_timeout=True)

            x_db = []
            y_db = []
            date_db = []
            title_db = []

            for doc in documents:
                if len(doc["reducedEmbedding"]) > 0:
                    x_db.append(doc["reducedEmbedding"][0])
                    y_db.append(doc["reducedEmbedding"][1])
                    date_db.append(doc["discoverDate"])
                    title_db.append(doc["title"])

            data = dict(x=x_db, y=y_db, date=date_db, title=title_db)
            return data
        except Exception as e:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.LOGGER.error(
                "{}, {}, {}, {}, {}".format(
                    start_date, exc_type, fname, exc_tb.tb_lineno, str(e)
                )
            )
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
