import spacy
from spacy import displacy
from collections import Counter
from pprint import pprint
from collections import defaultdict
import sys
import os
import logging


class NamedEntityRecognition:
    def __init__(self, nlp_model=None):
        if nlp_model is not None:
            self.nlp = nlp_model
        else:
            self.nlp = spacy.load("it_core_news_sm")
        self.LOGGER = self.__get_logger()
        self.LOGGER.info("Named Entity Recognition Ready")

    def named_entity_recognition_process(self, doc):
        # collection.insert_one(self.news_json[0])
        try:

            def add_freq(k, v):
                v["freq"] = freq_dict[k]
                return v

            parsed_doc = self.nlp(doc["parsed_text"])
            freq_dict = defaultdict(int)
            ner_data = {}
            for ent in parsed_doc.ents:
                freq_dict[ent.text.lower()] += 1
                ner_data[ent.text.lower()] = {"label": ent.label_, "freq": None}
            ner_data = {k: add_freq(k, v) for k, v in ner_data.items()}
            return ner_data
        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            # print("{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno))
            self.LOGGER.error(
                "{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno)
            )

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("NewsAnalyzer")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "core_modules/log/named_entity_recognition.log"
        if not os.path.isdir("core_modules/log/"):
            os.mkdir("core_modules/log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger


if __name__ == "__main__":
    named_entity_recognition = NamedEntityRecognition()
    text = "test"
