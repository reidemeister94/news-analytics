import spacy
from spacy import displacy
from collections import Counter
from pprint import pprint


class NamedEntityRecognition:
    def __init__(self):
        self.nlp_it = spacy.load("it_core_news_sm")
        self.nlp_en = spacy.load("en_core_web_sm")

    def named_entity_recognition_process(self, doc_text):
        # collection.insert_one(self.news_json[0])
        doc = self.nlp_en(doc_text)
        elem_pos_type = [(X, X.ent_iob_, X.ent_type_) for X in doc]
        labels = [x.label_ for x in doc.ents]
        items = [x.text for x in doc.ents]
        most_common_items = Counter(items).most_common(10)
        return elem_pos_type, labels, items, most_common_items
