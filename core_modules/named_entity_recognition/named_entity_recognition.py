import spacy
from spacy import displacy
from collections import Counter
from pprint import pprint

class NamedEntityRecognition():

    def __init__(self):
        self.nlp_it = spacy.load('it_core_news_sm')
        self.nlp_en = spacy.load('en_core_web_sm')

    def named_entity_recognition(self, MONGO_CLIENT):
        collection = MONGO_CLIENT['news']['article']
        # collection.insert_one(self.news_json[0])
        doc = self.nlp_en(self.news_json[0]['text'])
        print('News Text:')
        print(doc)
        print('"' * 75)
        print('Elem, where is element (begin, inside, outside), Named entity type:')
        pprint([(X, X.ent_iob_, X.ent_type_) for X in doc])
        print('"' * 75)
        labels = [x.label_ for x in doc.ents]
        print('Labels:')
        pprint(Counter(labels))
        print('"' * 75)
        items = [x.text for x in doc.ents]
        print('{} Most common items in doc'.format(3))
        pprint(Counter(items).most_common(3))
        print('"' * 75)
