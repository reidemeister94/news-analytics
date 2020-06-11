import spacy
from spacy import displacy
from collections import Counter
from pprint import pprint
from collections import defaultdict


class NamedEntityRecognition:
    def __init__(self, nlp_model=None):
        if nlp_model is not None:
            self.nlp = nlp_model
        else:
            self.nlp = spacy.load("it_core_news_sm")

    def named_entity_recognition_process(self, parsed_doc_text):
        # collection.insert_one(self.news_json[0])
        def add_freq(k, v):
            v.append(freq_dict[k])
            return v

        doc = self.nlp(parsed_doc_text)
        freq_dict = defaultdict(int)
        ner_data = {}
        for ent in doc.ents:
            freq_dict[ent.text.lower()] += 1
            ner_data[ent.text.lower()] = [ent.label_]
        ner_data = {k: add_freq(v) for k, v in ner_data.items()}
        return ner_data


if __name__ == "__main__":
    named_entity_recognition = NamedEntityRecognition()
    text = "test"
