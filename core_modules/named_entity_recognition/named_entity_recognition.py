import spacy
from spacy import displacy
from collections import Counter
from pprint import pprint


class NamedEntityRecognition:
    def __init__(self, nlp_model=None):
        if nlp_model is not None:
            self.nlp = nlp_model
        else:
            self.nlp = spacy.load("it_core_news_sm")

    def named_entity_recognition_process(self, doc_text):
        # collection.insert_one(self.news_json[0])
        doc = self.nlp(doc_text)
        elem_pos_type = [(str(X), X.ent_iob_, X.ent_type_) for X in doc]
        labels = [x.label_ for x in doc.ents]
        items = [x.text for x in doc.ents]
        most_common_items = Counter(items).most_common(10)
        return elem_pos_type, labels, items, most_common_items


if __name__ == "__main__":
    named_entity_recognition = NamedEntityRecognition()
    text = """The Centers for Disease Control and Prevention (CDC) said the novel coronavirus, or COVID-19, is spread mainly from person-to-person by those in close contact, or through coughing and sneezing by someone who’s infected.

Symptoms of the coronavirus can show up between two and 14 days of exposure, health officials say. Symptoms include fever, cough, and shortness of breath.

For most people, COVID-19 causes only mild or moderate symptoms, such as fever and cough. But some severe cases can lead to death.

Most people can recover from the virus at home using over-the-counter medications to treat their symptoms.

Some people who have the virus don’t show any symptoms, but they can still spread it to others. The CDC estimates that up to 35% of all cases are asymptomatic."""

    elem_pos_type, labels, items, most_common_items = named_entity_recognition.named_entity_recognition_process(text)
    #print(elem_pos_type)
    for k, v in most_common_items:
        print(type(k),type(v))