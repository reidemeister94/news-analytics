import networkx as nx
import numpy as np
from pymongo import MongoClient
from nltk.tokenize.punkt import PunktSentenceTokenizer
from sklearn.feature_extraction.text import TfidfTransformer, CountVectorizer
import yaml
from bert_serving.client import BertClient
from pprint import pprint
import math

class NewsSimilarity:
    def __init__(self):
        with open('../configuration/configuration.yaml','r') as f:
            self.CONFIG = yaml.load(f, Loader=yaml.FullLoader)
        self.CLIENT = MongoClient(self.CONFIG['mongourl'])
        self.BC = BertClient(port=5555, port_out=5556, check_version=False)
 
    def text_rank(self, document):
        sentence_tokenizer = PunktSentenceTokenizer()
        sentences = sentence_tokenizer.tokenize(document)
        bow_matrix = CountVectorizer().fit_transform(sentences)
        normalized = TfidfTransformer().fit_transform(bow_matrix)
        similarity_graph = normalized * normalized.T
        nx_graph = nx.from_scipy_sparse_matrix(similarity_graph)
        scores = nx.pagerank(nx_graph)
        return sorted(((scores[i],s) for i,s in enumerate(sentences)),
                    reverse=True)

    def scoring(self,first_doc, second_doc):
        cosine = np.dot(first_doc, second_doc) / \
            (np.linalg.norm(first_doc) * np.linalg.norm(second_doc))
        return 1 / (1 + math.exp(-100 * (cosine - 0.95)))

    def similarity_pair(self,pair):
        #first, extract two not analyzed sentences from db (their encodings)
        #then, return their similarity
        first_doc = None
        second_doc = None
        similarity = self.scoring(first_doc,second_doc)
        # now add this similarity to db (need to think about what db collection to use)
    
    def encode_news(self):
        ###
        # TODO:
        #extract not processed news and compute the encoding
        #of their main paragraph
        ###
        document = None
        text_rank = self.text_rank(document)
        main_phrase = ' '.join(text_rank[0][1], text_rank[1][1], text_rank[2][1])
        self.BC.encode(main_phrase)
        ###
        # TODO:
        #update the db, adding the encoded paragraph






doc = """L’ultimo bollettino diffuso dalla Protezione civile evidenzia una consistente riduzione dei nuovi contagiati da Coronavirus. Il dato relativo alle nuove vittime registrate in un giorno è in calo rispetto a quello delle ultime rilevazioni. Nelle ultime 24 ore si registrano 274 nuove vittime (ieri erano 369, il giorno prima 236). A questo punto, il totale delle vittime in Italia è arrivato a toccare quota 30mila (29.958).

Continua a diminuire il numero degli “attualmente positivi” al virus. La serie con il segno meno davanti è cominciata una settimana fa, quando si è registrato un calo di -3.106 malati di Covid-19. A seguire: sei giorni fa il decremento è stato di -608 pazienti, cinque giorni fa di -239, quattro giorni fa di -525, tre giorni fa di -199, due giorni fa di -1.513, ieri di -6.939 e oggi di -1904 in 24 ore. Cifra che porta al ribasso il totale degli attualmente positivi al virus fino a 89.624 (ieri erano in totale 91.528 ).

Complessivamente, i casi totali di persone colpite dal Covid-19 dall’inizio del monitoraggio dell’epidemia sono arrivati a quota 215.858 (ieri era di 214.457), con un incremento di +1.401 in un giorno (sostanzialmente analogo a quello di ieri di ++1.444). Quanto ai tamponi, ne sono stati effettuati 70.359 in 24 ore (ieri il dato era di 64.263 tamponi effettuati), il totale dei test è dunque arrivato a quota 2.381.288, per un totale di casi testati di 1.563.557."""

pprint(textrank(doc))