from gensim.models import Phrases

import ast
import pickle
import pandas as pd
import json

class LdaUtils:

    def __init__(self):
        pass

    def get_word_collocations(self, tokens):
        '''
        Use the Phrases model to find sequences of 2 and 3 tokens that
        often apper together (e.g. 'european', 'union' -> 'european-union')
        '''
        bigrams = Phrases(tokens)
        trigrams = Phrases(bigrams[tokens], min_count=1)
        return list(trigrams[bigrams[tokens]])
        # return [self._string_to_list(tokens_list) for tokens_list in trigrams[bigrams[tokens]]]

    def topics_document_to_dataframe(self, topics_document, num_topics):
        '''
        Returns the topics for a given document in a Pandas dataframe
        '''
        res = pd.DataFrame(columns=range(num_topics))
        for topic_weight in topics_document:
            res.loc[0, topic_weight[0]] = topic_weight[1]
        return res

    def string_to_list(self, tokens):
        '''
        Old function used to fix the format of the get_word_collocations()
        return value
        '''
        return ast.literal_eval(tokens)

    def save_lda_model(self, ldaModule, location):
        # Test location
        #location = "./lda_model/lda_checkpoint"
        # chekpointfile = open(location, "wb")
        # pickle.dump(ldaModule.model, chekpointfile)
        # chekpointfile.close()
        # # save dictionary
        # with open(location + '_dictionary.json', 'w') as writer:
        #     json.dumps(dict(ldaModule.dictionary), writer)
        # # save corpus
        # with open(location + '_corpus.json', 'w') as writer:
        #     json.dumps(dict(ldaModule.corpus), writer)
        # return
        with open(location + '.pickle', 'wb') as output:
            pickle.dump(ldaModule, output, pickle.HIGHEST_PROTOCOL)
    
    def load_lda_model(self, location):
        # Test location
        #location = "./lda_model/lda_checkpoint"
        # checkpointfile = open(location,"rb")
        # loaded_lda = pickle.load(checkpointfile)
        # checkpointfile.close()
        # # load dictionary
        # with open(location + '_dictionary.json', 'r') as jsonfile:
        #     dictionary =  json.load(jsonfile)
        # # load corpus
        # with open(location + '_corpus.json', 'r') as jsonfile:
        #     corpus = json.load(jsonfile)
        # return loaded_lda, dictionary, corpus
        with open(location + '.pickle', 'rb') as input_file:
            ldaModule = pickle.load(input_file)
        return ldaModule


if __name__ == '__main__':
    lda_utils = LdaUtils()
