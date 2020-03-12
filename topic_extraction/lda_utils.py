from gensim.models import Phrases

import ast


class LdaUtils:

    def __init__(self):
        pass

    def get_word_collocations(self, tokens):
        bigrams = Phrases(tokens)
        trigrams = Phrases(bigrams[tokens], min_count=1)
        return list(trigrams[bigrams[tokens]])
        # return [self._string_to_list(tokens_list) for tokens_list in trigrams[bigrams[tokens]]]

    def topics_document_to_dataframe(self, topics_document, num_topics):
        res = pd.DataFrame(columns=range(num_topics))
        for topic_weight in topics_document:
            res.loc[0, topic_weight[0]] = topic_weight[1]
        return res

    def _string_to_list(self, tokens):
        return ast.literal_eval(tokens)


if __name__ == '__main__':
    lda_utils = LdaUtils()
