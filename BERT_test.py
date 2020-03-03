import numpy as np
from bert_serving.client import BertClient


def scoring(pair):
    import math
    query_vec_1, query_vec_2 = bc.encode(pair)
    cosine = np.dot(query_vec_1, query_vec_2) / \
        (np.linalg.norm(query_vec_1) * np.linalg.norm(query_vec_2))
    return 1 / (1 + math.exp(-100 * (cosine - 0.95)))


with BertClient(port=5555, port_out=5556, check_version=False) as bc:

    from sentence_pairs import Pairs
    print("Start testing")

    for i, p in enumerate(Pairs):
        print(p)
        print("Similarity of Pair {}: ".format(i + 1), scoring(p))
        print('=' * 75)
