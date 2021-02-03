import sys

sys.path.append("../..")

import spacy
import ast
import os

import pyLDAvis.gensim
import pyLDAvis

from gensim.models import Phrases
from gensim import corpora, models

from pymongo import MongoClient
from itertools import chain
from datetime import datetime

from core_modules.topic_extraction.lda_module import LdaModule

try:
    import pickle
except Exception:
    import pickle5 as pickle

"""
    Database functions
"""


def db_news_extraction(client, lang, query, chunk_size, limit=0):
    if lang != "it":
        name_coll = "article_" + lang
    else:
        name_coll = "article"
    collection = client["news"][name_coll]

    not_processed_docs = collection.find(
        query, no_cursor_timeout=True, batch_size=chunk_size
    ).limit(limit)
    return collection, not_processed_docs


def build_query(start, end):
    q = {"discoverDate": {"$gte": start, "$lt": end}}
    return q


def yield_rows(cursor, chunk_size):
    """
    Generator to yield chunks from cursor
    :param cursor:
    :param chunk_size:
    :return:
    """
    chunk = []
    for i, row in enumerate(cursor):
        if i % chunk_size == 0 and i > 0:
            yield chunk
            del chunk[:]
        chunk.append(row)
    yield chunk


def update_dates(start, end):
    start_month = start.month + 1
    start_year = start.year
    end_month = end.month + 1
    end_year = end.year
    if start_month == 13:
        start_month = 1
        start_year = start.year + 1
    if end_month == 13:
        end_month = 1
        end_year = end.year + 1
    new_start = datetime(start_year, start_month, 1, 0, 0)
    new_end = datetime(end_year, end_month, 1, 0, 0)
    return new_start, new_end


"""
    Documents parsing
"""


def parse_text(nlp, raw_data):
    doc = nlp(raw_data)
    # Retrieve sentences
    sentences = sentence_tokenize(doc)
    # print(len(sentences))
    # Lemmatize + remove stop words
    lemmas = lemmatize_tokens(nlp, sentences)
    # print(len(lemmas))
    # Flatten results into a single list
    parsed_text = flatten_list(lemmas)

    return parsed_text


def fix_stop_words(lang, nlp):
    for word in nlp.Defaults.stop_words:
        nlp.vocab[word].is_stop = True
    if lang == "it":
        nlp.vocab["dio"].is_stop = True
    elif lang == "de":
        nlp.vocab["Prozent"].is_stop = True
    return


def add_custom_stop_words(nlp, custom_stop_words):
    for cw in custom_stop_words:
        nlp.vocab[cw].is_stop = True
    return


def sentence_tokenize(data):
    return [sent for sent in data.sents]


def lemmatize_tokens(nlp, data):
    lemmas = []
    for sent in data:
        sent_tokens = []
        for token in sent:
            candidate = token.lemma_.replace("’", "")
            if (
                not nlp.vocab[candidate].is_stop
                and not token.is_punct
                and len(candidate) > 1
                and not candidate.isspace()
            ):
                sent_tokens.append(candidate)
        lemmas.append(sent_tokens)
        sent_tokens = []
    return lemmas


def flatten_list(data):
    return list(chain.from_iterable(data))


"""
    LDA helper functions
"""


def get_word_collocations(tokens):
    bigrams = Phrases(tokens)
    trigrams = Phrases(bigrams[tokens], min_count=1)
    return list(trigrams[bigrams[tokens]])


def string_to_list(tokens):
    return ast.literal_eval(tokens)


def save_lda_model(ldaModule, location):
    with open(location + ".pickle", "wb") as output:
        pickle.dump(ldaModule, output, pickle.HIGHEST_PROTOCOL)


def load_lda_model(location):
    with open(location + ".pickle", "rb") as input_file:
        ldaModule = pickle.load(input_file)
    return ldaModule


def build_dictionary(doc_collection, use_collocations=True, doc_threshold=3):
    if use_collocations:
        doc_collection = get_word_collocations(doc_collection)
    else:
        doc_collection = [string_to_list(t) for t in doc_collection]

    dictionary = corpora.Dictionary(doc_collection)

    if doc_threshold > 0:
        dictionary.filter_extremes(no_below=doc_threshold)

    return dictionary


def build_corpus(doc_collection, dictionary):
    corpus = [dictionary.doc2bow(list_of_tokens) for list_of_tokens in doc_collection]
    return corpus


def build_lda_model(corpus, dictionary, num_topics=20, passes=4, alpha=0.01, eta=0.01):
    model = models.LdaModel(
        corpus,
        num_topics=num_topics,
        id2word=dictionary,
        passes=passes,
        alpha=[alpha] * num_topics,
        eta=[eta] * len(dictionary.keys()),
    )
    return model


def get_topics(model, corpus, num_docs):
    topics = [model[corpus[i]] for i in range(num_docs)]
    return topics


"""
    Save results
"""


def create_folders(base_folder, subfolder):
    try:
        os.mkdir("{}/{}".format(base_folder, subfolder))
    except Exception:
        print("{}/{} already exists".format(base_folder, subfolder))


def save_topics_vis(base_folder, subfolder, date, final_lda_model, corpus, dictionary):
    LDAvis_prepared = pyLDAvis.gensim.prepare(final_lda_model, corpus, dictionary)
    pyLDAvis.save_html(
        LDAvis_prepared,
        "{}/{}/lda_vis_{}.html".format(base_folder, subfolder, date.strftime("%m_%Y")),
    )


def main():

    mongourl = "mongodb://admin:adminpassword@localhost:27017"
    MONGO_CLIENT = MongoClient(mongourl)

    START_YEAR = 2020
    START_MONTH = 1
    END_YEAR = 2020
    END_MONTH = 2
    START = datetime(START_YEAR, START_MONTH, 1, 0, 0)
    END = datetime(END_YEAR, END_MONTH, 1, 0, 0)
    lang = "de"
    base_folder = "lda_testing_{}".format(lang)
    chunk_size = 5000

    a = 0.01
    b = 0.91
    passes = 20
    num_topics = 3

    try:
        os.mkdir(base_folder)
    except Exception:
        print("{} already exists".format(base_folder))

    if lang == "en":
        nlp = spacy.load("en_core_web_md")
    else:
        nlp = spacy.load("{}_core_news_md".format(lang))

    fix_stop_words(lang, nlp)

    while END.year <= 2020:
        # Get documents from DB and parse them
        documents = []
        query = build_query(START, END)
        _, not_processed_docs = db_news_extraction(
            MONGO_CLIENT, lang, query, chunk_size, limit=5000
        )
        chunks = yield_rows(not_processed_docs, chunk_size)
        for chunk in chunks:
            for doc in chunk:
                parsed_doc = parse_text(nlp, doc["text"])
                documents.append(parsed_doc)

        if len(documents) > 0:
            # Create folder structure
            subfolder = "run_{}".format(START.strftime("%m_%Y"))
            create_folders(base_folder, subfolder)

            # Build dictionary and corpus for LDA model
            dictionary = build_dictionary(documents)
            corpus = build_corpus(documents, dictionary)

            # Create topic model
            final_lda_model = build_lda_model(
                corpus, dictionary, num_topics=num_topics, passes=passes, alpha=a, eta=b
            )

            # Prepare and save LDA model visualization
            save_topics_vis(base_folder, subfolder, START, final_lda_model, corpus, dictionary)

        START, END = update_dates(START, END)
        not_processed_docs.close()


main()
