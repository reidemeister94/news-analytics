# -*- coding: utf-8 -*-
"""Triples from Other.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1ewYvSG8NLkOEUZPfilYob7Fuf-EY_1RY
"""

import spacy
from spacy import displacy
import neuralcoref
import logging
import sys
import os


class TripleExtraction:

    # dependency markers for subjects
    SUBJECTS = {"nsubj", "nsubjpass", "csubj", "csubjpass", "agent"}  # , "expl"}
    # dependency markers for objects
    OBJECTS = {"dobj", "dative", "attr", "oprd", "pobj"}
    # POS tags that will break adjoining items
    BREAKER_POS = {"VERB"}  # , "CCONJ"}
    # words that are negations
    NEGATIONS = {"no", "not", "n't", "never", "none"}

    def __init__(self, nlp_model=None):
        if nlp_model is not None:
            self.nlp = nlp_model
        else:
            # self.nlp = spacy.load("en_core_web_lg")
            self.nlp = spacy.load("en_core_web_md")

        # if "neuralcoref" not in self.nlp.pipe_names:
        #     coref = neuralcoref.NeuralCoref(self.nlp.vocab)
        #     self.nlp.add_pipe(coref, name="neuralcoref")
        self.LOGGER = self.__get_logger()
        self.LOGGER.info("=" * 120)
        self.LOGGER.info("Triple Extraction Ready")

    # does dependency set contain any coordinating conjunctions?
    def contains_conj(self, depSet):
        return (
            "and" in depSet
            or "or" in depSet
            or "nor" in depSet
            or "but" in depSet
            or "yet" in depSet
            or "so" in depSet
            or "for" in depSet
        )

    # get subs joined by conjunctions
    def _get_subs_from_conjunctions(self, subs):
        more_subs = []
        for sub in subs:
            # rights is a generator
            rights = list(sub.rights)
            rightDeps = {tok.lower_ for tok in rights}
            if self.contains_conj(rightDeps):
                more_subs.extend(
                    [tok for tok in rights if tok.dep_ in self.SUBJECTS or tok.pos_ == "NOUN"]
                )
                if len(more_subs) > 0:
                    more_subs.extend(self._get_subs_from_conjunctions(more_subs))

        return more_subs

    # get objects joined by conjunctions
    def _get_objs_from_conjunctions(self, objs):
        more_objs = []
        for obj in objs:
            # rights is a generator
            rights = list(obj.rights)
            rightDeps = {tok.lower_ for tok in rights}
            if self.contains_conj(rightDeps):
                more_objs.extend(
                    [tok for tok in rights if tok.dep_ in self.OBJECTS or tok.pos_ == "NOUN"]
                )
                if len(more_objs) > 0:
                    more_objs.extend(self._get_objs_from_conjunctions(more_objs))
        return more_objs

    # find sub dependencies
    def _find_subs(self, tok):
        head = tok.head
        while head.pos_ != "VERB" and head.pos_ != "NOUN" and head.head != head:
            head = head.head
        if head.pos_ == "VERB":
            subs = [tok for tok in head.lefts if tok.dep_ == "SUB"]
            if len(subs) > 0:
                subs.extend(self._get_subs_from_conjunctions(subs))
                return subs
            elif head.head != head:
                return self._find_subs(head)
        elif head.pos_ == "NOUN":
            return [head]
        return []

    # is the tok set's left or right negated?
    def _is_negated(self, tok):
        parts = list(tok.lefts) + list(tok.rights)
        for dep in parts:
            if dep.lower_ in self.NEGATIONS:
                return True
        return False

    # get grammatical objects for a given set of dependencies (including passive sentences)
    def _get_objs_from_prepositions(self, deps, is_pas):
        objs = []
        for dep in deps:
            if dep.pos_ == "ADP" and (dep.dep_ == "prep" or (is_pas and dep.dep_ == "agent")):
                objs.extend(
                    [
                        tok
                        for tok in dep.rights
                        if tok.dep_ in self.OBJECTS
                        or (tok.pos_ == "PRON" and tok.lower_ == "me")
                        or (is_pas and tok.dep_ == "pobj")
                    ]
                )
        return objs

    # get all functional subjects adjacent to the verb passed in
    def _get_all_subs(self, v):
        subs = [tok for tok in v.lefts if tok.dep_ in self.SUBJECTS]  # and tok.pos_ != "DET"]

        if len(subs) > 0:
            subs.extend(self._get_subs_from_conjunctions(subs))
        else:
            foundSubs = self._find_subs(v)
            subs.extend(foundSubs)

        correct_subs = []
        for sub in subs:
            found = False
            true_subject = None
            for child in sub.children:
                if child.text == "of":
                    found = True
                    for c in child.children:
                        true_subject = c
                        break

            if found and true_subject.text != "of" and true_subject.text is not None:
                correct_subs.append(true_subject)
            elif not found:
                correct_subs.append(sub)

        return correct_subs

    # is the token a verb?  (include auxiliary verbs)
    def _is_verb_or_aux(self, tok):
        return (
            tok.pos_ == "VERB"
            and (tok.dep_ != "aux" and tok.dep_ != "auxpass")
            or tok.pos_ == "AUX"
            and (tok.dep_ != "aux" and tok.dep_ != "auxpass" and tok.dep_ != "xcomp")
        )

    # return the verb to the right of this verb in a CCONJ relationship if applicable
    # returns a tuple, first part True|False and second part the modified verb if True
    def _right_of_verb_is_conj_verb(self, v):
        # rights is a generator
        rights = list(v.rights)

        # VERB CCONJ VERB (e.g. he beat and hurt me)
        if len(rights) > 1 and rights[0].pos_ == "CCONJ":
            for tok in rights[1:]:
                if self._is_verb_or_aux(tok):
                    return True, tok

        if len(rights) > 0:
            for r in rights:
                if r.dep_ == "xcomp" or r.dep_ == "conj":
                    return True, r

        return False, v

    # get all objects for an active/passive sentence
    def _get_all_objs(self, v, is_pas):
        # rights is a generator
        rights = list(v.rights)

        objs = [
            tok for tok in rights if tok.dep_ in self.OBJECTS
        ]  # or (is_pas and tok.dep_ == 'pobj')]
        if objs == []:
            objs.extend(self._get_objs_from_prepositions(rights, is_pas))

        # potential_new_verb, potential_new_objs =
        # _get_obj_from_xcomp(rights, is_pas)
        # if potential_new_verb is not None
        # and potential_new_objs is not None
        # and len(potential_new_objs) > 0:
        #     objs.extend(potential_new_objs)
        #     v = potential_new_verb
        if len(objs) > 0:
            objs.extend(self._get_objs_from_conjunctions(objs))

        correct_objs = []
        for obj in objs:
            found = False
            true_object = None
            for child in obj.children:
                if child.text == "of":
                    found = True
                    for c in child.children:
                        true_object = c
                        break

            if found and true_object.text != "of" and true_object.text is not None:
                correct_objs.append(true_object)
            elif not found:
                correct_objs.append(obj)

        return v, correct_objs

    # return true if the sentence is passive -
    # at he moment a sentence is assumed passive
    # if it has an auxpass verb
    def _is_passive(self, tokens):
        for tok in tokens:
            if tok.dep_ == "auxpass":
                return True
        return False

    # convert a list of tokens to a string
    def to_str(self, tokens):
        tmp_text = ""
        first_time = True

        for item in tokens:
            if not first_time and item.text not in self.NEGATIONS:
                tmp_text += " "
            if first_time:
                first_time = False
            tmp_text += item.text

        return tmp_text

    def is_subtext(self, text, svos):
        sub_subject = False
        sub_verb = False
        sub_object = False

        for svo in svos:
            if text[0] in svo[0]:
                sub_subject = True

            if text[1] in svo[1]:
                sub_verb = True

            if text[2] in svo[2]:
                sub_object = True

        if sub_subject and sub_verb and sub_object:
            return True
        else:
            return False

    def populate_svos_array(self, verb, is_pas, sub, svos, tokens, visited, isConj, conjVerb):

        try:
            current_verb, objs = self._get_all_objs(verb, is_pas)
            if objs == [] and isConj:
                _, objs = self._get_all_objs(conjVerb, is_pas)

            verbNegated = self._is_negated(verb)

            for obj in objs:
                negation = ""
                # objNegated = _is_negated(obj)
                if verbNegated:
                    negation = "not "
                if is_pas:
                    first = [obj]  # expand(obj, tokens, visited)
                    second = [sub]  # expand(sub, tokens, visited)

                    if not (len(first) == 1 and first[0].pos_ == "DET"):
                        text = (
                            self.to_str(first),
                            negation + current_verb.lemma_,
                            self.to_str(second),
                        )  # to_str([current_verb])

                        if text not in svos and not self.is_subtext(text, svos):
                            svos.append(text)
                else:
                    first = [sub]  # expand(sub, tokens, visited)
                    second = [obj]  # expand(obj, tokens, visited)

                    if not (len(first) == 1 and first[0].pos_ == "DET"):
                        text = (
                            self.to_str(first),
                            negation + current_verb.lemma_,
                            self.to_str(second),
                        )  # get_complete_verb(current_verb)

                        if text not in svos and not self.is_subtext(text, svos):
                            svos.append(text)

        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            # print("{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno))
            self.LOGGER.error(
                "{}, {}, {}, {}".format(tokens["_id"], exc_type, fname, exc_tb.tb_lineno)
            )

    # find verbs and their subjects / objects to create SVOs, detect passive/active sentences
    def findSVOs(self, tokens):
        try:
            svos = []
            is_pas = self._is_passive(tokens)
            verbs = [tok for tok in tokens if self._is_verb_or_aux(tok)]
            for v in verbs:
                visited = set()  # recursion detection
                subs = self._get_all_subs(v)
                # hopefully there are subs, if not, don't examine this verb any longer
                current_verb = v
                isConjVerb = True

                if len(subs) > 0:
                    while isConjVerb:
                        isConjVerb, conjV = self._right_of_verb_is_conj_verb(current_verb)
                        if isConjVerb:
                            # get all objects on the right of the second verb

                            for sub in subs:
                                # if is_pas:  # reverse object / subject for passive
                                if conjV.dep_ != "xcomp":
                                    self.populate_svos_array(
                                        current_verb,
                                        is_pas,
                                        sub,
                                        svos,
                                        tokens,
                                        visited,
                                        isConjVerb,
                                        conjV,
                                    )
                                    self.populate_svos_array(
                                        conjV,
                                        is_pas,
                                        sub,
                                        svos,
                                        tokens,
                                        visited,
                                        isConjVerb,
                                        conjV,
                                    )

                                else:
                                    self.populate_svos_array(
                                        current_verb,
                                        is_pas,
                                        sub,
                                        svos,
                                        tokens,
                                        visited,
                                        isConjVerb,
                                        conjV,
                                    )

                        else:
                            for sub in subs:
                                self.populate_svos_array(
                                    current_verb,
                                    is_pas,
                                    sub,
                                    svos,
                                    tokens,
                                    visited,
                                    isConjVerb,
                                    conjV,
                                )

                        current_verb = conjV

            return svos

        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            # print("{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno))
            self.LOGGER.error(
                "{}, {}, {}, {}".format(tokens["_id"], exc_type, fname, exc_tb.tb_lineno)
            )

    def resolve_coreferences(self, original_text):
        try:
            doc = self.nlp(original_text)

            pos_to_change = []
            new_terms = []

            if doc._.has_coref:
                for coref in doc._.coref_clusters:
                    for c in coref:
                        if c != coref[0]:
                            if (
                                not (len(coref[0]) == 1 and coref[0][0].dep_ == "poss")
                                or len(coref[0]) > 1
                            ):
                                pos_to_change.append(c.start)
                                pos_to_change.append(c.end)
                                new_terms.append(coref[0])

                if new_terms != []:
                    new_doc = []
                    begin_pos = 0
                    end_pos = pos_to_change[0]
                    pos_to_change.remove(pos_to_change[0])

                    while True:
                        new_doc.append(doc[begin_pos:end_pos].text)
                        if len(new_terms) > 0:
                            new_doc.append(new_terms[0].text)
                            new_terms.remove(new_terms[0])

                        if len(pos_to_change) == 0:
                            break

                        if len(pos_to_change) > 1:
                            begin_pos = pos_to_change[0]
                            end_pos = pos_to_change[1]
                            pos_to_change.remove(pos_to_change[0])
                            pos_to_change.remove(pos_to_change[0])

                        elif len(pos_to_change) == 1:
                            begin_pos = pos_to_change[0]
                            end_pos = len(doc)
                            pos_to_change.remove(pos_to_change[0])

                    new_text = ""
                    for el in new_doc:
                        new_text += el.replace("\\", "")
                        new_text += " "

                    sentence_array = new_text.split(". ")
                    if "" in sentence_array:
                        sentence_array.remove("")
                else:
                    sentence_array = original_text.split(". ")
                    if "" in sentence_array:
                        sentence_array.remove("")
            else:
                sentence_array = original_text.split(". ")
                if "" in sentence_array:
                    sentence_array.remove("")

            return sentence_array

        except Exception:
            exc_type, _, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            # print("{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno))
            self.LOGGER.error(
                "{}, {}, {}, {}".format(doc["_id"], exc_type, fname, exc_tb.tb_lineno)
            )

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("TripleExtraction")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        log_path = "core_modules/log/triple_extraction.log"
        if not os.path.isdir("core_modules/log/"):
            os.mkdir("core_modules/log/")
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger

    def perform_triples_extraction(self, p_array=None):
        if p_array is None:
            return []
        else:
            paragraph_array = p_array
        res = []
        for paragraph in paragraph_array:

            # Soluzione tappa-buchi
            # Funzione da chiamare poi:
            # original_text_array = self.resolve_coreferences(paragraph)
            original_text_array = paragraph.split(". ")
            if "" in original_text_array:
                original_text_array.remove("")
            # print(paragraph)
            # ========

            for text in original_text_array:
                try:
                    # print(text, end="\n")

                    # displacy.render(nlp(text), style = "dep", jupyter = True)
                    svo = self.findSVOs(self.nlp(text))

                    # for s in svo:
                    #     print("  ", s[0], "-", s[1], "-", s[2])

                    # print("")
                    res.append(svo)

                except Exception:
                    exc_type, _, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    # print("{},{},{},{}".format(doc["_id"],exc_type,fname,exc_tb.tb_lineno))
                    self.LOGGER.error(
                        "{}, {}, {}, {}".format(
                            self.nlp(text)["_id"], exc_type, fname, exc_tb.tb_lineno
                        )
                    )
                    return None
            return res


if __name__ == "__main__":
    triple_extraction = TripleExtraction()
