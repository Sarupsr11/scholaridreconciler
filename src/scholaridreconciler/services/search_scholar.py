import logging
from typing import Any

from scholaridreconciler.models.scholar import Scholar
from scholaridreconciler.services.query_dblp import DblpSearch
from scholaridreconciler.services.query_wikidata_directly import WikidataSearch
from scholaridreconciler.services.scholar_affiliation import ScholarRetrieve
from scholaridreconciler.services.scholar_scoring import FuzzyScoring
from scholaridreconciler.utils import ConfidenceScoring

logging.basicConfig(level=logging.INFO)

class SearchScholar:

    def __init__(self,scholar: Scholar, bias = 'wikidata'):
        self._scholar = scholar
        self._scholar_list: list[Any] = []
        self._dblp_enhanced_scholar: list[Any] = []
        self._bias = bias
        self._result = None
        self._final_log = None

    def enrichment(self):
        dblp_scholar = DblpSearch(self._scholar)
        self._dblp_enhanced_scholar.extend(dblp_scholar.get_scholar_list_dblp())
        dblp_scholar = []
        log_list = []
        for scholar in self._dblp_enhanced_scholar:
            wiki_scholar = WikidataSearch(scholar)
            scholar_list = wiki_scholar.get_scholar_list()
            confidence = ConfidenceScoring(scholar, scholar_list)
            max_scholar, log = confidence.get_final_result()
            log_list.append(log)
            dblp_scholar.append(max_scholar)
        
        if dblp_scholar:
            max_score = dblp_scholar[0].confidence_score
            index = 0
            for i, scholar in enumerate(dblp_scholar):
                if scholar.confidence_score > max_score:
                    max_score = scholar.confidence_score
                    result = scholar
                    index = i
            self._result, self._final_log =  result, log_list[index]
        else:
            self._result = None

    def wikidata_search(self):
        wiki_scholar = WikidataSearch(self._scholar)
        scholar_list = wiki_scholar.get_scholar_list()
        if scholar_list:
            confidence = ConfidenceScoring(self._scholar, scholar_list)
            self._result, self._final_log = confidence.get_final_result()
        else:
            self._result = None
    
    def fuzzy_search(self):
        scholar = ScholarRetrieve(self._scholar)
        scholar_df = scholar.execute()
        fuzzy = FuzzyScoring(self._scholar, scholar_df)
        
        return fuzzy.return_topk()

    def search(self):
        if self._bias == 'wikidata':
            logging.info("Searching in Wikidata")
            self.wikidata_search()
            if self._result is None:
                logging.info("Searching in DBLP")
                self.enrichment()
                if self._result is None:
                    logging.info("Fuzzy Search")
                    self._result = self.fuzzy_search()
                else:
                    self._result = None
        elif self._bias == 'dblp':
            logging.info("Searching in DBLP")
            self.enrichment()
            if self._result is None:
                logging.info("Fuzzy Search")
                self._result = self.fuzzy_search()
            else:
                self._result = None
         
        
        
                

            

    
    
            





scholar = Scholar(name="Stefan Decker", first_name="Josef", family_name="Decker",
                  affiliation_raw="RWTH Aache University",)

search_scholar = SearchScholar(scholar)
search_scholar.search()
print(search_scholar._result)
print(search_scholar._final_log)