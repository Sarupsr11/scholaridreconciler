
from typing import Any

import pandas as pd

from scholaridreconciler.models.scholar import Scholar
from scholaridreconciler.services.countries_stopwords import countries_wikidata
from scholaridreconciler.services.organisation_preprocessing import OrganisationPreprocessing


class AffiliationSegregate:

    def __init__(self, scholar:Scholar):
        self._scholar = scholar
        self._countries: list[Any] = []

    
        

    def collect_countries(self):
        processor = OrganisationPreprocessing(df = pd.DataFrame({}))
        org = processor.remove_special_chars(self._scholar.affiliation_raw.lower())
        words = org.split()
        for word in words:
            if word in countries_wikidata:
                self._countries.append(word.strip())

    def collect_each_part(self):
        words = self._scholar.affiliation_raw.split(",")
        collect_word = []
        for word in words:
            if word.lower().strip() not in countries_wikidata:             
                collect_word.append(word.strip())
                
            
        return collect_word


