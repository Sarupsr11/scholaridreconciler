
from scholaridreconciler.models.scholar import Scholar
from scholaridreconciler.services.organisation_preprocessing import OrganisationPreprocessing
from scholaridreconciler.services.countries_stopwords import countries_wikidata

class AffiliationSegregate:

    def __init__(self, scholar:Scholar):
        self.scholar = scholar
        self.countries = []

    
        

    def collect_countries(self):
        processor = OrganisationPreprocessing()
        org = processor.remove_special_chars(self.scholar.affiliation_raw.lower())
        words = org.split()
        for word in words:
            if word in countries_wikidata:
                self.countries.append(word.strip())

    def collect_each_part(self):

        words = self.scholar.affiliation_raw.split(",")
        collect_word = []
        for word in words:
            if word.lower().strip() not in countries_wikidata:             
                collect_word.append(word.strip())
                
            
        return collect_word


        
