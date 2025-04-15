import logging
import os
import sqlite3

import pandas as pd
from rapidfuzz import fuzz, process

from scholaridreconciler.models.scholar import Scholar
from scholaridreconciler.services.affiliation_segregation import AffiliationSegregate
from scholaridreconciler.services.countries_stopwords import countries_wikidata
from scholaridreconciler.services.organisation_preprocessing import OrganisationPreprocessing

# create a organisation dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
db_path = os.getenv("DATABASE_PATH",
                    os.path.join(os.path.dirname(os.path.abspath(__file__)),".cache"
                                 ,"db", "organisation_data.db"))

# Ensure the directory exists
os.makedirs(os.path.dirname(db_path), exist_ok=True)
print(f"Connecting to database at: {db_path}")
connection = sqlite3.connect(db_path,check_same_thread=False)




class OrganisationSearch:
    
    def __init__(self, scholar:Scholar):
        self._df = pd.DataFrame({})  # Initialize an empty DataFrame
        self._scholar = scholar
        self._connection = connection
        self._process_word = OrganisationPreprocessing(self._df)
        self._original = self._scholar.affiliation_raw.lower()
        self._aff_seg = AffiliationSegregate(self._scholar)
        self._aff_seg.collect_countries()
        self._country = self._aff_seg.countries
        self._country_uri = []
        self._affiliation_index = {}
        

    def detect_countryuri(self):
        for i in range(len(self._country)):
            self._country_uri.append(countries_wikidata[self._country[i]])
        return self._country_uri

    def search_with_no_processing(self):
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE org LIKE ?"""
        new_df = pd.read_sql(query, self._connection, params=(f'%{self._original}%',))
        self._df = pd.concat([self._df, new_df], ignore_index=True)
        return len(self._df) > 0

    def search_with_important_words(self):
        org = self._process_word.remove_freqWords(self._original)
        org_split = org.split()
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE ImpWord LIKE ?"""

        for word in org_split:
            new_df = pd.read_sql(query, self._connection, params=(f'%{word}%',))
            self._df = pd.concat([self._df, new_df], ignore_index=True)

        return len(self._df) > 0
            

    def search_with_abbreviation(self):
        org = self._process_word.abbreviation(self._original)
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE abv_org LIKE ?"""
        new_df = pd.read_sql(query, self._connection, params=(f'{org}%',))
        self._df = pd.concat([self._df, new_df], ignore_index=True)

        # Reverse abbreviation
        rev_org = org[::-1]  
        query = """SELECT uri, ImpWord abv_org FROM organisation_with_loc WHERE abv_org LIKE ?"""
        new_df = pd.read_sql(query, self._connection, params=(f'{rev_org}%',))
        self._df = pd.concat([self._df, new_df], ignore_index=True)
        return len(self._df) > 0

    def search_with_abbreviation_with_dot(self):
        org = self._process_word.abbreviation_with_dot(self._original)
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE abv_org_with_dot LIKE ?"""
        new_df = pd.read_sql(query, self._connection, params=(f'{org}%',))
        self._df = pd.concat([self._df, new_df], ignore_index=True)

        rev_org = org[::-1]
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE abv_org_with_dot LIKE ?"""
        new_df = pd.read_sql(query, self._connection, params=(f'{rev_org}%',))
        self._df = pd.concat([self._df, new_df], ignore_index=True)
        return len(self._df) > 0


    def search_with_no_processing_and_country(self):
        
        
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE  countryuri = ? and org LIKE ?"""
        new_df = pd.read_sql(query, self._connection, params=(self._country_uri[-1],f'%{self._original}%',))
        self._df = pd.concat([self._df, new_df], ignore_index=True)
        
        return len(self._df) > 0

    def search_with_important_words_and_country(self):
        
        org = self._process_word.remove_freqWords(self._original)
        org_split = org.split()
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE countryuri = ? and ImpWord LIKE ?"""

        for word in org_split:
            new_df = pd.read_sql(query, self._connection, params=(self._country_uri[-1],f'%{word}%',))
            self._df = pd.concat([self._df, new_df], ignore_index=True)
        return len(self._df) > 0
    
    def search_with_abbreviation_and_country(self):
        
        org = self._process_word.abbreviation(self._original)
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE countryuri = ? and  abv_org LIKE ?"""
        new_df = pd.read_sql(query, self._connection, params=(self._country_uri[-1],f'{org}%',))
        self._df = pd.concat([self._df, new_df], ignore_index=True)

        # Reverse abbreviation
        rev_org = org[::-1]  
        query = """SELECT uri, ImpWord abv_org FROM organisation_with_loc WHERE countryuri = ? and abv_org LIKE ?"""
        new_df = pd.read_sql(query, self._connection, params=(self._country_uri[-1],f'{rev_org}%',))
        self._df = pd.concat([self._df, new_df], ignore_index=True)
        return len(self._df) > 0

    def search_with_abbreviation_with_dot_and_country(self):
        org = self._process_word.abbreviation_with_dot(self._original)
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE countryuri = ? and abv_org_with_dot LIKE ?"""
        new_df = pd.read_sql(query, self._connection, params=(self._country_uri[-1],f'{org}%',))
        self._df = pd.concat([self._df, new_df], ignore_index=True)

        rev_org = org[::-1]
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE countryuri = ? and abv_org_with_dot LIKE ?"""
        new_df = pd.read_sql(query, self._connection, params=(self._country_uri[-1],f'{rev_org}%',))
        self._df = pd.concat([self._df, new_df], ignore_index=True)
        return  len(self._df) > 0
        

    def fuzzy_matching_aff(self):

        choices = self._df['ImpWord'].unique()
        org = self._process_word.remove_freqWords(self._original)
        matches_affiliation = process.extract(query=org, choices=choices,
                                                scorer=fuzz.WRatio,
                                                score_cutoff=80, limit=None)

        for org, score, _ in matches_affiliation:
            index = self._df.loc[self._df.ImpWord == org].uri.to_numpy()
            if len(index) > 0:
                for i in range(len(index)):
                    index_no = index[i]

                    self._affiliation_index.update({index_no: score})

    def selection_preference(self):
        if self.detect_countryuri():
            if self.search_with_no_processing_and_country():
                self.fuzzy_matching_aff()
                return
            if self.search_with_important_words_and_country():
                self.fuzzy_matching_aff()
                return
            if self.search_with_abbreviation_and_country():
                self.fuzzy_matching_aff()
                return
            if self.search_with_abbreviation_with_dot_and_country():
                self.fuzzy_matching_aff()
                return
            
        else:
            if self.search_with_no_processing():
                self.fuzzy_matching_aff()
                return
            if self.search_with_important_words():
                self.fuzzy_matching_aff()
                return
            if self.search_with_abbreviation():
                self.fuzzy_matching_aff()
                return
            if self.search_with_abbreviation_with_dot():
                self.fuzzy_matching_aff()
                return
        
        

scholar = Scholar(affiliation_raw="RWTH Aache University")
org = OrganisationSearch(scholar)
org.selection_preference()
print(org._affiliation_index)











