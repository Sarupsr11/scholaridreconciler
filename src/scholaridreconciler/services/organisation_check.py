import sqlite3
from typing import Any
import pandas as pd
import logging
import os
from rapidfuzz import fuzz, process
from scholaridreconciler.models.scholar import Scholar
from scholaridreconciler.services.organisation_preprocessing import OrganisationPreprocessing
from scholaridreconciler.services.countries_stopwords import countries_wikidata
from scholaridreconciler.services.affiliation_segregation import AffiliationSegregate
# create a organisation dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
db_path = os.getenv("DATABASE_PATH",
                    os.path.join(os.path.dirname(os.path.abspath(__file__)),".cache"
                                 ,"db", "organisation_data.db"))

# Ensure the directory exists
os.makedirs(os.path.dirname(db_path), exist_ok=True)
print(f"Connecting to database at: {db_path}")
connection = sqlite3.connect(db_path,check_same_thread=False)



import pandas as pd

class OrganisationSearch:
    
    def __init__(self, scholar:Scholar):
        self.df = pd.DataFrame({})  # Initialize an empty DataFrame
        self.scholar = scholar
        self.connection = connection
        self.process_word = OrganisationPreprocessing()
        self.original = self.scholar.affiliation_raw.lower()
        aff_seg = AffiliationSegregate(self.scholar)
        aff_seg.collect_countries()
        self.country = aff_seg.countries
        self.country_uri = []
        self.affiliation_index = {}
        

    def detect_countryuri(self):
        for i in range(len(self.country)):
            self.country_uri.append(countries_wikidata[self.country[i]])
        return self.country_uri

    def search_with_no_processing(self):
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE org LIKE ?"""
        new_df = pd.read_sql(query, self.connection, params=(f'%{self.original}%',))
        self.df = pd.concat([self.df, new_df], ignore_index=True)
        if len(self.df) > 0:
            return True
        else:
            return False

    def search_with_important_words(self):
        org = self.process_word.remove_freqWords(self.original)
        org_split = org.split()
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE ImpWord LIKE ?"""

        for word in org_split:
            new_df = pd.read_sql(query, self.connection, params=(f'%{word}%',))
            self.df = pd.concat([self.df, new_df], ignore_index=True)

        if len(self.df) > 0:
            return True
        else:
            return False

    def search_with_abbreviation(self):
        org = self.process_word.abbreviation(self.original)
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE abv_org LIKE ?"""
        new_df = pd.read_sql(query, self.connection, params=(f'{org}%',))
        self.df = pd.concat([self.df, new_df], ignore_index=True)

        # Reverse abbreviation
        rev_org = org[::-1]  
        query = """SELECT uri, ImpWord abv_org FROM organisation_with_loc WHERE abv_org LIKE ?"""
        new_df = pd.read_sql(query, self.connection, params=(f'{rev_org}%',))
        self.df = pd.concat([self.df, new_df], ignore_index=True)
        if len(self.df) > 0:
            return True
        else:
            return False

    def search_with_abbreviation_with_dot(self):
        org = self.process_word.abbreviation_with_dot(self.original)
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE abv_org_with_dot LIKE ?"""
        new_df = pd.read_sql(query, self.connection, params=(f'{org}%',))
        self.df = pd.concat([self.df, new_df], ignore_index=True)

        rev_org = org[::-1]
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE abv_org_with_dot LIKE ?"""
        new_df = pd.read_sql(query, self.connection, params=(f'{rev_org}%',))
        self.df = pd.concat([self.df, new_df], ignore_index=True)
        if len(self.df) > 0:
            return True
        else:
            return False


    def search_with_no_processing_and_country(self):
        
        
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE  countryuri = ? and org LIKE ?"""
        new_df = pd.read_sql(query, self.connection, params=(self.country_uri[-1],f'%{self.original}%',))
        self.df = pd.concat([self.df, new_df], ignore_index=True)
        
        if len(self.df) > 0 :
            return True
        else:
            return False

    def search_with_important_words_and_country(self):
        
        org = self.process_word.remove_freqWords(self.original)
        org_split = org.split()
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE countryuri = ? and ImpWord LIKE ?"""

        for word in org_split:
            new_df = pd.read_sql(query, self.connection, params=(self.country_uri[-1],f'%{word}%',))
            self.df = pd.concat([self.df, new_df], ignore_index=True)
        if len(self.df) > 0:
            return True
        else:
            return False

    def search_with_abbreviation_and_country(self):
        
        org = self.process_word.abbreviation(self.original)
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE countryuri = ? and  abv_org LIKE ?"""
        new_df = pd.read_sql(query, self.connection, params=(self.country_uri[-1],f'{org}%',))
        self.df = pd.concat([self.df, new_df], ignore_index=True)

        # Reverse abbreviation
        rev_org = org[::-1]  
        query = """SELECT uri, ImpWord abv_org FROM organisation_with_loc WHERE countryuri = ? and abv_org LIKE ?"""
        new_df = pd.read_sql(query, self.connection, params=(self.country_uri[-1],f'{rev_org}%',))
        self.df = pd.concat([self.df, new_df], ignore_index=True)
        if len(self.df) > 0:
            return True
        else:
            return False

    def search_with_abbreviation_with_dot_and_country(self):
        org = self.process_word.abbreviation_with_dot(self.original)
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE countryuri = ? and abv_org_with_dot LIKE ?"""
        new_df = pd.read_sql(query, self.connection, params=(self.country_uri[-1],f'{org}%',))
        self.df = pd.concat([self.df, new_df], ignore_index=True)

        rev_org = org[::-1]
        query = """SELECT uri, ImpWord FROM organisation_with_loc WHERE countryuri = ? and abv_org_with_dot LIKE ?"""
        new_df = pd.read_sql(query, self.connection, params=(self.country_uri[-1],f'{rev_org}%',))
        self.df = pd.concat([self.df, new_df], ignore_index=True)
        if len(self.df) > 0:
            return True
        else:
            return False
        

    def fuzzy_matching_aff(self):

        choices = self.df['ImpWord'].unique()
        org = self.process_word.remove_freqWords(self.original)
        matches_affiliation = process.extract(query=org, choices=choices,
                                                scorer=fuzz.WRatio,
                                                score_cutoff=80, limit=None)

        for org, score, _ in matches_affiliation:
            index = self.df.loc[self.df.ImpWord == org].uri.to_numpy()
            if len(index) > 0:
                for i in range(len(index)):
                    index_no = index[i]

                    self.affiliation_index.update({index_no: score})

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
        
        

# scholar = Scholar(affiliation_raw="RWTH Aache University")
# org = OrganisationSearch(scholar)
# org.selection_preference()
# print(org.affiliation_index)











