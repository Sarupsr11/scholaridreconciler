
import re
import pandas as pd
from scholaridreconciler.services.countries_stopwords import countries_wikidata, stopwords, useless_words




class OrganisationPreprocessing:

    def __init__(self, df : pd.DataFrame = pd.DataFrame({})):

        self.df = df
        
    def lowercase_all_words(self, org: str) -> str:

        """
        lowercase every word in an organisation string to avoid case senstivity.

        """    
        return org.lower()

    def remove_special_chars(self,org:str) -> str:
        """
        Remove special characters from the organisation name maintaing equal spacing between words.
        """
        org = self.lowercase_all_words(org)
        org = re.sub(r'[^A-Za-z0-9\s]', ' ', org).strip()
        org = re.sub(r'\s+', ' ', org)
        return org


    def remove_freqWords(self, org:str) -> str:
        """
        Remove frequent words from the organisation name.
        """
        
        org = self.remove_special_chars(org)
        words = org.strip().split()
        important_words = [word for word in words if word not in stopwords
                           and word not in countries_wikidata
                           and word not in useless_words]
        return " ".join(important_words)

    def abbreviation(self,org: str) -> str:

        """
        Abbreviate organisation name

        """
        org = self.remove_special_chars(org)
        words = org.split()
        abv_org = ""
        for word in words:
            abv_org += word[0]

        return abv_org

    def abbreviation_with_dot(self, org: str) -> str:
        """
        Abbreviate organisation name with dot in the middle of each character.

        """
        org = self.remove_special_chars(org)
        words = org.split()
        abv_org_with_dot = ""
        for word in words:
            abv_org_with_dot += word[0]+"."

        return abv_org_with_dot[:-1] 
    
   
    def extending_dataframe(self):

        self.df['ImpWord'] = [self.remove_freqWords(org) for org in self.df['org'].to_numpy()]
        self.df['abv_org'] = [self.abbreviation(org) for org in self.df['org'].to_numpy()]
        self.df['abv_org_with_dot'] = [self.abbreviation_with_dot(org) for org in self.df['org'].to_numpy()]

        return self.df











