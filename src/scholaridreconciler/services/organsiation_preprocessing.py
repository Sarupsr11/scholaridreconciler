
import pandas as pd
from nltk.probability import FreqDist

from scholaridreconciler.services.countries_stopwords import countries_wikidata


def preprocess_organisation(organisation_dataframe:pd.DataFrame):
    tokens = [org.split() for org in organisation_dataframe['org'].dropna().unique()]
    flat_tokens = [word for sublist in tokens for word in sublist]
    fdist = FreqDist(flat_tokens)
    stopwords = fdist.most_common(1000)
    stopwords_list = [common[0] for common in stopwords]
    stopwords_list.append(countries_wikidata)
    

    def filter_organization_name(name):
        if name is not None:
            name = name.replace(',','')
            words = name.strip().split()
            important_words = [word for word in words if word.lower() not in stopwords_list]
            return " ".join(important_words)
        else:
            return None
    
    organisation_dataframe['processed_org'] = [filter_organization_name(name) 
                                               for name in organisation_dataframe['org'].to_numpy()]
    return organisation_dataframe, stopwords_list