
import numpy as np
import pandas as pd
from countries_stopwords import countries_wikidata
from nltk.probability import FreqDist


def preprocess_organisation(organisation_dataframe:pd.DataFrame):
    tokens = [org.split() for org in organisation_dataframe['org'].dropna().unique()]
    flat_tokens = [word for sublist in tokens for word in sublist]
    fdist = FreqDist(flat_tokens)
    stopwords = fdist.most_common(100)
    stopwords_list = [common[0] for common in stopwords]
    stopwords_list.append(list(countries_wikidata.keys()))
    

    def filter_organization_name(name):
        if name is not np.nan:
            words = name.split()
            important_words = [word for word in words if word.lower() not in stopwords_list]
            return " ".join(important_words)
        else:
            return np.nan
    
    organisation_dataframe['processed_org'] = [filter_organization_name(name) 
                                               for name in organisation_dataframe['org'].to_numpy()]
    return organisation_dataframe, stopwords_list