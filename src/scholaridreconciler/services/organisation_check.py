import sqlite3

import pandas as pd
from rapidfuzz import fuzz, process
from typing import Any
from scholaridreconciler.models.scholar import Scholar
from scholaridreconciler.services.countries_stopwords import countries_wikidata, stopwords

# create a organisation dataframe

connection = sqlite3.connect('src/scholaridreconciler/services/organisation_data.db')


def filter_organization_name(name):
    if name is not None:
        name_new = name.replace(',','')
        words = name_new.strip().split()
        important_words = [word.lower() for word in words if word.lower() not in countries_wikidata 
                           and word.lower() not in stopwords]

        return " ".join(important_words)
        
    else:
        return None 


def brute_search(organisation):

    affiliation_index: dict[Any,Any] = {}
    organisation_df = pd.DataFrame({})
    organisation_part = organisation.split()
    for word in organisation_part:
        query = "select uri, processed_org from organisation_with_loc where processed_org like ?;"
        organisation_df = pd.concat([organisation_df ,
                                     pd.read_sql(query, connection,params=(f'%{word}%',))],ignore_index=True)
    choices = organisation_df['processed_org'].unique()
    matches_affiliation = process.extract(query = organisation,choices = choices,
                                          scorer = fuzz.WRatio,score_cutoff=70,limit = None)
    for org, score, _ in matches_affiliation:
        index = organisation_df.loc[organisation_df.processed_org == org].uri.to_numpy()
        if len(index)>0:
            for i in range(len(index)):
                index_no = index[i]

                affiliation_index.update({index_no:score})       
    return affiliation_index




def top_organisation(scholar:Scholar):

    affiliation_index: dict[Any,Any] = {}

    if scholar.affiliation is not None:
        
        (organisation ,city, country ) = scholar.affiliation
        if country[1] is not None:
            country_qid = countries_wikidata[country[1].lower()]
            query = "select uri, org from organisation_with_loc where countryuri = ? ;"
            organisation_df = pd.read_sql(query, connection, params=(country_qid,))
            choices = organisation_df['org'].unique()
            matches_affiliation = process.extract(query = organisation[1].lower(),choices = choices,
                                                  scorer = fuzz.WRatio,score_cutoff=70,limit = None)

            if len(matches_affiliation)>0:
                for org, score, _ in matches_affiliation:
                    index = organisation_df.loc[organisation_df.org == org].uri.to_numpy()
                    if len(index)>0:
                        for i in range(len(index)):
                            index_no = index[i]

                            affiliation_index.update({index_no:score})

            else:
                affiliation_index = brute_search(filter_organization_name(organisation[1].lower()))


        else: 
            organisation_string = filter_organization_name(organisation[1].lower())
            query = "select uri, org from organisation_with_loc where processed_org like ?;"
            organisation_df = pd.read_sql(query, connection, params=(f'%{organisation_string}%',))
            choices = organisation_df['org'].unique()
            
            matches_affiliation = process.extract(query = organisation[1].lower(),
                                                  choices = choices, scorer = fuzz.WRatio,
                                                  score_cutoff=70,limit = None)

            if len(matches_affiliation)>0:
                for org, score, _ in matches_affiliation:
                    index = organisation_df.loc[organisation_df.org == org].uri.to_numpy()
                    if len(index)>0:
                        for i in range(len(index)):
                            index_no = index[i]

                            affiliation_index.update({index_no:score})

            else:
                affiliation_index = brute_search(filter_organization_name(organisation[1].lower()))


    elif scholar.affiliation_raw is not None:

        country_name: str| None = None
        affiliation_name = scholar.affiliation_raw.replace(',','')
        for word in affiliation_name.split():
            if word.lower() in countries_wikidata:
                country_name = word.lower()
                break
        if country_name is not None:
            if country_name in countries_wikidata:
                country_qid = countries_wikidata[country_name]
                query = "select uri, org from organisation_with_loc where countryuri = ? ;"
                organisation_df = pd.read_sql(query, connection, params=(country_qid,))
                choices = organisation_df['org'].unique()
                organisation_string = filter_organization_name(scholar.affiliation_raw)
                matches_affiliation = process.extract(query = organisation_string, 
                                                    scorer = fuzz.partial_ratio ,
                                                    choices = choices,score_cutoff=90,
                                                    limit=None)

                if len(matches_affiliation)>0:
                    for org, score, _ in matches_affiliation:
                        index = organisation_df.loc[organisation_df.org == org].uri.to_numpy()
                        if len(index)>0:
                            for i in range(len(index)):
                                index_no = index[i]

                                affiliation_index.update({index_no:score})
                
                else:
                    affiliation_index = brute_search(filter_organization_name(scholar.affiliation_raw.lower()))


        elif scholar.affiliation_raw is not None and len(scholar.affiliation_raw)>0:
            organisation_string = filter_organization_name(scholar.affiliation_raw.lower())
            query = "select uri,org from organisation_with_loc where processed_org like ?;"
            organisation_df = pd.read_sql(query, connection, params=(f'%{organisation_string}%',))
            
            choices = organisation_df['org'].unique()
            matches_affiliation = process.extract(query = scholar.affiliation_raw.lower() ,
                                                    choices = choices, scorer = fuzz.WRatio,
                                                    score_cutoff=70,limit = None)

            if len(matches_affiliation)>0:
                for org, score, _ in matches_affiliation:
                    index = organisation_df.loc[organisation_df.org == org].uri.to_numpy()
                    if len(index)>0:
                        for i in range(len(index)):
                            index_no = index[i]

                            affiliation_index.update({index_no:score})
                
            else:
                affiliation_index = brute_search(filter_organization_name(scholar.affiliation_raw.lower()))
        else:
            raise ValueError("Affiliation Placeholder is Empty.")


    return affiliation_index




