import sqlite3

import numpy as np
import pandas as pd
import spacy
from countries_stopwords import countries_wikidata, stopwords
from rapidfuzz import fuzz, process

from scholaridreconciler.models.scholar import Scholar

# create a organisation dataframe

connection = sqlite3.connect('src/scholaridreconciler/services/organisation_data.db')




def filter_organization_name(name):
    if name is not np.nan:
        words = name.split()
        important_words = [word for word in words if word.lower() not in stopwords and words 
                           and word.lower() not in countries_wikidata]
        return " ".join(important_words)
    else:
        return np.nan 


def brute_search(organisation):
    affiliation_index: dict[any,any] = {}
    query = "select uri, org from organisation_with_loc;"
    organisation_df = pd.read_sql(query, connection)
    choices = organisation_df['org'].unique()
    matches_affiliation = process.extract(query = organisation,choices = choices,score_cutoff=80)
    for org, score, _ in matches_affiliation:
        index = organisation_df.loc[organisation_df.org == org].uri.to_numpy()
        if len(index)>0:
            for i in range(len(index)):
                index_no = index[i]

                affiliation_index.update({index_no:score})       
    return affiliation_index




def top_10_organisation(scholar:Scholar):

    affiliation_index: dict[any,any] = {}
    if scholar.affiliation is not None:
        
        (organisation ,city, country ) = scholar.affiliation
        if country[1] is not None:
            country_qid = countries_wikidata[country[1].lower()]
            query = "select uri, org from organisation_with_loc where countryuri = ? ;"
            organisation_df = pd.read_sql(query, connection, params=(country_qid,))
            choices = organisation_df['org'].unique()
            matches_affiliation = process.extract(query = organisation[1].lower(),choices = choices,score_cutoff=90)

            if len(matches_affiliation)>1:
                for org, score, _ in matches_affiliation:
                    index = organisation_df.loc[organisation_df.org == org].uri.to_numpy()
                    if len(index)>0:
                        for i in range(len(index)):
                            index_no = index[i]

                            affiliation_index.update({index_no:score})

            else:
                affiliation_index = brute_search(organisation[1].lower())


        else: 
            organisation_string = filter_organization_name(organisation[1].lower())
            query = "select uri, org from organisation_with_loc where processed_org like ?;"
            organisation_df = pd.read_sql(query, connection, params=(f'%{organisation_string}%',))
            choices = organisation_df['org'].unique()
            
            matches_affiliation = process.extract(query = organisation[1].lower(),
                                                  choices = choices, scorer= fuzz.WRatio,score_cutoff=90)

            if len(matches_affiliation)>1:
                for org, score, _ in matches_affiliation:
                    index = organisation_df.loc[organisation_df.org == org].uri.to_numpy()
                    if len(index)>0:
                        for i in range(len(index)):
                            index_no = index[i]

                            affiliation_index.update({index_no:score})

            else:
                affiliation_index = brute_search(organisation[1].lower())


    elif scholar.affiliation_raw is not None:
        nlp = spacy.load("en_core_web_sm")
        doc = nlp(scholar.affiliation_raw)
        for ent in doc.ents:
            if ent.label_ == 'GPE' and str(ent).lower() in countries_wikidata:
                break

        if str(ent).lower() in countries_wikidata:
            country_qid = countries_wikidata[str(ent).lower()]
            query = "select uri, org from organisation_with_loc where countryuri = ? ;"
            organisation_df = pd.read_sql(query, connection, params=(country_qid,))
            choices = organisation_df['org'].unique()
            organisation_string = filter_organization_name(scholar.affiliation_raw.lower())
            matches_affiliation = process.extract(query = organisation_string, 
                                                  scorer = fuzz.partial_ratio ,choices = choices,score_cutoff=90)

            if len(matches_affiliation)>1:
                for org, score, _ in matches_affiliation:
                    index = organisation_df.loc[organisation_df.org == org].uri.to_numpy()
                    if len(index)>0:
                        for i in range(len(index)):
                            index_no = index[i]

                            affiliation_index.update({index_no:score})
            
            else:
                affiliation_index = brute_search(scholar.affiliation_raw.lower())


        else:
            organisation_string = filter_organization_name(scholar.affiliation_raw.lower())
            query = "select uri,org from organisation_with_loc where processed_org like ?;"
            organisation_df = pd.read_sql(query, connection, params=(f'%{organisation_string}%',))
            choices = organisation_df['org'].unique()
            
            matches_affiliation = process.extract(query = scholar.affiliation_raw.lower() ,
                                                  choices = choices, scorer = fuzz.WRatio,score_cutoff=90)

            if len(matches_affiliation)>1:
                for org, score, _ in matches_affiliation:
                    index = organisation_df.loc[organisation_df.org == org].uri.to_numpy()
                    if len(index)>0:
                        for i in range(len(index)):
                            index_no = index[i]

                            affiliation_index.update({index_no:score})
            
            else:
                affiliation_index = brute_search(scholar.affiliation_raw.lower())

    return affiliation_index



# scholar = Scholar(affiliation_raw="RWTH University Germany")
# print(top_10_organisation(scholar))





            

# if __name__ == "__main__":

# # Set up multiprocessing
    
#     procs = []
#     # Define a list of test scholar objects with different affiliations
#     scholar_list = [
#     Scholar(affiliation_raw="RWTH Aachen University"),
#     Scholar(affiliation_raw="Massachusetts Institute of Technology"),
#     Scholar(affiliation_raw="Stanford University"),
#     Scholar(affiliation_raw="University of Oxford"),
#     Scholar(affiliation_raw="Harvard University"),
#     Scholar(affiliation_raw="California Institute of Technology (Caltech)"),
#     Scholar(affiliation_raw="University of Cambridge"),
#     Scholar(affiliation_raw="ETH Zurich - Swiss Federal Institute of Technology"),
#     Scholar(affiliation_raw="University of California, Berkeley"),
#     Scholar(affiliation_raw="Princeton University"),
#     Scholar(affiliation_raw="University of Chicago"),
#     Scholar(affiliation_raw="University College London"),
#     Scholar(affiliation_raw="University of Tokyo"),
#     Scholar(affiliation_raw="Imperial College London"),
#     Scholar(affiliation_raw="University of California, Los Angeles (UCLA)"),
#     Scholar(affiliation_raw="Yale University"),
#     Scholar(affiliation_raw="Columbia University"),
#     Scholar(affiliation_raw="University of California, San Diego (UCSD)"),
#     Scholar(affiliation_raw="University of California, San Francisco (UCSF)"),
#     Scholar(affiliation_raw="University of Edinburgh"),
#     Scholar(affiliation_raw="University of Michigan"),
#     Scholar(affiliation_raw="University of Toronto"),
#     Scholar(affiliation_raw="Peking University"),
#     Scholar(affiliation_raw="Tsinghua University"),
#     Scholar(affiliation_raw="Seoul National University"),
#     Scholar(affiliation_raw="University of Melbourne"),
#     Scholar(affiliation_raw="University of Sydney"),
#     Scholar(affiliation_raw="University of Pennsylvania"),
#     Scholar(affiliation_raw="National University of Singapore"),
#     Scholar(affiliation_raw="University of Hong Kong"),
#     Scholar(affiliation_raw="Australian National University"),
#     Scholar(affiliation_raw="University of Copenhagen"),
#     Scholar(affiliation_raw="University of Amsterdam"),
#     Scholar(affiliation_raw="University of Geneva"),
#     Scholar(affiliation_raw="University of Zurich"),
#     Scholar(affiliation_raw="McGill University"),
#     Scholar(affiliation_raw="Delft University of Technology"),
#     Scholar(affiliation_raw="Sorbonne University"),
#     Scholar(affiliation_raw="Fudan University"),
#     Scholar(affiliation_raw="University of Washington"),
#     Scholar(affiliation_raw="University of Minnesota"),
#     Scholar(affiliation_raw="Northwestern University"),
#     Scholar(affiliation_raw="University of California, Irvine"),
#     Scholar(affiliation_raw="Rice University"),
#     Scholar(affiliation_raw="University of Southern California (USC)"),
#     Scholar(affiliation_raw="University of California, Santa Barbara"),
#     Scholar(affiliation_raw="Johns Hopkins University"),
#     Scholar(affiliation_raw="University of California, Davis"),
#     Scholar(affiliation_raw="University of Paris-Saclay"),
#     Scholar(affiliation_raw="University of Queensland"),
#     Scholar(affiliation_raw="Kyoto University"),
#     Scholar(affiliation_raw="University of California, Santa Cruz"),
#     Scholar(affiliation_raw="Vanderbilt University"),
#     Scholar(affiliation_raw="New York University (NYU)"),
#     Scholar(affiliation_raw="Ohio State University"),
#     Scholar(affiliation_raw="Georgia Institute of Technology"),
#     Scholar(affiliation_raw="King’s College London"),
#     Scholar(affiliation_raw="University of Leeds"),
#     Scholar(affiliation_raw="Pennsylvania State University"),
#     Scholar(affiliation_raw="Université de Montréal"),
#     Scholar(affiliation_raw="University of California, Riverside"),
#     Scholar(affiliation_raw="Clemson University"),
#     Scholar(affiliation_raw="Indian Institute of Technology (IIT) Bombay"),
#     Scholar(affiliation_raw="Indian Institute of Technology (IIT) Delhi"),
#     Scholar(affiliation_raw="Indian Institute of Technology (IIT) Madras"),
#     Scholar(affiliation_raw="University of Delhi"),
#     Scholar(affiliation_raw="Indian Institute of Science"),
#     Scholar(affiliation_raw="Alma Mater Studiorum - University of Bologna"),
#     Scholar(affiliation_raw="Wageningen University & Research"),
#     Scholar(affiliation_raw="Hokkaido University"),
#     Scholar(affiliation_raw="The Chinese University of Hong Kong"),
#     Scholar(affiliation_raw="University of Illinois at Urbana-Champaign"),
#     Scholar(affiliation_raw="University of Miami"),
#     Scholar(affiliation_raw="University of Pittsburgh"),
#     Scholar(affiliation_raw="University of Wisconsin-Madison"),
#     Scholar(affiliation_raw="California State University"),
#     Scholar(affiliation_raw="Lund University"),
#     Scholar(affiliation_raw="Karolinska Institute"),
#     Scholar(affiliation_raw="University of Oslo"),
#     Scholar(affiliation_raw="University of Cape Town"),
#     Scholar(affiliation_raw="University of British Columbia"),
#     Scholar(affiliation_raw="Hunan University"),
#     Scholar(affiliation_raw="Indian Institute of Technology (IIT) Kanpur"),
#     Scholar(affiliation_raw="University of Science and Technology of China"),
#     Scholar(affiliation_raw="Boston University"),
#     Scholar(affiliation_raw="University of California, Berkeley (UC Berkeley)"),
#     Scholar(affiliation_raw="Purdue University"),
#     Scholar(affiliation_raw="University of Glasgow"),
#     Scholar(affiliation_raw="University of North Carolina at Chapel Hill"),
#     Scholar(affiliation_raw="University of Bristol"),
#     Scholar(affiliation_raw="Aalto University"),
#     Scholar(affiliation_raw="University of Adelaide"),
#     Scholar(affiliation_raw="University of St Andrews"),
#     Scholar(affiliation_raw="University of Western Australia"),
#     Scholar(affiliation_raw="Shenzhen University"),
#     Scholar(affiliation_raw="Beijing Normal University"),
#     Scholar(affiliation_raw="Technische Universität München (TUM)"),
#     Scholar(affiliation_raw="Tata Institute of Fundamental Research (TIFR)"),
#     Scholar(affiliation_raw="Aarhus University"),
#     Scholar(affiliation_raw="Nanjing University"),
#     Scholar(affiliation_raw="Hong Kong University of Science and Technology"),
#     Scholar(affiliation_raw="Chalmers University of Technology"),
#     Scholar(affiliation_raw="University of Freiburg"),
#     Scholar(affiliation_raw="Pohang University of Science and Technology (POSTECH)"),
#     Scholar(affiliation_raw="Universität Mannheim"),
#     Scholar(affiliation_raw="University of Vienna"),
#     Scholar(affiliation_raw="University of Amsterdam"),
#     Scholar(affiliation_raw="Vrije Universiteit Brussel"),
#     Scholar(affiliation_raw="Fachhochschule Aachen University"),
#     Scholar(affiliation_raw="University of Milan"),
#     Scholar(affiliation_raw="University of Ljubljana"),
#     Scholar(affiliation_raw="University of Warsaw")
#     ]


#     with Pool(4) as pool:
#         # Use map to pass the scholar objects directly
#         results = pool.map(top_10_organisation, scholar_list)
    
#     # Print results
#     print(results)


#     # Read the large DataFrame in chunks
    

#     # Define a function to be applied to each chunk
#     # Apply the function to each chunk in parallel
   

#     # Concatenate the result into a single DataFrame
   
