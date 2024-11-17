import logging
import sqlite3
from typing import Any

import pandas as pd
from SPARQLWrapper import JSON, SPARQLWrapper

from scholaridreconciler.services.organsiation_preprocessing import preprocess_organisation

"""
This script retrieves organization names and their Wikidata IDs based on specified types
and saves the data to a CSV file.
"""

WIKIDATA_SPARQL_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"

logging.basicConfig(level =logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def retrieve_possible_organisation(endpoint: str, limit: int, offset: int) -> list[Any]:
    
    query = f"""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    SELECT DISTINCT ?nameuri (LCASE(?nameLabel) AS ?nameVar) ?countryuri WHERE {{
      VALUES ?affiliation_type {{ 
        wd:Q3918 wd:Q38723 wd:Q31855 wd:Q16917 wd:Q327333 wd:Q484652 
        wd:Q4830453 wd:Q163740 wd:Q3914 wd:Q7075 wd:Q380962 wd:Q32053225 wd:Q112872396 wd:Q875538
        wd:Q2085381  wd:Q2385804  wd:Q294163 wd:Q245065 wd:Q157031 
        wd:Q1194093 wd:Q891723 wd:Q4287745 wd:Q748019 wd:Q155271 wd:Q1391145 wd:Q955824 
        wd:Q197952 wd:Q161726 wd:Q1785733 wd:Q708676 wd:Q336473
      }}
      ?name wdt:P31/wdt:P279* ?affiliation_type .
      ?name rdfs:label ?nameLabel .
      OPTIONAL{{
      ?name wdt:P17 ?country .
      }}
      FILTER (LANG(?nameLabel) = "en") .
      BIND (STRAFTER(STR(?name), "http://www.wikidata.org/entity/") AS ?nameuri).
      BIND (STRAFTER(STR(?country), "http://www.wikidata.org/entity/") AS ?countryuri).
    }}
    LIMIT {limit}
    OFFSET {offset}
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    sparql.setQuery(query)

    try:
        response = sparql.queryAndConvert()
        
        return response.get('results', {}).get('bindings', [])
    except Exception as e:
        logging.error(f"SPARQL query error: {e}")
        return []

def concurrent_run(endpoint: str) -> list[Any]:
    limit = 100000
    offset = 0
    organisation_json_data = []
    while True:
        result = retrieve_possible_organisation(endpoint, limit, offset)
        if not result:
            break
        organisation_json_data.extend(result)
        logging.info(f"Dataset size: {len(organisation_json_data)} records")
        offset += limit
    return organisation_json_data

def convert_to_dataframe_with_country(organisation: list[Any]) -> pd.DataFrame:
    n = len(organisation)
    organisation_dict = {
        'uri': [organisation[i].get('nameuri', {}).get('value', '')  for i in range(n)],
        'org': [organisation[i].get('nameVar', {}).get('value', '')  for i in range(n)],
        'countryuri': [organisation[i].get('countryuri', {}).get('value', '')  for i in range(n)],
    }
    return pd.DataFrame(organisation_dict)

# Execute and save organsiation data with country to CSV
organisation_json = concurrent_run(WIKIDATA_SPARQL_ENDPOINT)


# Execute preprocessed organisation data without country
preproccesed_dataframe, stop_words = preprocess_organisation(convert_to_dataframe_with_country(organisation_json))




def create_db(organisation:pd.DataFrame):
    connection = sqlite3.connect("src/scholaridreconciler/services/organisation_data.db")
    organisation.to_sql("organisation_with_loc",connection,if_exists='replace',index=False)
    connection.execute("CREATE INDEX  IF NOT EXISTS country on organisation_with_loc (countryuri) ")
    connection.execute("CREATE INDEX IF NOT EXISTS proc_org on organisation_with_loc (processed_org) ")




create_db(preproccesed_dataframe)
