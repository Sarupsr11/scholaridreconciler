import logging
from SPARQLWrapper import SPARQLWrapper, JSON # type:ignore
from typing import Any
from collections.abc import Callable
import pandas as pd
import requests


WIKIDATA_SPARQL_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata" 

logging.basicConfig(level =logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

def retrieve_possible_organisation(endpoint:str,LIMIT:int, OFFSET:int):
    QUERY = f"""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    SELECT DISTINCT ?nameuri (LCASE(?nameLabel) AS ?nameVar) WHERE 
    {{
    {{
        ?name wdt:P31/wdt:P279* wd:Q3918 .
        ?name rdfs:label ?nameLabel .
    }}
    UNION
    {{
        ?name wdt:P31/wdt:P279* wd:Q38723 .
        ?name rdfs:label ?nameLabel .
    }}
    UNION
    {{
        ?name wdt:P31/wdt:P279* wd:Q31855 .
        ?name rdfs:label ?nameLabel .
    }}
    UNION
    {{
        ?name wdt:P31/wdt:P279* wd:Q16917 .
        ?name rdfs:label ?nameLabel .
    }}
    UNION
    {{
        ?name wdt:P31/wdt:P279* wd:Q327333 .
        ?name rdfs:label ?nameLabel .
    }}
    UNION
    {{
        ?name wdt:P31/wdt:P279* wd:Q484652 .
        ?name rdfs:label ?nameLabel .
    }}
    UNION
    {{
        ?name wdt:P31/wdt:P279* wd:Q4830453 .
        ?name rdfs:label ?nameLabel .
    }}
    UNION
    {{
        ?name wdt:P31/wdt:P279* wd:Q163740 .
        ?name rdfs:label ?nameLabel .
    }}
    UNION
    {{
        ?name wdt:P31/wdt:P279* wd:Q7075 .
        ?name rdfs:label ?nameLabel .
    }}
    UNION
    {{
        ?name wdt:P31/wdt:P279* wd:Q163740 .
        ?name rdfs:label ?nameLabel .
    }}
    UNION
    {{
        ?name wdt:P31/wdt:P279* wd:Q3914 .
        ?name rdfs:label ?nameLabel .
    }}
    UNION
    {{
        ?name wdt:P31/wdt:P279* wd:Q380962 .
        ?name rdfs:label ?nameLabel .
    }}
    UNION
    {{
        ?name wdt:P31/wdt:P279* wd:Q32053225 .
        ?name rdfs:label ?nameLabel .
    }}
    UNION
    {{
        ?name wdt:P31/wdt:P279* wd:Q112872396 .
        ?name rdfs:label ?nameLabel .
    }}
    FILTER (LANG(?nameLabel) = "en") .
    BIND(STRAFTER(STR(?name), "http://www.wikidata.org/entity/") AS ?nameuri).
    }}
    LIMIT {LIMIT}
    OFFSET {OFFSET}
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    sparql.setQuery(QUERY)
    try:
        # Run the SPARQL query in a separate thread to avoid blocking
        response = sparql.queryAndConvert()
        return response['results']['bindings']
    except Exception as e:
        logging.info(f"JSON decode error: {e}")
        return None

def concurrent_run(WIKIDATA_SPARQL_ENDPOINT:str) -> list[Any]:
    limit = 100000
    offset = 0
    organisation_json_data = []
    while True:

        result = retrieve_possible_organisation(WIKIDATA_SPARQL_ENDPOINT,limit,offset)
        if not result:
            break
        organisation_json_data.extend(result)
        logging.info(f"Currently the dataset has {len(organisation_json_data)} elements")
        offset += limit
    return organisation_json_data

def convert_to_dataframe(organisation: list[Any]) -> pd.DataFrame:
    n = len(organisation)
    organisation_dict = {
        'uri': [organisation[i]['nameuri']['value'] for i in range(n)],
        'org': [organisation[i]['nameVar']['value'] for i in range(n)],
    }   
    return pd.DataFrame(organisation_dict)

def save_csv_file(organisation_dataframe: pd.DataFrame)-> None:
    organisation_dataframe.to_csv("organisation_data.csv",index=False)



save_csv_file(convert_to_dataframe(concurrent_run(WIKIDATA_SPARQL_ENDPOINT)))
