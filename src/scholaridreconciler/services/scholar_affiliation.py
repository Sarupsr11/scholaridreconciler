import logging
from SPARQLWrapper import SPARQLWrapper, JSON # type:ignore
import pandas as pd
from scholaridreconciler.models.scholar import Scholar
from organisation_check import top_10_organisation

WIKIDATA_SPARQL_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata" 


# scholar = Scholar(name="Stefan Decker", affiliation_raw="RWTH Aachen University")


def scholar_retrieve(scholar:Scholar):
    score_dict = top_10_organisation(scholar)
    aff = list(score_dict.keys())
    
    aff_values = " ".join(f"wd:{id}" for id in aff)
    query = f"""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX wikibase: <http://wikiba.se/ontology#>
    SELECT distinct ?nameuri 
            (LCASE(?nameLabel) AS ?nameVar) 
            (LCASE(?firstname) AS ?fname) 
            (LCASE(?lastname) AS ?lname)  
            (LCASE(?affname) AS ?org)
            ?affuri
    WHERE {{
    VALUES ?aff {{ {aff_values}   }}
    ?name wdt:P31 wd:Q5.
    {{?name wdt:P108 ?aff.}}
    UNION {{
        ?name wdt:P69 ?aff.
    }}
    UNION {{
        ?name wdt:P463 ?aff.
    }}
    UNION {{
        ?name wdt:P1416 ?aff.
    }}
    UNION {{
        ?name wdt:P512 ?aff.
    }}
    UNION {{
        ?name wdt:P39 ?aff.
    }}
    ?name rdfs:label ?nameLabel.
    ?name wdt:P735 ?fn.
    ?name wdt:P734 ?ln.
    ?fn rdfs:label ?firstname.
    ?ln rdfs:label ?lastname.
    ?aff rdfs:label ?affname.
    FILTER(LANG(?affname)="en").
    FILTER(LANG(?nameLabel)="en").
    FILTER(LANG(?firstname)="en").
    FILTER(LANG(?lastname)="en").
    BIND(STRAFTER(STR(?name), "http://www.wikidata.org/entity/") AS ?nameuri).
    BIND(STRAFTER(STR(?aff), "http://www.wikidata.org/entity/") AS ?affuri).
    }}

    """
    sparql = SPARQLWrapper(WIKIDATA_SPARQL_ENDPOINT)
    sparql.setReturnFormat(JSON)
    sparql.setQuery(query)
    try:
            # Run the SPARQL query in a separate thread to avoid blocking
        response = sparql.queryAndConvert()
        return response.get('results', {}).get('bindings', [])
    except Exception as e:
        logging.info(f"JSON decode error: {e}")

def convert_to_dataframe(scholar:Scholar) :
    # Prepare lists for each column, with error handling for missing fields


    score_dict = top_10_organisation(scholar)
    scholar_list = scholar_retrieve(scholar)
    n = len(scholar_list)
    scholar_dict = {
        'nameuri': [scholar_list[i].get('nameuri', {}).get('value', '') for i in range(n)],
        'Label': [scholar_list[i].get('nameVar', {}).get('value', '') for i in range(n)],
        'first_name': [scholar_list[i].get('fname', {}).get('value', '') for i in range(n)],
        'last_name': [scholar_list[i].get('lname', {}).get('value', '') for i in range(n)],
        'affiliation': [scholar_list[i].get('org', {}).get('value', '') for i in range(n)],
        'affiliation_score': [
            score_dict.get(scholar_list[i].get('affuri', {}).get('value'), 0) for i in range(n)
        ]
    }
    return pd.DataFrame(scholar_dict)

# print(convert_to_dataframe(scholar_retrieve(scholar,score_dict),score_dict).info())