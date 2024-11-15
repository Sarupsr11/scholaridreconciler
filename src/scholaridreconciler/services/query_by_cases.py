import logging

from SPARQLWrapper import JSON, SPARQLWrapper
from pydantic import ValidationError
from models.sch import Scholar

WIKIDATA_SPARQL_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"

DBLP_SPARQL_ENDPOINT = "https://sparql.dblp.org/sparql"



def search_by_ids(scholar:Scholar):
    Label = scholar.name
    first_name = scholar.first_name
    last_name = scholar.family_name
    affiliation = scholar.affiliation
    affiliation_raw = scholar.affiliation_raw
    dblp = scholar.dblp_id
    orcid = scholar.orcid_id
    googleScholar = scholar.googleScholar_id
    mathGenealogy = scholar.mathGenealogy_id

    if ((dblp is not None) or (orcid is not None) or (googleScholar is not None) or (mathGenealogy is not None)):
        query = f"""
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX wd: <http://www.wikidata.org/entity/>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT ?person
                WHERE
                {{ 
                     VALUES (?dblp ?googlescholar ?orcid ?mathGenealogy) 
                      {{ ({dblp} {googleScholar} {orcid} {mathGenealogy})}}
                    ?person wdt:P31 wd:Q5.
                    {{?person wdt:P2456 ?dblp.}}
                    union
					{{?person wdt:P549 ?mathGenealogy.}}
					union
					{{?person wdt:P1960 ?googlescholar.}}
					union
					{{?person wdt:P496 ?orcid.}}
                }}
                GROUP BY ?person

            """
        sparql = SPARQLWrapper(WIKIDATA_SPARQL_ENDPOINT)
        sparql.setReturnFormat(JSON)
        sparql.setQuery(query)

        try:
            response = sparql.queryAndConvert()
            
            return response.get('results', {}).get('bindings', [])
        except Exception as e:
            logging.error(f"SPARQL query error: {e}")
            return []

try:
    scholar = Scholar(dblp_id="DBLP12345")
    print(scholar.dblp_id)
except ValidationError as e:
    print(e.json())