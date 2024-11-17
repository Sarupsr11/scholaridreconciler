import logging

from SPARQLWrapper import JSON, SPARQLWrapper

from scholaridreconciler.models.scholar import Scholar

WIKIDATA_SPARQL_ENDPOINT = "https://qlever.cs.uni-freiburg.de/api/wikidata"

DBLP_SPARQL_ENDPOINT = "https://sparql.dblp.org/sparql"


def wiki_search_by_ids(scholar:Scholar):

    dblp = scholar.dblp_id
    orcid = scholar.orcid_id
    googleScholar = scholar.googleScholar_id
    mathGenealogy = scholar.mathGenealogy_id


    query = f"""
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX wd: <http://www.wikidata.org/entity/>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT ?person
                WHERE
                {{ 
                     VALUES (?dblp ?googlescholar ?orcid ?mathGenealogy) 
                      {{ ("{dblp}" "{googleScholar}" "{orcid}" "{mathGenealogy}")}}
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



def dblp_search_by_ids(scholar:Scholar):
    
    dblp = scholar.dblp_id
    orcid = scholar.orcid_id
    googleScholar = scholar.googleScholar_id
    mathGenealogy = scholar.mathGenealogy_id

    
    query = f"""
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX dblps: <https://dblp.org/rdf/schema-2020-07-01#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX dblp: <https://dblp.org/rdf/schema#>
                PREFIX datacite: <http://purl.org/spar/datacite/>
                PREFIX litre: <http://purl.org/spar/literal/>
                SELECT ?person ?name ?aff ?dblp ?googleScholar ?orcid ?mathGenealogy 
                WHERE
                {{ 
                     VALUES (?dblp ?googleScholar ?orcid ?mathGenealogy) 
                      {{ ("{dblp}" "{googleScholar}" "{orcid}" "{mathGenealogy}")}}
                    ?person rdf:type dblp:Person.
                    ?person dblp:creatorname ?name.
                    ?person dblp:affiliation ?aff.
                    {{
                    ?person datacite:hasIdentifier ?dblp_blank .
                    ?dblp_blank datacite:usesIdentifierScheme datacite:dblp ;
                                litre:hasLiteralValue ?dblp .
                    }}
                    union
					{{
                    ?person datacite:hasIdentifier ?orcid_blank .
                    ?orcid_blank datacite:usesIdentifierScheme datacite:orcid ;
                                litre:hasLiteralValue ?orcid.
                    }}
					union
                    {{ 
                    ?person datacite:hasIdentifier ?googleScholar_blank .
                    ?googleScholar_blank datacite:usesIdentifierScheme datacite:google-scholar ;
                                    litre:hasLiteralValue ?googleScholar .
                    }}
					union
					{{
                    ?person datacite:hasIdentifier ?mathGenealogy_blank .
                    ?mathGenealogy_blank datacite:usesIdentifierScheme datacite:math-genealogy ;
                                litre:hasLiteralValue ?mathGenealogy.
                    }}
                }}
                GROUP BY ?person

            """
    sparql = SPARQLWrapper(DBLP_SPARQL_ENDPOINT)
    sparql.setReturnFormat(JSON)
    sparql.setQuery(query)

    try:
        response = sparql.queryAndConvert()
            
        return response.get('results', {}).get('bindings', [])
    except Exception as e:
        logging.error(f"SPARQL query error: {e}")
        return []



def direct_name_affiliation_search(scholar:Scholar):

    Label = scholar.name
    affiliation = scholar.affiliation
    affiliation_raw = scholar.affiliation_raw
    


    query = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX wd: <http://www.wikidata.org/entity/>
            PREFIX wdt: <http://www.wikidata.org/prop/direct/>

            SELECT DISTINCT ?item ?itemLabel WHERE 
            {{
            ?item wdt:P31 wd:Q5;  # instance of human
              rdfs:label "{Label}"@en.
            
    """

    if affiliation:
        query += f"""
              OPTIONAL {{
                  ?item wdt:P108 ?affiliation.
                  ?affiliation rdfs:label "{affiliation_raw}"@en.
              }}
        """

    query += """
    }
    LIMIT 5
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






scholar = Scholar(name="Stefan Decker", first_name="Josef", family_name="Decker",
                  affiliation_raw="RWTH Aachen University",
                  dblp_id="d/StefanDecker",orcid_id="0000-0001-6324-7164",
                  googleScholar_id="uhVkSswAAAAJ",mathGenealogy_id="284760")

print(direct_name_affiliation_search(scholar))

