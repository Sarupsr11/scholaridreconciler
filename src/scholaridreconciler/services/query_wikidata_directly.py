import logging
from functools import wraps
from typing import Any

from rapidfuzz import process
from SPARQLWrapper import JSON, SPARQLWrapper

from scholaridreconciler.models.scholar import Scholar
from scholaridreconciler.services.affiliation_segregation import AffiliationSegregate
from scholaridreconciler.services.api_endpoint import Endpoint
from scholaridreconciler.services.load_sparql_query import LoadQueryIntoDict


def execute_sparql_query(func):
        """
        Decorator to execute a SPARQL query and handle errors.
        """
        @wraps(func)
        def wrapper(self):
            query = func(self)  # Get the query from the wrapped function
            sparql = SPARQLWrapper(self._wikidata_api)   # SPARQLWrapper object
            sparql.setReturnFormat(JSON)                # Set return format to JSON         
            sparql.setQuery(query)                      # Set the query to be executed
            
            try:
                response = sparql.queryAndConvert()
                return response.get('results', {}).get('bindings', [])
            except Exception as e:
                logging.error(f"SPARQL query error: {e}")
                return []

        return wrapper


def fill_placeholders(query_type: str):
    """
    Decorator to load and format SPARQL queries with scholar data.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self):
            query_file = "src/scholaridreconciler/services/sparql_queries.yaml"
            query_name = "get_scholar_wiki_id"
            query_load = LoadQueryIntoDict()
            query = query_load.load_queries(query_file)

            if query_name not in query["queries"] or query_type not in query["queries"][query_name]:
                raise KeyError(f"Query '{query_type}' not found under '{query_name}' in YAML file.")

            query_template = query["queries"][query_name][query_type]
            return query_template.format(scholar=self._scholar, affiliation_union = self._union_blocks)
        return wrapper
    return decorator

class WikidataSearch:
    """
    Class to search for a scholar in Wikidata using their IDs.
    """
    def __init__(self, scholar: Scholar):
        self._scholar = scholar
        self._endpoint = Endpoint()
        self._wikidata_api = self._endpoint.preference_wikidata()
        self._scholar_list: list[Any] = []
        self._union_blocks = None

    def generate_affiliation_unions(self):
        aff_seg = AffiliationSegregate(self._scholar)
        affiliation_parts = aff_seg.collect_each_part()
        self._union_blocks = "\n".join([
            f'''{{
                ?aff rdfs:label "{part}"@en.
            }} UNION {{
                ?aff skos:altLabel "{part}"@en.
            }} UNION''' for part in affiliation_parts
        ])
        self._union_blocks += """{{ OPTIONAL{{?item wdt:P31 "{}".}} }}"""

    
            

    @fill_placeholders(query_type="query_wiki_by_id")
    def fill_placeholders_ids(self):
        """
        Fill placeholders in the SPARQL query with scholar's IDs.
        """
        pass
    
    @fill_placeholders(query_type="query_wiki_by_name")
    def fill_placeholders_names(self):
        """
        Fill placeholders in the SPARQL query with scholar name and affiliation.
        """
        pass

    @execute_sparql_query
    def wiki_search_by_ids(self):
        """
        Search for a scholar in Wikidata using their IDs.
        """
        

        logging.info("Searching using IDs.")
        return self.fill_placeholders_ids()

    @execute_sparql_query
    def wiki_search_by_names(self):
        """
        Search for a scholar in Wikidata using their IDs.
        """
        

        logging.info("Searching by names")
        return self.fill_placeholders_names()
        
    def wiki_preference(self):
        """
        Identifiers given preference over ambiguous attributes.
        """
        if not self._scholar.identiers_present_bool():
            return self.wiki_search_by_names()
        else:
            if self.wiki_search_by_ids():
                return self.wiki_search_by_ids()
            else:
                return self.wiki_search_by_names()
            
    def json_to_scholar_list(self):

        results = self.wiki_preference()
        if len(results) > 0:
            for result in results:
                fields = ["name", "fname", "lname", "affiliation"]
                extracted_values = {
                    field: result.get(field, {}).get("value", "").split("|") for field in fields
                }

                matching_values = {
                    "name": extracted_values["name"][0],
                    "first_name": extracted_values["fname"][0],
                    "family_name": extracted_values["lname"][0],
                    "affiliation": extracted_values["affiliation"][0],
                }

                # Apply fuzzy matching only if the scholar attribute exists
                for key, scholar_attr in [
                    ("name", self._scholar.name),
                    ("fname", self._scholar.first_name),
                    ("lname", self._scholar.family_name),
                    ("affiliation", self._scholar.affiliation_raw),
                ]:
                    if scholar_attr:
                        matching_values[key] = process.extract(scholar_attr, extracted_values[key], limit=1)[0][0]
                
                
                # Create a Scholar object with extracted values and identifiers
                self._scholar_list.append(Scholar(
                    name=matching_values["name"],
                    first_name=matching_values["first_name"],   
                    family_name=matching_values["family_name"],
                    affiliation_raw=matching_values["affiliation"],
                    wikidata_id=result.get("wiki_id", {}).get("value"),
                    dblp_id=result.get("dblp_id", {}).get("value"),
                    orcid_id=result.get("orcid_id", {}).get("value"),
                    google_scholar_id=result.get("google_scholar_id", {}).get("value"),
                    acm=result.get("acm_id", {}).get("value"),
                    gnd=result.get("gnd_id", {}).get("value"),
                    mathGenealogy=result.get("mathGenealogy_id", {}).get("value"),
                    github=result.get("github_id", {}).get("value"),
                    twitter=result.get("twitter_id", {}).get("value"),
                    confidence_score=1.0  # High confidence since we have a match
                ))

    def get_scholar_list(self):
        self.generate_affiliation_unions()
        self.json_to_scholar_list()
        return self._scholar_list

        



scholar = Scholar(affiliation_raw="RWTH Aachen University, Department of Computer Science",name = "Stefan Decker")
wikidata = WikidataSearch(scholar)
print(wikidata.get_scholar_list())




