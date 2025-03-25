import logging
import yaml
from SPARQLWrapper import JSON, SPARQLWrapper
from scholaridreconciler.models.scholar import Scholar
from scholaridreconciler.services.api_endpoint import Endpoint
from functools import wraps
from scholaridreconciler.services.load_sparql_query import LoadQueryIntoDict
from scholaridreconciler.services.affiliation_segregation import AffiliationSegregate



class DblpSearch:
    """
    Class to search for a scholar in Wikidata using their IDs.
    """
    def __init__(self, scholar: Scholar):
        self.scholar = scholar
        self.endpoint = Endpoint()
        self.dblp_api = self.endpoint.dblp_api_in_use()
        self.scholar_list = []
        self.union_blocks = None

    def generate_affiliation_unions(self):
        aff_seg = AffiliationSegregate(self.scholar)
        affiliation_parts = aff_seg.collect_each_part()
        self.union_blocks = "\n".join([
            f'''{{
                ?aff dblp:primaryAffiliation ?affname.
            }} 
            FILTER(CONTAINS(LCASE(?affname), LCASE("{part}")))
            ''' for part in affiliation_parts
        ])
        


    def execute_sparql_query(func):
        """
        Decorator to execute a SPARQL query and handle errors.
        """
        @wraps(func)
        def wrapper(self):
            query = func(self)  # Get the query from the wrapped function
            sparql = SPARQLWrapper(self.dblp_api)
            sparql.setReturnFormat(JSON)
            sparql.setQuery(query)
            
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
                query_name = "get_scholar_dblp_id"
                query_load = LoadQueryIntoDict()
                query = query_load.load_queries(query_file)

                if query_name not in query["queries"] or query_type not in query["queries"][query_name]:
                    raise KeyError(f"Query '{query_type}' not found under '{query_name}' in YAML file.")

                query_template = query["queries"][query_name][query_type]
                return query_template.format(scholar=self.scholar, affiliation_union = self.union_blocks)
            
            return wrapper
        
        return decorator

    @fill_placeholders(query_type="query_dblp_by_id")
    def fill_placeholders_ids(self) -> str:
        """
        Fill placeholders in the SPARQL query with scholar's IDs.
        """
        pass

    @fill_placeholders(query_type="query_dblp_by_name")
    def fill_placeholders_names(self) -> str:
        """
        Fill placeholders in the SPARQL query with scholar's name and affiliation.
        """
        pass

    @execute_sparql_query
    def dblp_search_by_ids(self):
        """
        Search for a scholar in Wikidata using their IDs.
        """
        logging.info("Searching using IDs.")
        return self.fill_placeholders_ids()

    @execute_sparql_query
    def dblp_search_by_names(self):
        """
        Search for a scholar in Wikidata using their Names.
        """
        logging.info("Searching by names.")
        return self.fill_placeholders_names()

    def dblp_preference(self):
        """
        Identifiers given preference over ambiguous attributes.
        """
        if not self.scholar.identiers_present_bool():
            return self.dblp_search_by_names()
        else:
            return self.dblp_search_by_ids() or self.dblp_search_by_names()
        
    def json_to_scholar_list(self):
        
        """
        Convert JSON response to a list of Scholar objects.
        """
        results_dblp = self.dblp_preference()
        if results_dblp:
            for i in range(len(results_dblp)):
                results = {key: val["value"] for key, val in results_dblp[i].items()}
                name = results.get("nameVar_1","")
                affiliation_raw = results.get("affiliationVar_1","")
                acm_id = results.get("acmVar_1")
                gnd_id = results.get("gndVar_1")
                math_genealogy_id = results.get("mathGenealogyVar_1")
                dblp_id = results.get("dblpVar_1")
                orcid_id = results.get("orcidVar_1")
                google_scholar_id = results.get("googleScholarVar_1")
                github = results.get("githubVar_1")
                twitter = results.get("twitterVar_1")
                self.scholar_list.append(Scholar(
                    name=name,
                    affiliation_raw=affiliation_raw,
                    orcid_id=orcid_id,
                    dblp_id=dblp_id,
                    google_scholar_id=google_scholar_id,
                    mathGenealogy=math_genealogy_id,
                    acm=acm_id,
                    gnd=gnd_id,
                    github=github,
                    twitter=twitter
                ))

    def get_scholar_list_dblp(self):
        self.generate_affiliation_unions()
        self.json_to_scholar_list()
        return self.scholar_list



# scholar = Scholar(name="Stefan Decker", affiliation_raw="RWTH, Aachen, Department of Computer Science, Germany")
# dblp = DblpSearch()
# print(dblp.get_scholar_list_dblp(scholar))


