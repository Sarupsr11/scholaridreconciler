import logging
import pandas as pd
from functools import wraps
from SPARQLWrapper import JSON, SPARQLWrapper  # type:ignore
from scholaridreconciler.models.scholar import Scholar
from scholaridreconciler.services.organisation_check import OrganisationSearch
from scholaridreconciler.services.api_endpoint import Endpoint
from scholaridreconciler.services.load_sparql_query import LoadQueryIntoDict 
from scholaridreconciler.services.affiliation_segregation import AffiliationSegregate


# scholar = Scholar(name="Stefan Decker", affiliation_raw="RWTH Aachen University")

import logging
import pandas as pd
from functools import wraps
from SPARQLWrapper import SPARQLWrapper, JSON

class ScholarRetrieve:

    def __init__(self, scholar: Scholar):
        self.retrieved_org = None
        self.scholar = scholar
        self.endpoint = Endpoint()
        self.wikidata_api = self.endpoint.preference_wikidata()
        self.json_data = []
        self.df = None

    def generate_affiliation_unions(self):
        aff_seg = AffiliationSegregate(self.scholar)
        affiliation_parts = aff_seg.collect_each_part()
        self.union_blocks = "\n".join([
            f'''{{
                ?aff rdfs:label "{part}"@en.
            }} UNION {{
                ?aff skos:altLabel "{part}"@en.
            }} UNION''' for part in affiliation_parts
        ])
        self.union_blocks += """{{ OPTIONAL{{?item wdt:P31 "{}".}} }}"""
    
    def collect_organisation(self):
        """Collect top organizations associated with the scholar."""
        org = OrganisationSearch(self.scholar)
        org.selection_preference()
        self.retrieved_org = " ".join(f"wd:{id}" for id in org.affiliation_index.keys())

    def execute_sparql_query(func):
        """
        Decorator to execute a SPARQL query and handle errors.
        """
        @wraps(func)
        def wrapper(self):
            query = func(self)  # Get the query from the wrapped function
            if not query:
                logging.error("SPARQL query is empty. Skipping execution.")
                return None

            sparql = SPARQLWrapper(self.wikidata_api)
            sparql.setReturnFormat(JSON)
            sparql.setQuery(query)

            try:
                response = sparql.queryAndConvert()
                results = response.get("results", {}).get("bindings", [])
                return results  # Return results instead of modifying json_data directly
            except Exception as e:
                logging.error(f"SPARQL query error: {e}")
                return None  # Return None on failure

        return wrapper

    def fill_placeholders(query_type: str):
        """
        Decorator to load and format SPARQL queries with scholar data.
        """
        def decorator(func):
            @wraps(func)
            def wrapper(self, input):
                query_file = "src/scholaridreconciler/services/sparql_queries.yaml"
                query_name = "collect_scholar_data"
                query_load = LoadQueryIntoDict()
                query = query_load.load_queries(query_file)
                

                if query_name not in query["queries"] or query_type not in query["queries"][query_name]:
                    raise KeyError(f"Query '{query_type}' not found under '{query_name}' in YAML file.")

                query_template = query["queries"][query_name][query_type]
                return query_template.format(aff_values= input)  # Execute function with formatted query
            
            return wrapper
       
        return decorator

    @fill_placeholders(query_type="query_employee")
    def fill_placeholders_aff(self, formmated_org: str) -> str:
        """Return the formatted SPARQL query instead of just passing."""
        pass
    
    @fill_placeholders(query_type="query_employee_with_exact")
    def fill_placeholders_aff_with_exact(self, formmated_org: str) -> str:
        """Return the formatted SPARQL query instead of just passing."""
        pass

    @execute_sparql_query
    def extract_with_aff_approx(self):
        self.collect_organisation()
        return self.fill_placeholders_aff(self.retrieved_org)  # Return the query

    @execute_sparql_query
    def extract_with_aff_exact(self):
        self.generate_affiliation_unions()
        return self.fill_placeholders_aff_with_exact(self.union_blocks)  # Return the query
    

    def collect_scholar_data(self):
        """
        Collect scholar data from Wikidata.
        """
        
        self.json_data.extend(self.extract_with_aff_exact())
        self.json_data.extend(self.extract_with_aff_approx())

    def convert_to_dataframe(self) -> pd.DataFrame:
        """
        Convert retrieved JSON data into a Pandas DataFrame.
        """
        n = len(self.json_data)
        scholar_dict = {
            'nameuri': [self.json_data[i].get('nameuri', {}).get('value', '') for i in range(n)],
            'Label': [self.json_data[i].get('nameVar', {}).get('value', '') for i in range(n)],
            'first_name': [self.json_data[i].get('fname', {}).get('value', '') for i in range(n)],
            'last_name': [self.json_data[i].get('lname', {}).get('value', '') for i in range(n)],
            'affiliation': [self.json_data[i].get('org', {}).get('value', '') for i in range(n)],
        }
        self.df = pd.DataFrame(scholar_dict)

    def execute(self):
        self.collect_scholar_data()
        self.convert_to_dataframe()
        return self.df


# scholar = Scholar(affiliation_raw="RWTH Aache University",name = "Stefan Decker")
# wikidata = ScholarRetrieve(scholar)
# print(wikidata.execute())


