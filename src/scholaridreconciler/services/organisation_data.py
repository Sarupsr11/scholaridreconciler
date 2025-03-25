import logging
import sqlite3
import os
from typing import Any
import pandas as pd
from SPARQLWrapper import JSON, SPARQLWrapper
from scholaridreconciler.services.api_endpoint import Endpoint
from scholaridreconciler.services.organisation_preprocessing import OrganisationPreprocessing
from scholaridreconciler.services.load_sparql_query import LoadQueryIntoDict
from concurrent.futures import ThreadPoolExecutor, as_completed
"""
This script retrieves organization names and their Wikidata IDs based on specified types
and saves the data to a CSV file.
"""

logging.basicConfig(level =logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class RetrieveAffiliation:

    def __init__(self):
        self.endpoint = Endpoint()
        self.wikidata_api = self.endpoint.wiki_api_in_use()
        self.organisation_json_data = []
        self.df = None
        

    def retrieve_possible_organisation(self, limit, offset) -> list[Any]:
        query = self.fill_placeholders("query_affiliation", limit, offset)
        sparql = SPARQLWrapper(self.wikidata_api)   # SPARQLWrapper object
        sparql.setReturnFormat(JSON)                # Set return format to JSON         
        sparql.setQuery(query)                      # Set the query to be executed

        try:
            response = sparql.queryAndConvert()
            return response.get('results', {}).get('bindings', [])
        except Exception as e:
            logging.error(f"SPARQL query error: {e}")
            return []



    def fill_placeholders(self,query_type, limit, offset):
        query_file = "src/scholaridreconciler/services/sparql_queries.yaml"
        query_name = "get_affiliation_data"
        query_load = LoadQueryIntoDict()
        query = query_load.load_queries(query_file)

        if query_name not in query["queries"] or query_type not in query["queries"][query_name]:
            raise KeyError(f"Query '{query_type}' not found under '{query_name}' in YAML file.")

        query_template = query["queries"][query_name][query_type]
        return query_template.format(limit = limit,offset = offset)

    def concurrent_run(self):
        """

        set a limit to retrieving data to avoid overload and retrieve iteratively.

        """
        limit = 100000
        max_offset = 3000000  # Assume an upper limit for example purposes
        offsets = range(0,max_offset,limit)
        max_workers = max((os.cpu_count() or 1) - 1, 1)  # At least 1 worker
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            
            future_to_offset = {
                    executor.submit(self.retrieve_possible_organisation, limit, offset): offset for offset in offsets
            }
        
            for future in as_completed(future_to_offset):
                offset = future_to_offset[future]
                try:
                    result = future.result()
                    if result:
                        self.organisation_json_data.extend(result)
                        logging.info(f"Processed offset {offset}, total records: {len(self.organisation_json_data)}")
                except Exception as e:
                    logging.error(f"Error processing offset {offset}: {e}")

    def convert_to_dataframe(self):

        """
        convert the retrieve json data into a dataframe
        
        """
        n = len(self.organisation_json_data)
        org_dict = {
        'uri': [self.organisation_json_data[i].get('nameuri', {}).get('value', '')  for i in range(n)],
        'org': [self.organisation_json_data[i].get('nameVar', {}).get('value', '')  for i in range(n)],
        'countryuri': [self.organisation_json_data[i].get('countryuri', {}).get('value', '')  for i in range(n)],
        }
        self.df = pd.DataFrame(org_dict)


    def additional_preprocessing(self):

        """
        preprocessing for shortening and fastening the fuzzy matching search

        """

        df = OrganisationPreprocessing(self.df)
        self.df = df.extending_dataframe()

    def creating_database(self):

        """
        creating a database from the organisation database indexed on country's QID.
        
        """

        db_path = os.getenv("DATABASE_PATH",
                        os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache"
                                     , "db", "organisation_data.db"))

        # Ensure the directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Debugging path
        print(f"Connecting to database at: {db_path}")

        connection = sqlite3.connect(db_path)
        self.df.to_sql("organisation_with_loc", connection, if_exists='replace', index=False)
        connection.execute("CREATE INDEX  IF NOT EXISTS country on organisation_with_loc (countryuri) ")


    def execute_whole_process(self):

        """
        
        Execute the whole pipeline
        
        """

        self.concurrent_run()
        self.convert_to_dataframe()
        self.additional_preprocessing()
        self.creating_database()




