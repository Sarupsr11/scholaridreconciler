import logging
from functools import wraps

import requests

logging.basicConfig(level =logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def api_checker(func):
        """Decorator to check if an API is responding to a simple SPARQL query."""
        @wraps(func)
        def wrapper(self):
            api_url = func(self) 
            test_query = "ASK { ?s ?p ?o }"  # A simple SPARQL query to check if the endpoint responds
            try:
                response = requests.get(api_url, params={"query": test_query, "format": "json"})
                if response.status_code == 200:
                    logging.info(f"✅ API {api_url} is working!")
                    return api_url  # API is working
                elif response.status_code == 404:
                    logging.error(f"❌ API {api_url} returned 404 Not Found.")
                    return None
                else:
                    logging.error(f"⚠️ API {api_url} returned status code: {response.status_code}")
                    return None
            except requests.exceptions.RequestException as e:
                logging.error(f"❌ API {api_url} request failed: {e}")
                return None  # API is not working
        return wrapper

class Endpoint:
    """
    Endpoint class to check if Wikidata and DBLP SPARQL APIs are working.
    """
    _api_wiki = "https://qlever.cs.uni-freiburg.de/api/wikidata"
    _api_wiki_rwth = "https://qlever-api.wikidata.dbis.rwth-aachen.de/"
    _api_dblp = "https://sparql.dblp.org/sparql"

    
    

    @api_checker
    def wiki_api_in_use(self):
        logging.info("Checking which Wikidata API is in use using freiburg.")
        return self._api_wiki

    @api_checker
    def wiki_api_rwth_in_use(self):
        logging.info("Checking which Wikidata API is in use using rwth.")
        return self._api_wiki_rwth

    @api_checker
    def dblp_api_in_use(self):
        logging.info("Checking if DBLP API is in use.")
        return self._api_dblp

    def preference_wikidata(self):
        """Check which Wikidata API is preferred."""
        if self.wiki_api_rwth_in_use():
            return self._api_wiki_rwth
        else:
            return self._api_wiki


      
end = Endpoint()
print(end.preference_wikidata())
