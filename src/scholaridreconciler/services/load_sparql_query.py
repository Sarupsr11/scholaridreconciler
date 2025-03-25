import yaml
import logging

class LoadQueryIntoDict:

    def load_queries(self, query_file: str) -> str:
        """
        Load SPARQL query from a file.
        """
        try:
            with open(query_file, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            logging.error(f"Error loading query: {e}")
            return {}
        