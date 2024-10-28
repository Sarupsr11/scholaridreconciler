import logging
from SPARQLWrapper import SPARQLWrapper, JSON # type:ignore
from typing import Any
from collections.abc import Callable


# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Remove any existing handlers
logger.handlers.clear()

# Create a file handler with overwrite mode
file_handler = logging.FileHandler("app.log",mode="w")
file_handler.setLevel(logging.DEBUG)

# Create a formattter and det it for the handler
formatter = logging.Formatter("%(asctime)s-%(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(file_handler)

def get_url()-> str:
  return "https://sparql.dblp.org/sparql"

def query_statement()-> Any:
  query= """
  PREFIX datacite: <http://purl.org/spar/datacite/>
  PREFIX dblp: <https://dblp.org/rdf/schema#>
  PREFIX litre: <http://purl.org/spar/literal/>
  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  SELECT DISTINCT 
    (GROUP_CONCAT(DISTINCT?nameVar;SEPARATOR='|') AS ?name) 
    (GROUP_CONCAT(DISTINCT?homepageVar;SEPARATOR='|') AS ?homepage) 
    (GROUP_CONCAT(DISTINCT?affiliationVar;SEPARATOR='|') AS ?affiliation) 
    (GROUP_CONCAT(DISTINCT?dblpVar;SEPARATOR='|') AS ?dblp) 
    (GROUP_CONCAT(DISTINCT?wikidataVar;SEPARATOR='|') AS ?wikidata) 
    (GROUP_CONCAT(DISTINCT?orcidVar;SEPARATOR='|') AS ?orcid) 
    (GROUP_CONCAT(DISTINCT?googleScholarVar;SEPARATOR='|') AS ?googleScholar) 
    (GROUP_CONCAT(DISTINCT?acmVar;SEPARATOR='|') AS ?acm) 
    (GROUP_CONCAT(DISTINCT?twitterVar;SEPARATOR='|') AS ?twitter) 
    (GROUP_CONCAT(DISTINCT?githubVar;SEPARATOR='|') AS ?github) 
    (GROUP_CONCAT(DISTINCT?viafVar;SEPARATOR='|') AS ?viaf) 
    (GROUP_CONCAT(DISTINCT?scigraphVar;SEPARATOR='|') AS ?scigraph) 
    (GROUP_CONCAT(DISTINCT?zbmathVar;SEPARATOR='|') AS ?zbmath) 
    (GROUP_CONCAT(DISTINCT?researchGateVar;SEPARATOR='|') AS ?researchGate) 
    (GROUP_CONCAT(DISTINCT?mathGenealogyVar;SEPARATOR='|') AS ?mathGenealogy) 
    (GROUP_CONCAT(DISTINCT?locVar;SEPARATOR='|') AS ?loc) 
    (GROUP_CONCAT(DISTINCT?linkedinVar;SEPARATOR='|') AS ?linkedin) 
    (GROUP_CONCAT(DISTINCT?lattesVar;SEPARATOR='|') AS ?lattes) 
    (GROUP_CONCAT(DISTINCT?isniVar;SEPARATOR='|') AS ?isni) 
    (GROUP_CONCAT(DISTINCT?ieeeVar;SEPARATOR='|') AS ?ieee) 
    (GROUP_CONCAT(DISTINCT?geprisVar;SEPARATOR='|') AS ?gepris) 
    (GROUP_CONCAT(DISTINCT?gndVar;SEPARATOR='|') AS ?gnd) 
  WHERE {
    ?editor rdf:type dblp:Person .
    ?editor dblp:primaryCreatorName ?nameVar .
    ?editor datacite:hasIdentifier ?wikidata_blank .
    ?wikidata_blank datacite:usesIdentifierScheme datacite:wikidata ;
                    litre:hasLiteralValue ?wikidataVar .
    OPTIONAL { ?editor dblp:primaryHomepage ?homepageVar .
   }
    OPTIONAL { ?editor dblp:primaryAffiliation ?affiliationVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?dblp_blank .
      ?dblp_blank datacite:usesIdentifierScheme datacite:dblp ;
                  litre:hasLiteralValue ?dblpVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?orcid_blank .
      ?orcid_blank datacite:usesIdentifierScheme datacite:orcid ;
                   litre:hasLiteralValue ?orcidVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?googleScholar_blank .
      ?googleScholar_blank datacite:usesIdentifierScheme datacite:google-scholar ;
                           litre:hasLiteralValue ?googleScholarVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?acm_blank .
      ?acm_blank datacite:usesIdentifierScheme datacite:acm ;
                 litre:hasLiteralValue ?acmVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?twitter_blank .
      ?twitter_blank datacite:usesIdentifierScheme datacite:twitter ;
                     litre:hasLiteralValue ?twitterVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?github_blank .
      ?github_blank datacite:usesIdentifierScheme datacite:github ;
                    litre:hasLiteralValue ?githubVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?viaf_blank .
      ?viaf_blank datacite:usesIdentifierScheme datacite:viaf ;
                  litre:hasLiteralValue ?viafVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?scigraph_blank .
      ?scigraph_blank datacite:usesIdentifierScheme datacite:scigraph ;
                      litre:hasLiteralValue ?scigraphVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?zbmath_blank .
      ?zbmath_blank datacite:usesIdentifierScheme datacite:zbmath ;
                    litre:hasLiteralValue ?zbmathVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?researchGate_blank .
      ?researchGate_blank datacite:usesIdentifierScheme datacite:research-gate ;
                          litre:hasLiteralValue ?researchGateVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?mathGenealogy_blank .
      ?mathGenealogy_blank datacite:usesIdentifierScheme datacite:math-genealogy ;
                           litre:hasLiteralValue ?mathGenealogyVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?loc_blank .
      ?loc_blank datacite:usesIdentifierScheme datacite:loc ;
                 litre:hasLiteralValue ?locVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?linkedin_blank .
      ?linkedin_blank datacite:usesIdentifierScheme datacite:linkedin ;
                      litre:hasLiteralValue ?linkedinVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?lattes_blank .
      ?lattes_blank datacite:usesIdentifierScheme datacite:lattes ;
                    litre:hasLiteralValue ?lattesVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?isni_blank .
      ?isni_blank datacite:usesIdentifierScheme datacite:isni ;
                  litre:hasLiteralValue ?isniVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?ieee_blank .
      ?ieee_blank datacite:usesIdentifierScheme datacite:ieee ;
                  litre:hasLiteralValue ?ieeeVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?gepris_blank .
      ?gepris_blank datacite:usesIdentifierScheme datacite:gepris ;
                    litre:hasLiteralValue ?geprisVar .
   }
    OPTIONAL { ?editor datacite:hasIdentifier ?gnd_blank .
      ?gnd_blank datacite:usesIdentifierScheme datacite:gnd ;
                 litre:hasLiteralValue ?gndVar .
   }
  }
  GROUP BY ?editor
  """
  return query

# define a sparql wrapper to query with an api
sparql = SPARQLWrapper(get_url())
sparql.setReturnFormat(JSON) 

def query_call()-> Any:
  try:
    ret = sparql.queryAndConvert()
    return ret
  except Exception as e:
    logger.error("Error:",e)

def convert_to_dataframe(func: Callable[...,Any]) -> Any:
  wikidata_author = func()
  n = len(wikidata_author['results']['bindings'])
  author_with_wikidata_id =
  {
    'name': [wikidata_author['results']['bindings'][i]['name']['value'] for i in range(n)],
    'homepage': [wikidata_author['results']['bindings'][i]['homepage']['value'] for i in range(n)],
    'affiliation': [wikidata_author['results']['bindings'][i]['affiliation']['value'] for i in range(n)],
    'dblp': [wikidata_author['results']['bindings'][i]['dblp']['value'] for i in range(n)],
    'wikidata': [wikidata_author['results']['bindings'][i]['wikidata']['value'] for i in range(n)],
    'orcid': [wikidata_author['results']['bindings'][i]['orcid']['value'] for i in range(n)],
    'googleScholar': [wikidata_author['results']['bindings'][i]['googleScholar']['value'] for i in range(n)],
    'acm': [wikidata_author['results']['bindings'][i]['acm']['value'] for i in range(n)],
    'github': [wikidata_author['results']['bindings'][i]['github']['value'] for i in range(n)],
    'viaf': [wikidata_author['results']['bindings'][i]['viaf']['value'] for i in range(n)],
    'scigraph': [wikidata_author['results']['bindings'][i]['scigraph']['value'] for i in range(n)],
    'zbmath': [wikidata_author['results']['bindings'][i]['zbmath']['value'] for i in range(n)],
    'researchGate': [wikidata_author['results']['bindings'][i]['researchGate']['value'] for i in range(n)]
  }

  author_with_wikidata_id_df = pd.DataFrame(author_with_wikidata_id)
  return author_with_wikidata_id_df

Dataframe_id = convert_to_dataframe(query_call())

  


    

  
  
