
import logging
from typing import Any

from rapidfuzz import process

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class ConfidenceScoring:

    def __init__(self,scholar,scholar_list):
        self._scholar = scholar
        self._scholar_list = scholar_list
        self._attributes = ["name", "first_name", "family_name", "affiliation_raw",
                  "dblp_id", "orcid_id", "google_scholar_id", "acm",
                  "gnd", "mathGenealogy", "github", "twitter"]
        self._matching_dict_wiki : dict[str,Any] = {}
        self._total_scholar_name = 0
        self._total_scholar_fn = 0
        self._total_scholar_ln = 0
        self._total_scholar_affiliation_raw = 0
        self._total_scholar_dblp = 0
        self._total_scholar_orcid = 0
        self._total_scholar_google = 0
        self._total_scholar_mathGenealogy = 0
        self._total_scholar_acm = 0
        self._total_scholar_gnd = 0
        self._total_scholar_github = 0
        self._total_scholar_twitter = 0
        self._total_count = 0.0
        self._count_list = {}
        self._scholar_with_most_features = {}
        self._max_scholar = None
        self.inference()
        self.get_matching_attributes()
        self.update_total_count_and_inference()
        self.intialize_count_list()
        self.update_count_list()
        self.keep_track_with_same_confidence_and_tie_breaker()
        self.final_inference_update()

        

    
    def inference(self):
        self._matching_dict_wiki  = {
            'total_scholar_retrieved_in_the_process': len(self._scholar_list),
            'number_of_scholars_with_equal_confidence': 0.0,
            'matching_name_score': 0.0,
            'matching_first_name_score': 0.0,
            'matching_family_name_score': 0.0,
            'matching_affiliation_raw_score': 0.0,
            'matching_dblp_id': 'not_provided',
            'matching_orcid_id': 'not_provided',
            'matching_google_scholar_id': 'not_provided',
            'matching_acm': 'not_provided',
            'matching_mathGenealogy': 'not_provided',
            'matching_gnd': 'not_provided',
            'matching_github': 'not_provided',
            'matching_twitter': 'not_provided',
            'Important': 'If there are more than one scholars with equal confidence, we choose the one with highest'
                         ' number of the existing identifiers (considered here)  in Wikidata.',
            'All Wikidata QID with same confidence': list([])
        }
        
    def length_of_scholar_list(self):
        logging.info(f"Total Scholar in the list: {len(self._scholar_list)}")
        
    
    def get_matching_attributes(self):
        """ Returns a dictionary of non-empty matching attributes for a given scholar. """
        
        def get_exact_name_matches(attr_name):
            """Returns a list of scholars where the attribute exactly matches."""
            return [
                getattr(s, attr_name) for s in self._scholar_list 
                if getattr(s, attr_name) and getattr(s, attr_name) == getattr(self._scholar, attr_name)
            ]

        def get_id_matches(attr_name):
            """Returns a list of scholars where at least one ID matches (comma-separated)."""

            scholar_ids = set(getattr(self._scholar, attr_name, "").split(","))
            return [
                getattr(s, attr_name) for s in self._scholar_list 
                if getattr(s, attr_name) and scholar_ids.intersection(set(getattr(s, attr_name, "").split(",")))
            ]

        if self._scholar.name:
            self._total_scholar_name = len(get_exact_name_matches("name"))
        if self._scholar.first_name:
            self._total_scholar_fn =  len(get_exact_name_matches("first_name"))
        if self._scholar.family_name:
            self._total_scholar_ln = len(get_exact_name_matches("family_name"))
        if self._scholar.affiliation_raw:
            self._total_scholar_affiliation_raw = len(get_exact_name_matches("affiliation_raw"))
        if self._scholar.dblp_id:
            self._total_scholar_dblp = len(get_id_matches("dblp_id"))
        if self._scholar.orcid_id:
            self._total_scholar_orcid = len(get_id_matches("orcid_id"))
        if self._scholar.google_scholar_id:
            self._total_scholar_google = len(get_id_matches("google_scholar_id"))
        if self._scholar.mathGenealogy:
            self._total_scholar_mathGenealogy = len(get_id_matches("mathGenealogy"))
        if self._scholar.acm:
            self._total_scholar_acm = len(get_id_matches("acm"))
        if self._scholar.gnd:
            self._total_scholar_gnd = len(get_id_matches("gnd"))
        if self._scholar.github:
            self._total_scholar_github = len(get_id_matches("github"))
        if self._scholar.twitter:
            self._total_scholar_twitter = len(get_id_matches("twitter"))
        
    def update_total_count_and_inference(self):

        def update_total_count(attr_name):
            if getattr(self._scholar, attr_name) is not None and getattr(self._scholar, attr_name) != '':
                self._total_count += 1

        for attr in self._attributes:
            update_total_count(attr)
        

    def intialize_count_list(self):
        for j in range(len(self._scholar_list)):
            self._count_list.update({j:0.0})

    def update_count_list(self):
        """Updates the count list with the confidence score for each scholar in the list."""

        def update_count_list_with_ids(index,attr_name, total_scholar_attr):
            """Updates the count list with the confidence score for each scholar in the list based on IDs."""

            
            if getattr(self._scholar, attr_name) is not None and getattr(self._scholar, attr_name) != '':
                for attr in getattr(self._scholar, attr_name).split(","):
                    if attr in getattr(self._scholar_list[i], attr_name).split(","):
                        self._count_list[index] += 1/total_scholar_attr
                self._count_list[index] = self._count_list[i]/len(getattr(self._scholar_list[i], attr_name).split(","))

        def update_count_list_with_names(index,attr_name, total_scholar_attr):
            """Updates the count list with the confidence score for each scholar in the list based on names."""

            
            if getattr(self._scholar, attr_name) is not None and getattr(self._scholar, attr_name) != '':

                if total_scholar_attr:
                    self._count_list[index] += ((1/total_scholar_attr)*
                                        (process.extract(getattr(self._scholar, attr_name),
                                        [getattr(self._scholar_list[i], attr_name)]
                                        )[0][1]/100)*(1-abs(len(getattr(self._scholar, attr_name))
                                        -len(getattr(self._scholar_list[i], attr_name))
                                        )/(len(getattr(self._scholar, attr_name))
                                        +len(getattr(self._scholar_list[i], attr_name)))))
                else:
                    self._count_list[index] += (process.extract(getattr(self._scholar, attr_name),
                                    [getattr(self._scholar_list[i], attr_name)]
                                    )[0][1] / 100) * (1 - abs(len(getattr(self._scholar, attr_name))
                                    - len(getattr(self._scholar_list[i], attr_name))
                                    ) / (len(getattr(self._scholar, attr_name))
                                    + len(getattr(self._scholar_list[i], attr_name))))
        
        for i in range(len(self._scholar_list)):

            update_count_list_with_ids(i,"dblp_id", self._total_scholar_dblp)
            update_count_list_with_ids(i,"orcid_id", self._total_scholar_orcid)
            update_count_list_with_ids(i,"google_scholar_id", self._total_scholar_google)
            update_count_list_with_ids(i,"acm", self._total_scholar_acm)
            update_count_list_with_ids(i,"gnd", self._total_scholar_gnd)
            update_count_list_with_ids(i,"mathGenealogy", self._total_scholar_mathGenealogy)
            update_count_list_with_ids(i,"github", self._total_scholar_github)
            update_count_list_with_ids(i,"twitter", self._total_scholar_twitter)
            update_count_list_with_names(i,"name", self._total_scholar_name)
            update_count_list_with_names(i,"first_name", self._total_scholar_fn)
            update_count_list_with_names(i,"family_name", self._total_scholar_ln)
            update_count_list_with_names(i,"affiliation_raw", self._total_scholar_affiliation_raw)

    def keep_track_with_same_confidence_and_tie_breaker(self):


        def tie_breaker_track(index,attr_name):
            """Tie breaker tracker for scholars with equal confidence."""

            if getattr(self._scholar_list[index], attr_name) != '' and getattr(self._scholar_list[index], attr_name):
                return 1
            else:
                return 0

        for j in range(len(self._scholar_list)):
            
            name_attr = ["name", "first_name", "family_name", "affiliation_raw"]
            total_relevant_feature = sum([tie_breaker_track(j,attr) for attr in self._attributes
                                           if attr not in name_attr])
            self._scholar_with_most_features.update({j: total_relevant_feature})


        max_scholar_index = 0
        count_max_index = []

        for j in self._count_list:

            if self._count_list[j] >= self._count_list[max_scholar_index]:
                max_scholar_index = j

        for j in self._count_list:

            if self._count_list[j] == self._count_list[max_scholar_index]:
                count_max_index.append(j)
                try:
                    self._matching_dict_wiki['number_of_scholars_with_equal_confidence'] += 1.0
                    self._matching_dict_wiki['All Wikidata QID with same confidence'].append(self._scholar_list[j].wikidata_id)
                except TypeError as e:
                    print("TypeError occurred:", e)
                    print("Current state of matching_dict_wiki:", self._matching_dict_wiki)

            wiki_index = []
            max_scholar_index = count_max_index[0]
            no_of_features = self._scholar_with_most_features[count_max_index[0]]
            for k in count_max_index:
                if self._scholar_with_most_features[k] >= no_of_features:
                    self._max_scholar_index = k
                    no_of_features = self._scholar_with_most_features[k]
                    wiki_index.append(k)

            self._max_scholar = self._scholar_list[max_scholar_index]
            self._max_scholar.confidence_score = round(self._count_list[max_scholar_index] / self._total_count, 3)

    def final_inference_update(self):

        def update_each_id_key(attr_name):

            if (getattr(self._scholar, attr_name) is not None and getattr(self._max_scholar, attr_name) is not None and
                getattr(self._scholar, attr_name) != '' and getattr(self._max_scholar, attr_name) != ''):
                for attr in getattr(self._scholar, attr_name).split(","):
                    if attr in getattr(self._max_scholar, attr_name).split(","):
                        self._matching_dict_wiki['matching_' + attr_name] = 'Yes'

        def update_each_name_key(attr_name):

            if (getattr(self._scholar, attr_name) is not None and getattr(self._max_scholar, attr_name) is not None and
                getattr(self._scholar, attr_name) != '' and getattr(self._max_scholar, attr_name) != ''):
                self._matching_dict_wiki['matching_' + attr_name + '_score'] = (process.extract(getattr(self._scholar, attr_name),
                                          [getattr(self._max_scholar, attr_name)]
                                          )[0][1] / 100) * (1 - abs(len(getattr(self._scholar, attr_name))
                                        - len(getattr(self._max_scholar, attr_name)
                                        ) )/ (len(getattr(self._scholar, attr_name))
                                        + len(getattr(self._max_scholar, attr_name))))

        update_each_id_key("dblp_id")
        update_each_id_key("orcid_id")
        update_each_id_key("google_scholar_id")
        update_each_id_key("acm")
        update_each_id_key("gnd")
        update_each_id_key("mathGenealogy")
        update_each_id_key("twitter")
        update_each_id_key("github")
        update_each_name_key("name")
        update_each_name_key("first_name")
        update_each_name_key("family_name")
        update_each_name_key("affiliation_raw")

    def get_final_result(self):
        return self._max_scholar , self._matching_dict_wiki








