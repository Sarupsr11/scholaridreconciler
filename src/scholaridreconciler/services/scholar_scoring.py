

import pandas as pd
from rapidfuzz import fuzz, process

from scholaridreconciler.models.scholar import Scholar


class FuzzyScoring:

    """
    A scoring object to calculate the matching score between the scholar and the retrieved data
    
    """

    def __init__(self, scholar: Scholar, scholar_df:pd.DataFrame, topk: int = 1):
        self._scholar = scholar
        self._scholar_df = scholar_df
        self._attributes = ["name", "first_name", "family_name", "affiliation_raw",
                  "dblp_id", "orcid_id", "google_scholar_id", "acm",
                  "gnd", "mathGenealogy", "github", "twitter"]
        self._extra_identifier_attributes = 0
        self._scholar_df['name_title'] = self._scholar_df['Label'].apply(lambda x: x.split()[-1])
        self._scholar_df['name_first'] = self._scholar_df['Label'].apply(lambda x: " ".join(x.split()[:-1])
                                    if len(x.split()) > 1 else x)

        self._scholar_df['fname_score'] = 0
        self._scholar_df['lname_score'] = 0
        self._scholar_df['name_score'] = 0
        self._scholar_df['affiliation_score'] = 0
        self._affiliation_nunique = scholar_df['affiliation'].nunique()
        self._name_nunique = scholar_df['Label'].nunique()
        self._name_first_nunique = scholar_df['name_first'].nunique()
        self._name_title_nunique = scholar_df['name_title'].nunique()
        self._first_name_nunique = scholar_df['first_name'].nunique()
        self._family_name_nunique = scholar_df['last_name'].nunique()
        self._topk = topk
        self.update_values()


    def update_values(self):

        def update_extra_identifier_attributes():

            if self._scholar.dblp_id is not None and self._scholar.dblp_id != '':

                self._extra_identifier_attributes += 1

            if self._scholar.orcid_id is not None and self._scholar.orcid_id != '':
                self._extra_identifier_attributes += 1

            if self._scholar.google_scholar_id is not None and self._scholar.google_scholar_id != '':
                self._extra_identifier_attributes += 1

            if self._scholar.acm is not None and self.scholar.acm != '':
                self._extra_identifier_attributes += 1

            if self._scholar.gnd is not None and self._scholar.gnd != '':
                self._extra_identifier_attributes += 1

            if self._scholar.mathGenealogy is not None and self._scholar.mathGenealogy != '':
                self._extra_identifier_attributes += 1

            if self._scholar.github is not None and self._scholar.github != '':
                self._extra_identifier_attributes += 1
            
            if self._scholar.twitter is not None and self._scholar.twitter != '':

                self._extra_identifier_attributes += 1

        def update_fuzzy_matching_scores():


            if self._scholar.affiliation_raw is not None:
                affiliation = self._scholar.affiliation_raw.lower()
                self._scholar_df['affiliation_score'] = self._scholar_df['affiliation'].apply(
                    lambda x: process.extract(x, [affiliation],scorer=fuzz.WRatio)[0][1] * (
                                1 - abs(len(x) - len(affiliation))
                                / (len(x) + len(affiliation))))

            if self._scholar.first_name is not None:
                fname = self._scholar.first_name.lower()
                self._scholar_df['fname_score'] = self._scholar_df['first_name'].apply(
                    lambda x: process.extract(x, [fname], scorer=fuzz.WRatio)[0][1] * (
                                  1 - abs(len(x) - len(fname)) / (len(x) + len(fname))))

            if self._scholar.family_name is not None:
                lname = self._scholar.family_name.lower()
                self._scholar_df['lname_score'] = self._scholar_df['last_name'].apply(
                    lambda x: process.extract(x, [lname], scorer=fuzz.WRatio)[0][1] * 
                                  1 - abs(len(x) - len(lname)) / (len(x) + len(lname)))

            if self._scholar.name is not None:
                name = self._scholar.name.lower()
                name_title = self._scholar.name.lower().split()[-1]
                name_first = " ".join(self._scholar.name.lower().split()[:-1])
                self._scholar_df['name_title_score'] = self._scholar_df['name_title'].apply(
                    lambda x: process.extract(x, [name_title], scorer=fuzz.WRatio)[0][1] * (
                                  1 - abs(len(x) - len(name_title)) / (len(x) + len(name_title))))
                self._scholar_df['name_score'] = self._scholar_df['Label'].apply(
                    lambda x: process.extract(x, [name], scorer=fuzz.WRatio)[0][1] * (
                                  1 - abs(len(x) - len(name)) / (len(x) + len(name))))   
                self._scholar_df['name_first_score'] = self._scholar_df['name_first'].apply(
                    lambda x: process.extract(x, [name_first], scorer=fuzz.WRatio)[0][1] * (
                                  1 - abs(len(x) - len(name_first)) / (len(x) + len(name_first))))

        update_extra_identifier_attributes()
        update_fuzzy_matching_scores()

    def calculate_confidence_score(self):
        denominator = (self._affiliation_nunique + self._name_nunique + self._name_title_nunique
                     + self._first_name_nunique + self._family_name_nunique + self._name_first_nunique)
        self._scholar_df['confidence_score'] = (self._name_nunique * self._scholar_df['name_score'] +
                        self._name_first_nunique * self._scholar_df['name_first_score'] +
                        self._name_title_nunique * self._scholar_df['name_title_score'] +
                        self._first_name_nunique * self._scholar_df['fname_score'] +
                        self._family_name_nunique * self._scholar_df['lname_score'] +
                        self._affiliation_nunique * self._scholar_df['affiliation_score']) / (100 * denominator)
        
        self._scholar_df = self._scholar_df.sort_values(by='confidence_score', ascending=False)



    def return_topk(self):
        self.calculate_confidence_score()
        return self._scholar_df.head(self._topk).to_dict(orient='records')


# scholar = Scholar(name='Biao Huang',
#                   affiliation_raw="University of Alberta, Department of Chemical and Materials Engineering, "
#                                   "Edmonton, AB, Canada")
# sch = ScholarRetrieve(scholar)
# sch_df = sch.execute()
# fuzzy = FuzzyScoring(scholar, sch_df)
# print(fuzzy.return_topk())