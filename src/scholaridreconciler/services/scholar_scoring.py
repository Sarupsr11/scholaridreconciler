
from rapidfuzz import fuzz, process
import pandas as pd
from scholaridreconciler.models.scholar import Scholar
from scholaridreconciler.services.scholar_affiliation import ScholarRetrieve
from typing import Any





class FuzzyScoring:

    """
    A scoring object to calculate the matching score between the scholar and the retrieved data
    
    """

    def __init__(self, scholar: Scholar, scholar_df:pd.DataFrame, topk: int = 1):
        self.scholar = scholar
        self.scholar_df = scholar_df
        self.attributes = ["name", "first_name", "family_name", "affiliation_raw",
                  "dblp_id", "orcid_id", "google_scholar_id", "acm",
                  "gnd", "mathGenealogy", "github", "twitter"]
        self.extra_identifier_attributes = 0
        self.scholar_df['name_title'] = self.scholar_df['Label'].apply(lambda x: x.split()[-1])
        self.scholar_df['name_first'] = self.scholar_df['Label'].apply(lambda x: " ".join(x.split()[:-1])
                                    if len(x.split()) > 1 else x)

        self.scholar_df['fname_score'] = 0
        self.scholar_df['lname_score'] = 0
        self.scholar_df['name_score'] = 0
        self.scholar_df['affiliation_score'] = 0
        self.affiliation_nunique = scholar_df['affiliation'].nunique()
        self.name_nunique = scholar_df['Label'].nunique()
        self.name_first_nunique = scholar_df['name_first'].nunique()
        self.name_title_nunique = scholar_df['name_title'].nunique()
        self.first_name_nunique = scholar_df['first_name'].nunique()
        self.family_name_nunique = scholar_df['last_name'].nunique()
        self.topk = topk
        self.update_values()


    def update_values(self):

        def update_extra_identifier_attributes():

            if self.scholar.dblp_id is not None and self.scholar.dblp_id != '':

                self.extra_identifier_attributes += 1

            if self.scholar.orcid_id is not None and self.scholar.orcid_id != '':
                self.extra_identifier_attributes += 1

            if self.scholar.google_scholar_id is not None and self.scholar.google_scholar_id != '':
                self.extra_identifier_attributes += 1

            if self.scholar.acm is not None and self.scholar.acm != '':
                self.extra_identifier_attributes += 1

            if self.scholar.gnd is not None and self.scholar.gnd != '':
                self.extra_identifier_attributes += 1

            if self.scholar.mathGenealogy is not None and self.scholar.mathGenealogy != '':
                self.extra_identifier_attributes += 1

            if self.scholar.github is not None and self.scholar.github != '':
                self.extra_identifier_attributes += 1
            
            if self.scholar.twitter is not None and self.scholar.twitter != '':

                self.extra_identifier_attributes += 1

        def update_fuzzy_matching_scores():


            if self.scholar.affiliation_raw is not None:
                affiliation = self.scholar.affiliation_raw.lower()
                self.scholar_df['affiliation_score'] = self.scholar_df['affiliation'].apply(
                    lambda x: process.extract(x, [affiliation],scorer=fuzz.WRatio)[0][1] * (
                                1 - abs(len(x) - len(affiliation))
                                / (len(x) + len(affiliation))))

            if self.scholar.first_name is not None:
                fname = self.scholar.first_name.lower()
                self.scholar_df['fname_score'] = self.scholar_df['first_name'].apply(
                    lambda x: process.extract(x, [fname], scorer=fuzz.WRatio)[0][1] * (
                                  1 - abs(len(x) - len(fname)) / (len(x) + len(fname))))

            if self.scholar.family_name is not None:
                lname = self.scholar.family_name.lower()
                self.scholar_df['lname_score'] = self.scholar_df['last_name'].apply(
                    lambda x: process.extract(x, [lname], scorer=fuzz.WRatio)[0][1] * 
                                  1 - abs(len(x) - len(lname)) / (len(x) + len(lname)))

            if self.scholar.name is not None:
                name = self.scholar.name.lower()
                name_title = self.scholar.name.lower().split()[-1]
                name_first = " ".join(self.scholar.name.lower().split()[:-1])
                self.scholar_df['name_title_score'] = self.scholar_df['name_title'].apply(
                    lambda x: process.extract(x, [name_title], scorer=fuzz.WRatio)[0][1] * (
                                  1 - abs(len(x) - len(name_title)) / (len(x) + len(name_title))))
                self.scholar_df['name_score'] = self.scholar_df['Label'].apply(
                    lambda x: process.extract(x, [name], scorer=fuzz.WRatio)[0][1] * (
                                  1 - abs(len(x) - len(name)) / (len(x) + len(name))))   
                self.scholar_df['name_first_score'] = self.scholar_df['name_first'].apply(
                    lambda x: process.extract(x, [name_first], scorer=fuzz.WRatio)[0][1] * (
                                  1 - abs(len(x) - len(name_first)) / (len(x) + len(name_first))))

        update_extra_identifier_attributes()
        update_fuzzy_matching_scores()

    def calculate_confidence_score(self):
        denominator = (self.affiliation_nunique + self.name_nunique + self.name_title_nunique
                     + self.first_name_nunique + self.family_name_nunique + self.name_first_nunique)
        self.scholar_df['confidence_score'] = (self.name_nunique * self.scholar_df['name_score'] +
                                            self.name_first_nunique * self.scholar_df['name_first_score'] +
                                            self.name_title_nunique * self.scholar_df['name_title_score'] +
                                            self.first_name_nunique * self.scholar_df['fname_score'] +
                                            self.family_name_nunique * self.scholar_df['lname_score'] +
                                            self.affiliation_nunique * self.scholar_df['affiliation_score']) / (100 * denominator)
        
        self.scholar_df = self.scholar_df.sort_values(by='confidence_score', ascending=False)



    def return_topk(self):
        self.calculate_confidence_score()
        return self.scholar_df.head(self.topk).to_dict(orient='records')


# scholar = Scholar(name='Biao Huang',
#                   affiliation_raw="University of Alberta, Department of Chemical and Materials Engineering, "
#                                   "Edmonton, AB, Canada")
# sch = ScholarRetrieve(scholar)
# sch_df = sch.execute()
# fuzzy = FuzzyScoring(scholar, sch_df)
# print(fuzzy.return_topk())