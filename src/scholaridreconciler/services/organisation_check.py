from rapidfuzz import fuzz, process
import pandas as pd
from scholaridreconciler.models.scholar import Scholar

# create a organisation dataframe
organisation_df = pd.read_csv("organisation_data.csv")


def top_10_organisation(scholar:Scholar):
    affiliation_index: dict[any,any] = {}

    matches_affiliation = process.extract(scholar.affiliation_raw.lower(), organisation_df.org.unique(), scorer=fuzz.WRatio, limit = 10 )
    for org, score, _ in matches_affiliation:
        index = organisation_df.loc[organisation_df.org == org].uri.to_numpy()[0]
        affiliation_index.update({index:score})
    return affiliation_index



# scholar = Scholar(affiliation_raw="RWTH Aachen University")
# print(top_10_organisation(scholar))