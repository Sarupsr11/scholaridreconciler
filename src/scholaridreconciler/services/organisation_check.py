from rapidfuzz import fuzz, process
import pandas as pd
from scholaridreconciler.models.scholar import Scholar

# create a organisation dataframe
import os
import pandas as pd

file_path = "organisation_data.csv"
if os.path.exists(file_path):
    organisation_df = pd.read_csv(file_path)
else:
    raise FileNotFoundError(f"{file_path} does not exist. Make sure the file is in the root directory.")



def top_10_organisation(scholar:Scholar):
    affiliation_index: dict[any,any] = {}

    matches_affiliation = process.extract(scholar.affiliation_raw.lower(), organisation_df.org.unique(), scorer=fuzz.WRatio, limit = 10 )
    for org, score, _ in matches_affiliation:
        index = organisation_df.loc[organisation_df.org == org].uri.to_numpy()
        if len(index)>0:
            for i in range(len(index)):
                index_no = index[i]

                affiliation_index.update({index_no:score})
    return affiliation_index



scholar = Scholar(affiliation_raw="RWTH Aachen University")
print(top_10_organisation(scholar))