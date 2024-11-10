
from scholar_affiliation import convert_to_dataframe
from scholaridreconciler.models.scholar import Scholar
from rapidfuzz import fuzz, process


def scoring(scholar:Scholar):
    fname = scholar.first_name.lower()
    lname = scholar.family_name.lower()
    name = scholar.name.lower()
    # affiliation = scholar.affiliation_raw.lower()

    scholar_df = convert_to_dataframe(scholar)
    scholar_df['name_score'] = scholar_df['Label'].apply(lambda x: process.extract(x,[name],scorer = fuzz.WRatio)[0][1])
    scholar_df['fname_score'] = scholar_df['first_name'].apply(lambda x: process.extract(x,[fname],scorer = fuzz.WRatio)[0][1])
    scholar_df['lname_score'] = scholar_df['last_name'].apply(lambda x: process.extract(x,[lname],scorer = fuzz.WRatio)[0][1])
    scholar_df['confidence_score'] = (scholar_df['name_score']+scholar_df['fname_score']+scholar_df['lname_score']+scholar_df['affiliation_score'])/4
    
    return scholar_df.loc[scholar_df.confidence_score == max(scholar_df.confidence_score.to_numpy())]
    


# scholar = Scholar(first_name="Josef",family_name="Decker", name="S. Decker", affiliation_raw="RWTH Aachen University")
# scoring(scholar)


