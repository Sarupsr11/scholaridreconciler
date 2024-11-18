
from rapidfuzz import fuzz, process

from scholaridreconciler.models.scholar import Scholar
from scholaridreconciler.services.scholar_affiliation import convert_to_dataframe


def naive_scoring(scholar:Scholar):

    if scholar.affiliation is not None or scholar.affiliation_raw is not None:
        scholar_df = convert_to_dataframe(scholar)

        if scholar.first_name is not None:
            fname = scholar.first_name.lower()
            scholar_df['fname_score'] = scholar_df['first_name'].apply(lambda x: 
                                                               process.extract(x,[fname],scorer = fuzz.WRatio)[0][1])
        if scholar.family_name is not None:
            lname = scholar.family_name.lower()
            scholar_df['lname_score'] = scholar_df['last_name'].apply(lambda x: 
                                                                process.extract(x,[lname],scorer = fuzz.WRatio)[0][1])
            
        if scholar.name is not None:
            name = scholar.name.lower()
            scholar_df['name_score'] = scholar_df['Label'].apply(lambda x: 
                                                            process.extract(x,[name],scorer = fuzz.WRatio)[0][1])
        
        

        if (scholar.first_name is not None and scholar.family_name is not None and
            scholar.name is not None):   
            scholar_df['confidence_score'] = (scholar_df['name_score']+scholar_df['fname_score']+
                                        scholar_df['lname_score']+scholar_df['affiliation_score'])/4
        elif scholar.first_name is not None and scholar.family_name is not None:
            scholar_df['confidence_score'] = (scholar_df['fname_score']+
                                        scholar_df['lname_score']+scholar_df['affiliation_score'])/3
        elif (scholar.family_name is not None and  scholar.name is not None):
            scholar_df['confidence_score'] = (scholar_df['name_score']+
                                        scholar_df['lname_score']+scholar_df['affiliation_score'])/3
        elif (scholar.first_name is not None and scholar.name is not None):  
            scholar_df['confidence_score'] = (scholar_df['name_score']+scholar_df['fname_score']+
                                            +scholar_df['affiliation_score'])/3

        else:
            scholar_df['confidence_score'] = (scholar_df['name_score']+
                                            +scholar_df['affiliation_score'])/2

        
        
        
        
        
        return scholar_df['confidence_score'].loc[scholar_df['confidence_score'] 
                                                  == max(scholar_df['confidence_score'] )].to_list()[0]
    else:
        raise ValueError("Affiliation is not given")
    
    
    


# scholar = Scholar(name="Michael T. Schaub", 
#                   affiliation_raw="Rwth Aachen University")
# print(naive_scoring(scholar))


