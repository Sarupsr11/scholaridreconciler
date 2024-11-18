from scholaridreconciler.services.scholar_scoring import naive_scoring
from scholaridreconciler.services.query_by_cases import dblp_search_by_ids
from scholaridreconciler.services.query_by_cases import wiki_search_by_ids, direct_name_affiliation_search
from scholaridreconciler.models.scholar import Scholar

def search_query(scholar:Scholar):
    if ((scholar.dblp_id is not None) or (scholar.orcid_id is not None) or
        (scholar.googleScholar_id is not None) or (scholar.mathGenealogy_id is not None)):
        answer = wiki_search_by_ids(scholar)
        if answer is not None:
            wikidata_id = answer[0].get('uri', {}).get('value', '')
            return wikidata_id
        else:
            answer = dblp_search_by_ids(scholar)
            new_scholar = Scholar()  # use the result returned by dblp to again search in wikidata
            answer = wiki_search_by_ids(new_scholar)
            if answer is not None:
                return answer
            else:
                answer = direct_name_affiliation_search(new_scholar)
                if answer is not None:
                    return answer
                else:
                    answer = naive_scoring(new_scholar)
                    if answer is not None:
                        return answer
                    else:
                        raise ValueError("Wikidata id is not present")
    else:
        answer = direct_name_affiliation_search(scholar)
        if answer is not None:
            return answer
        else:
            answer = naive_scoring(scholar)
            if answer is not None:
                return answer
            else:
                raise ValueError("Wikidata id is not present")



# scholar = Scholar(name="Stefan Decker", first_name="Josef", family_name="Decker",
#                   affiliation_raw="RWTH Aachen University",
#                   dblp_id="d/StefanDecker",orcid_id="0000-0001-6324-7164",
#                   googleScholar_id="uhVkSswAAAAJ",mathGenealogy_id="284760")

# print(search_query(scholar))