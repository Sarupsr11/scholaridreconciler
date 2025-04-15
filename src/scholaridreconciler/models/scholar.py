

from pydantic import BaseModel, model_validator


class Affiliation(BaseModel):
    """
    an affiliation
    """
    name: str | None = None
    location: str | None = None
    country: str | None = None


class Scholar(BaseModel):
    """
    a scholar
    
    """
    first_name: str | None = None
    family_name: str | None = None
    name: str | None = None
    affiliation_raw: str | None = None
    affiliation: Affiliation | None = None
    dblp_id: str | None = None
    orcid_id: str | None = None
    google_scholar_id: str | None = None
    wikidata_id:str | None = None
    confidence_score: float | None = None
    acm: str | None = None
    twitter: str | None = None
    github: str | None = None
    gnd: str | None = None
    mathGenealogy: str | None = None
    class Config:
        extra = "allow"

    
    
    
    def identiers_present_bool(self):
        return any([self.dblp_id, self.orcid_id, self.google_scholar_id, 
                    self.wikidata_id, self.acm, self.twitter, self.github, self.gnd, self.mathGenealogy])
    
    def ambiguous_properties(self):
        return any([self.first_name, self.family_name, self.name,self.affiliation_raw,self.affiliation])

    @model_validator(mode='after')
    def affiliation_selection(self):
        if self.affiliation_raw:
            self.affiliation_raw = self.affiliation_raw
            return self
        elif self.affiliation:
            self.affiliation_raw = self.affiliation.name+', '+ self.affiliation.location + ', ' + self.affiliation.country
            return self
        else:
            self.affiliation_raw = None
            return self
        
