from pydantic import BaseModel, PrivateAttr, ConfigDict
from typing import Optional

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
    
    first_name: Optional[str]  = None 
    family_name: Optional[str]  = None
    name: Optional[str]  = None
    affiliation_raw: Optional[str]  = None
    dblp_id: Optional[str]  = None
    googleScholar_id: Optional[str]  = None
    orcid_id: Optional[str]  = None
    mathGenealogy_id: Optional[str]  = None
    affiliation: Optional[Affiliation]  = None
    class Config:
        extra = "allow"
    
