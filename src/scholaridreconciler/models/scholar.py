from pydantic import BaseModel


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


