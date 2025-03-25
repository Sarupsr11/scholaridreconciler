from fastapi import FastAPI
from starlette.responses import RedirectResponse
from scholaridreconciler.services.organisation_data import RetrieveAffiliation
from scholaridreconciler.services.search_scholar import SearchScholar
from scholaridreconciler.models.scholar import Scholar
import logging
import os
import sqlite3
app = FastAPI()


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
db_path = os.getenv("DATABASE_PATH",
                    os.path.join(os.path.dirname(os.path.abspath(__file__)),".cache"
                                ,"db", "organisation_data.db"))

# Ensure the directory exists
os.makedirs(os.path.dirname(db_path), exist_ok=True)
print(f"Connecting to database at: {db_path}")
connection = sqlite3.connect(db_path,check_same_thread=False)


@app.get("/", include_in_schema=False)
async def docs_redirect():
    """
    redirect the home page to docs
    """
    return RedirectResponse(url="/docs")


@app.post("/reconcile/scholar")
async def reconcile_scholar(scholar: Scholar):
    """
    reconcile given scholar
    :param scholar:
    :return:
    """
    if db_path is None:
        organisation = RetrieveAffiliation()
        organisation.execute_whole_process()
    
    search_scholar = SearchScholar(scholar)
    search_scholar.search()
    result , log = search_scholar.result, search_scholar.final_log
    if result:
        return {
            "message": "Success",
            "errors": [],
            "scholar": result,
            "log": log
        }
    else:
        return {
            "message": "Not implemented yet!",
            "errors": [],
            "scholar": scholar
        }


    