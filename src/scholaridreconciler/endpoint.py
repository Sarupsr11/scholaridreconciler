from fastapi import FastAPI
from starlette.responses import RedirectResponse

from scholaridreconciler.models.scholar import Scholar

app = FastAPI()


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
    return {
        "message": "Not implemented yet!",
        "errors": [],
        "scholar": scholar
    }
