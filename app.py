from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict, Any
from email_enchricher_final import parse_emails_with_ai
from job_enricher import parse_linkedin_with_ai

app = FastAPI()


class EmailsRequest(BaseModel):
    emails: List[str]

class JobRequest(BaseModel):
    linkedin_data: List[Dict[str, Any]]


@app.get("/")
async def read_root():
    return {"message": "Email Enricher API is running"}


@app.post("/enrich-emails/")
async def enrich_emails(request: EmailsRequest):
    enriched_data = await parse_emails_with_ai(request.emails)
    return {"results": enriched_data}


@app.post("/enrich-jobs/")
async def enrich_jobs(request: JobRequest):
    enriched_data = await parse_linkedin_with_ai(request.linkedin_data)
    return {"results": enriched_data}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
