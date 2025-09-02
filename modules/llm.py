import dotenv
from langchain_groq import ChatGroq
from pydantic import BaseModel
from typing import Optional

dotenv.load_dotenv(".env")

class EmailParseResult(BaseModel):
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company: Optional[str] = None

class LinkedInParseResult(BaseModel):
    job_title: Optional[str] = None
    company_name: Optional[str] = None

def get_llm(structured_output_model, api_key: str):
    return ChatGroq(
        api_key=api_key,
        model="llama-3.3-70b-versatile",
        temperature=0,
        max_tokens=None,
        timeout=None,
        max_retries=2,
    ).with_structured_output(structured_output_model)
