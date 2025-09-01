import asyncio
import dotenv
import csv
import os
from langchain_groq import ChatGroq
from pydantic import BaseModel
from typing import Optional, List

dotenv.load_dotenv(".env")

# Initialize Groq LLM with structured output binding
class EmailParseResult(BaseModel):
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company: Optional[str] = None


# Bind the structured output to the LLM
llm = ChatGroq(
    api_key=dotenv.get_key(".env", "API_KEY"),
    model="llama-3.3-70b-versatile",
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
).with_structured_output(EmailParseResult)


CSV_FILE = "data/output.csv"
os.makedirs("data", exist_ok=True)


def save_to_csv(parsed_list: List[dict], csv_file: str = CSV_FILE):
    """Save parsed results to CSV"""
    with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["email", "first_name", "last_name", "company"])
        writer.writeheader()
        writer.writerows(parsed_list)
    print(f"Saved {len(parsed_list)} results to {csv_file}")


async def extract_email_info(email: str) -> dict:
    """Extract email information using structured output"""
    prompt = f"""Extract first name, last name, and company name from the provided email address.

Rules:
- Extract names only from the username part (before @).
- If username looks like a real name, split into first/last.
- If username looks random → first_name=null, last_name=null.
- Extract company from domain, but ignore common providers like Gmail, Outlook, Yahoo, etc. → set company=null for these.

Email: {email}
"""
    
    try:
        print(f"Processing: {email}")
        
        # Use the structured output LLM directly
        response = await asyncio.to_thread(llm.invoke, prompt)
        
        # Convert Pydantic model to dict
        result_dict = {
            "email": response.email,
            "first_name": response.first_name,
            "last_name": response.last_name,
            "company": response.company
        }
        
        print(f"Parsed: {email} → {result_dict}")
        return result_dict
        
    except Exception as e:
        print(f"Error parsing {email}: {e}")
        return {"email": email, "first_name": None, "last_name": None, "company": None}


async def parse_emails_with_ai(emails: List[str]):
    async def process_email(email):
        await asyncio.sleep(0.5)  
        return await extract_email_info(email)
    tasks = [process_email(email) for email in emails]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    valid_results = []
    for i, result in enumerate(results):
        if isinstance(result, dict):
            valid_results.append(result)
        else:
            print(f"Failed to process {emails[i]}: {result}")
            valid_results.append({
                "email": emails[i], 
                "first_name": None, 
                "last_name": None, 
                "company": None
            })
    
    if valid_results:
        save_to_csv(valid_results)
    return valid_results

def read_emails_from_csv(csv_file_path: str, email_column: str = 'email') -> List[str]:
    emails = []
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if email_column in row and row[email_column]:
                    emails.append(row[email_column].strip())
        print(f"Read {len(emails)} emails from {csv_file_path}")
        return emails
    except FileNotFoundError:
        print(f"File {csv_file_path} not found")
        return []
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []

async def main():
    # Test emails
    test_emails = [
        "aakashsehrawat@epack.in",
        "kingdarksoul091@gmail.com", 
        "raj_kumar@tcs.co.in",
        "hemant@yahoo.com",
        "john.doe@microsoft.com",
        "sarah.wilson@amazon.com",
        "random123@outlook.com",
        "xzy789@hotmail.com"
    ]
    
    print("Processing emails with structured output...")
    results = await parse_emails_with_ai(test_emails)
    
    print(f"\nProcessed {len(results)} emails. Results saved in {CSV_FILE}")
    print("\nResults:")

    for result in results:
        email = result['email'][:29] + "..." if len(result['email']) > 29 else result['email']
        first_name = result['first_name'] or "N/A"
        last_name = result['last_name'] or "N/A" 
        company = result['company'] or "N/A"

if __name__ == "__main__":
    # Run the async function
    asyncio.run(main())
