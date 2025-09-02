import asyncio
from typing import List
from modules.llm import get_llm, EmailParseResult
from modules.utils import save_to_csv, read_emails_from_csv

llm = get_llm(EmailParseResult)

CSV_FILE = "data/output.csv"


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
        save_to_csv(valid_results, CSV_FILE, ["email", "first_name", "last_name", "company"])
    return valid_results

'''
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
    asyncio.run(main())'''