import asyncio
from typing import List
from modules.llm import get_llm, LinkedInParseResult
from modules.utils import save_to_csv, read_linkedin_from_csv

llm = get_llm(LinkedInParseResult)

CSV_FILE = "data/linkedin_output.csv"

async def extract_linkedin_info(linkedin_title: str, linkedin_description: str) -> dict:
    """Extract job title and company name from LinkedIn data using structured output"""
    prompt = f"""Extract the job title and company name from the provided LinkedIn profile information.

Rules:
- Extract the current job title (position/role) of the person
- Extract the current company name where they work
- Look for patterns like "Position at Company", "Role - Company", etc.
- If multiple positions are mentioned, prioritize the current/most recent one
- Return null if information cannot be clearly determined
- Clean up the extracted information (remove extra words like "at", "with", etc.)

LinkedIn Title: {linkedin_title}
LinkedIn Description: {linkedin_description}

Please extract the job title and company name.
"""
    
    try:
        print(f"Processing LinkedIn profile...")
        
        # Use the structured output LLM directly
        response = await asyncio.to_thread(llm.invoke, prompt)
        
        # Convert Pydantic model to dict
        result_dict = {
            "linkedin_title": linkedin_title,
            "linkedin_description": linkedin_description,
            "job_title": response.job_title,
            "company_name": response.company_name
        }
        
        print(f"Parsed: Job Title: {response.job_title}, Company: {response.company_name}")
        return result_dict
        
    except Exception as e:
        print(f"Error parsing LinkedIn data: {e}")
        return {
            "linkedin_title": linkedin_title,
            "linkedin_description": linkedin_description,
            "job_title": None, 
            "company_name": None
        }

async def parse_linkedin_with_ai(linkedin_data: List[dict]):
    async def process_linkedin(data):
        await asyncio.sleep(0.5)  
        return await extract_linkedin_info(data["linkedin_title"], data["linkedin_description"])
    
    tasks = [process_linkedin(data) for data in linkedin_data]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    valid_results = []
    for i, result in enumerate(results):
        if isinstance(result, dict):
            valid_results.append(result)
        else:
            print(f"Failed to process {linkedin_data[i]}: {result}")
            valid_results.append({
                "linkedin_title": linkedin_data[i].get("linkedin_title", ""),
                "linkedin_description": linkedin_data[i].get("linkedin_description", ""), 
                "job_title": None, 
                "company_name": None
            })
    
    if valid_results:
        save_to_csv(valid_results, CSV_FILE, ["linkedin_title", "linkedin_description", "job_title", "company_name"])
    return valid_results

async def main():
    # Test LinkedIn data (including your example)
    test_linkedin_data = [
        {
            "linkedin_title": "Siddini Venkatesh Prabhu - Professor at Indian Institute of Technology, Bombay | LinkedIn",
            "linkedin_description": "Professor at Indian Institute of Technology, Bombay · Experience: Indian Institute of Technology, Bombay · Location: Mumbai · 151 connections on LinkedIn. View Siddini Venkatesh Prabhu's profile on LinkedIn, a professional community of 1 billion members."
        },
        {
            "linkedin_title": "John Smith - Software Engineer at Google | LinkedIn",
            "linkedin_description": "Software Engineer at Google · Experience: Google, Microsoft · Location: San Francisco · 500+ connections on LinkedIn."
        },
        {
            "linkedin_title": "Sarah Johnson - Marketing Manager at Apple Inc. | LinkedIn", 
            "linkedin_description": "Marketing Manager at Apple Inc. · Experience: Apple Inc., Amazon · Location: Cupertino · 300+ connections on LinkedIn."
        }
    ]
    
    print("Processing LinkedIn profiles with structured output...")
    results = await parse_linkedin_with_ai(test_linkedin_data)
    
    print(f"\nProcessed {len(results)} LinkedIn profiles. Results saved in {CSV_FILE}")
    print("\nResults:")
    for result in results:
        job_title = result['job_title'] or "N/A"
        company_name = result['company_name'] or "N/A"
        print(f"Job Title: {job_title} | Company: {company_name}")

    # Alternative: Read from CSV file
    # csv_file_path = "input_linkedin_data.csv"  # Your input CSV file
    # linkedin_data = read_linkedin_from_csv(csv_file_path)
    # if linkedin_data:
    #     results = await parse_linkedin_with_ai(linkedin_data)

if __name__ == "__main__":
    # Run the async function
    asyncio.run(main())
