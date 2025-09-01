# email_enricher.py
import asyncio
import json



async def ask_llama(prompt: str) -> str:
    proc = await asyncio.create_subprocess_exec(
        "ollama", "run", "llama3",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await proc.communicate(input=prompt.encode())
    return stdout.decode()


def parse_llama_output(raw_output: str):
    try:
        start = raw_output.find("[")
        end = raw_output.rfind("]") + 1
        json_str = raw_output[start:end]
        data = json.loads(json_str)
        cleaned = []
        for item in data:
            cleaned.append({
                "email": item.get("email"),
                "first_name": item.get("first_name"),
                "last_name": item.get("last_name"),
                "company": item.get("company"),
            })
        return cleaned
    except Exception:
        return []


async def parse_single_email(email):
    prompt = f"""
Extract first name, last name, and company name from the provided email address.
- Extract names only from the username part (before @).
- If username looks like a real name, split into first/last.
- If username looks random → first_name=null, last_name=null.
- Extract company from domain, but ignore common providers like Gmail, Outlook, Yahoo, etc.
Return the result strictly as a JSON list of objects with keys: email, first_name, last_name, company.

Email:
["{email}"]
"""
    # print(prompt,"Prompt")
    raw_output = await ask_llama(prompt)
    return parse_llama_output(raw_output)


async def parse_emails_sequentially(emails):
    # Process emails one by one
    for email in emails:
        result = await parse_single_email(email)
        for entry in result:
            print(f"Email: {entry['email']}")
            print(f"First Name: {entry['first_name']}")
            print(f"Last Name: {entry['last_name']}")
            print(f"Company: {entry['company']}")
            print("-" * 40)


if __name__ == "__main__":
    emails = [
        "aakashsehrawat@epack.in",
        "john.doe@gmail.com",
        "jane.smith@techcorp.com",
        "hemant@gmail.com",
        "a.elshimy@eleganciagroup.com",
        "anand@aamorinox.com",
        "aneeraj@bechtel.com",
        "ashishchoudhary@anmolstainless.com",
        "ashu.lalit@havells.com"
    ]
    asyncio.run(parse_emails_sequentially(emails))
