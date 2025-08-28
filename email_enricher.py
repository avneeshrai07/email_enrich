import boto3
import json
import os
import re
from dotenv import load_dotenv

load_dotenv()

bedrock = boto3.client(
    service_name="bedrock-runtime",
    region_name=os.getenv("AWS_DEFAULT_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

def clean_json_response(text: str) -> str:
    text = re.sub(r"```json|```", "", text).strip()
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return match.group(0)
    return text  

def extract_from_email(email: str):
    prompt = f"""
    You are given an email address: {email}
    Extract the following details:
    - First Name
    - Last Name
    - Company Name (from the email domain)

    Return strictly in JSON with keys: first_name, last_name, company.
    No markdown, no explanations, only JSON.
    """

    response = bedrock.invoke_model(
        modelId="amazon.nova-pro-v1:0",  
        body=json.dumps({
            "messages": [
                {"role": "user", "content": [{"text": prompt}]}
            ],
            "inferenceConfig": {
                "temperature": 0.0,
                "maxTokens": 200
            }
        }),
        accept="application/json",
        contentType="application/json"
    )

    response_body = json.loads(response["body"].read())
    raw_text = response_body["output"]["message"]["content"][0]["text"]

    try:
        cleaned = clean_json_response(raw_text)
        return json.loads(cleaned)
    except Exception as e:
        return {"error": str(e), "raw_response": raw_text}

if __name__ == "__main__":
    with open("emails.txt", "r") as f:
        emails = [line.strip() for line in f if line.strip()]

    all_results = []
    for email in emails:
        result = extract_from_email(email)
        print(email, "➡️", result)
        if "error" not in result:
            all_results.append(result)

    with open("enriched_emails.json", "w") as f:
        json.dump(all_results, f, indent=2)

    print("\n✅ Enriched data saved to enriched_emails.json")
