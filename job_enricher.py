import os
import csv
import random
from datetime import datetime
from multiprocessing import Process, Manager, current_process
from modules.llm import get_llm, LinkedInParseResult

# ------------------------------
# Load API keys
# ------------------------------
API_KEYS = [os.getenv(f"API_KEY_{i}") for i in range(32)]
API_KEYS = [k for k in API_KEYS if k]

if not API_KEYS:
    raise RuntimeError("❌ Please set at least one API_KEY_X environment variable!")

# ------------------------------
# Data paths
# ------------------------------
DATA_DIR = "data"
CSV_FILE = os.path.join(DATA_DIR, "fetched_data.csv")
os.makedirs(DATA_DIR, exist_ok=True)

# ------------------------------
# Initialize LLM with a key
# ------------------------------
def get_llm_with_key(api_key: str):
    return get_llm(LinkedInParseResult, api_key=api_key)

# ------------------------------
# LLM call with retry
# ------------------------------
def call_llm_with_retry(llm_instance, prompt: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            return llm_instance.invoke(prompt)
        except Exception as e:
            if "429" in str(e):
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"[{current_process().name}] ⏳ Rate limit. Retrying in {wait_time:.1f}s...")
                import time; time.sleep(wait_time)
            else:
                raise
    raise RuntimeError(f"[{current_process().name}] Max retries exceeded.")

# ------------------------------
# Extract LinkedIn info
# ------------------------------
def extract_linkedin_info(llm_instance, title: str, description: str):
    prompt = f"""Extract the job title and company name from the provided LinkedIn profile information.

Rules:
- Extract the current job title (position/role) of the person
- Extract the current company name where they work
- Prioritize the most recent position
- Return null if information cannot be determined
- Clean extra words

LinkedIn Title: {title}
LinkedIn Description: {description}
"""
    try:
        response = call_llm_with_retry(llm_instance, prompt)
        return {"job_title": response.job_title, "company_name": response.company_name}
    except Exception as e:
        print(f"[{current_process().name}] ❌ Error: {e}")
        return {"job_title": "", "company_name": ""}

# ------------------------------
# Worker function
# ------------------------------
def worker(batch, api_key, seen_signatures, return_list):
    llm_instance = get_llm_with_key(api_key)
    results = []

    for i, row in enumerate(batch, start=1):
        title = row.get("linkedin_title", "").strip()
        description = row.get("linkedin_description", "").strip()
        signature = f"{title}|{description}".lower()

        # Global duplicate check
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)

        result = extract_linkedin_info(llm_instance, title, description)
        row.update(result)
        results.append(row)
        print(f"[{current_process().name}] ✅ [{i}/{len(batch)}] Parsed: {result['job_title']} at {result['company_name']}")

    return_list.extend(results)

# ------------------------------
# Main
# ------------------------------
def main():
    if not os.path.exists(CSV_FILE):
        print(f"❌ File {CSV_FILE} does not exist")
        return

    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    if not rows:
        print("⚠️ CSV is empty")
        return

    # Divide rows into 32 chunks
    num_keys = len(API_KEYS)
    chunk_size = (len(rows) + num_keys - 1) // num_keys
    chunks = [rows[i*chunk_size:(i+1)*chunk_size] for i in range(num_keys)]

    manager = Manager()
    seen_signatures = manager.set()
    return_list = manager.list()
    processes = []

    # Start a process for each API key
    for i, api_key in enumerate(API_KEYS):
        p = Process(target=worker, args=(chunks[i], api_key, seen_signatures, return_list), name=f"APIWorker-{i}")
        p.start()
        processes.append(p)

    # Wait for all processes to finish
    for p in processes:
        p.join()

    # Final CSV
    for col in ["company_name", "job_title"]:
        if col not in fieldnames:
            fieldnames.append(col)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(DATA_DIR, f"enriched_parallel_{timestamp}.csv")
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(return_list)

    print(f"🎉 All data processed. CSV saved at {output_file}")

# ------------------------------
# Entry point
# ------------------------------
if __name__ == "__main__":
    main()
