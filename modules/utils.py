import csv
import os
from typing import List, Dict, Any

def save_to_csv(parsed_list: List[Dict[str, Any]], csv_file: str, fieldnames: List[str]):
    """Save parsed results to CSV"""
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)
    with open(csv_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(parsed_list)
    print(f"Saved {len(parsed_list)} results to {csv_file}")

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

def read_linkedin_from_csv(csv_file_path: str, title_column: str = 'linkedin_title', desc_column: str = 'linkedin_description') -> List[Dict[str, str]]:
    linkedin_data = []
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if title_column in row and desc_column in row:
                    linkedin_data.append({
                        "linkedin_title": row[title_column].strip() if row[title_column] else "",
                        "linkedin_description": row[desc_column].strip() if row[desc_column] else ""
                    })
        print(f"Read {len(linkedin_data)} LinkedIn profiles from {csv_file_path}")
        return linkedin_data
    except FileNotFoundError:
        print(f"File {csv_file_path} not found")
        return []
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []
