import csv
import os
from typing import List, Dict, Any

def save_to_csv_row(row: Dict, file_path: str, headers: List[str]):
    """Save a single row (dict) to CSV immediately."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # If file does not exist, create with header
    file_exists = os.path.exists(file_path)

    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)

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

def read_data_from_csv(csv_file_path: str) -> List[Dict[str, str]]:
    data = []
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, start=1):
                # Collect title fields
                title_fields = [
                    row.get("linkedin_title", "").strip(),
                    row.get("linkedin_title_2", "").strip(),
                    row.get("title", "").strip()
                ]
                # Remove empty and duplicate titles
                unique_titles = list(dict.fromkeys([t for t in title_fields if t]))
                combined_title = " | ".join(unique_titles)

                # Collect description fields
                desc_fields = [
                    row.get("description1_snippets", "").strip(),
                    row.get("linkedin_description1", "").strip(),
                    row.get("linkedin_description2", "").strip()
                ]
                # Remove empty and duplicate descriptions
                unique_descs = list(dict.fromkeys([d for d in desc_fields if d]))
                combined_description = " | ".join(unique_descs)

                # Skip row if both title and description are empty
                if not combined_title and not combined_description:
                    continue

                row["linkedin_title"] = combined_title
                row["linkedin_description"] = combined_description
                data.append(row)

                if i % 100 == 0:
                    print(f"Processed {i} rows...")

        print(f"✅ Read {len(data)} rows from {csv_file_path}")
        return data

    except FileNotFoundError:
        print(f"❌ File {csv_file_path} not found")
        return []
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")
        return []

