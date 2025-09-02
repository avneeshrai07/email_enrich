import requests
import csv
import json

def fetch_all_data_to_csv(output_file="output.csv", page_size=100, max_pages=50):
    url = "https://orbit-data.miatibro.art/fetch_data_v2"
    page = 1
    all_data = []

    while page <= max_pages:
        payload = {"page": page, "page_size": page_size}
        response = requests.post(url, json=payload)

        print(f"➡️ Requesting page {page} ... Status: {response.status_code}")

        if response.status_code != 200:
            print(f"❌ Error: {response.status_code} - {response.text}")
            break

        try:
            data = response.json()
        except Exception as e:
            print("❌ Failed to parse JSON:", e)
            print("Response text:", response.text[:500])  
            break

        print(f"Raw response sample: {json.dumps(data, indent=2)[:500]}")

        if isinstance(data, dict) and "data" in data:
            records = data["data"]
        elif isinstance(data, list):
            records = data
        else:
            print("⚠️ Unknown data format. Stopping.")
            break

        if not records:
            print("✅ No more data found. Stopping early.")
            break

        print(f"✅ Page {page} -> {len(records)} records")
        all_data.extend(records)

        page += 1

    if not all_data:
        print("⚠️ No data collected, CSV not saved.")
        return

    # Save to CSV
    keys = all_data[0].keys()
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(all_data)

    print(f"💾 Saved {len(all_data)} records to {output_file}")


if __name__ == "__main__":
    fetch_all_data_to_csv("fetched_data.csv", page_size=100, max_pages=50)
