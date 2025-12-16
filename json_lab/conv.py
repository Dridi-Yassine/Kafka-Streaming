import csv
import json
from datetime import datetime


INPUT_CSV = "transactions_dirty.csv"
OUTPUT_JSON = "transactions_dirty.json"


def csv_row_to_json(row: dict) -> dict:
 
    return {
        "transaction_id": int(row["transaction_id"]),
        "user_id": int(row["user_id"]),
        "amount": float(row["amount"]),
        "timestamp": datetime.strptime(
            row["timestamp"], "%Y-%m-%d %H:%M:%S"
        ).isoformat(),
        "event_type": "transaction",
        "schema_version": 2
    }


def convert_csv_to_json():
    events = []

    with open(INPUT_CSV, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            try:
                event = csv_row_to_json(row)
                events.append(event)
            except Exception as e:
                print(f"Skipped malformed row: {row} ({e})")

    with open(OUTPUT_JSON, "w", encoding="utf-8") as jsonfile:
        json.dump(events, jsonfile, indent=2, ensure_ascii=False)

    print(f"Conversion completed: {OUTPUT_JSON} ({len(events)} events)")


if __name__ == "__main__":
    convert_csv_to_json()
