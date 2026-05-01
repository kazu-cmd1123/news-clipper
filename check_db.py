import database
import logging
import json

logging.basicConfig(level=logging.INFO)

def check_db():
    print("--- Database Check ---")
    client = database.get_db_client()
    if not client:
        print("Failed to get DB client")
        return
        
    try:
        res = client.table("keywords").select("*").execute()
        print(f"Total keyword records: {len(res.data)}")
        for row in res.data:
            print(json.dumps(row, indent=2))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_db()
