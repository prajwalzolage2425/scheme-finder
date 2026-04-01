"""
tools/seed_db.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
One-time script to load schemes_data.json into MongoDB.
Run this ONCE after setting up your database.

Usage:
    python tools/seed_db.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json, os
from pymongo import MongoClient, ASCENDING
from dotenv import load_dotenv

load_dotenv()

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_PATH   = os.path.join(BASE_DIR, "database", "schemes_data.json")
MONGO_URI   = os.getenv("MONGO_URI", "mongodb://localhost:27017/schemefinder")

def seed():
    client = MongoClient(MONGO_URI)
    db     = client.get_default_database()
    col    = db["schemes"]

    # Load JSON
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        schemes = json.load(f)

    inserted = 0
    skipped  = 0

    for scheme in schemes:
        # Use scheme id as unique key — don't insert duplicates
        existing = col.find_one({"id": scheme["id"]})
        if not existing:
            col.insert_one(scheme)
            inserted += 1
        else:
            skipped += 1

    # Create index on 'id' for fast lookups
    col.create_index([("id", ASCENDING)], unique=True)
    col.create_index([("state", ASCENDING)])
    col.create_index([("category", ASCENDING)])

    print(f"\n✅ Seeding complete!")
    print(f"   Inserted : {inserted}")
    print(f"   Skipped  : {skipped} (already exist)")
    print(f"   Total    : {col.count_documents({})}")
    client.close()

if __name__ == "__main__":
    seed()
