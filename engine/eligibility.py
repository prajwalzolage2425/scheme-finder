"""
engine/eligibility.py
Reads schemes from MongoDB instead of JSON file.
"""

import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/schemefinder")

def get_schemes_collection():
    client = MongoClient(MONGO_URI)
    db     = client.get_default_database()
    return db["schemes"], client


def load_schemes():
    """Load all schemes from MongoDB."""
    col, client = get_schemes_collection()
    schemes = list(col.find({}, {"_id": 0}))
    client.close()
    return schemes


def get_scheme_by_id(scheme_id):
    """Fetch a single scheme by its id field."""
    col, client = get_schemes_collection()
    scheme = col.find_one({"id": scheme_id}, {"_id": 0})
    client.close()
    return scheme


def calculate_match_score(scheme, user_data):
    score     = 0
    max_score = 0
    reasons   = []
    blockers  = []

    rules = scheme.get("eligibility", {})

    if "max_income" in rules:
        max_score += 30
        user_income = int(user_data.get("annual_income", 0))
        if user_income <= rules["max_income"]:
            score += 30
            reasons.append(f"Income ₹{user_income:,} is within limit of ₹{rules['max_income']:,}")
        else:
            blockers.append(f"Income ₹{user_income:,} exceeds limit of ₹{rules['max_income']:,}")

    if "min_age" in rules or "max_age" in rules:
        max_score += 20
        user_age = int(user_data.get("age", 0))
        if user_age >= rules.get("min_age", 0) and user_age <= rules.get("max_age", 200):
            score += 20
            reasons.append(f"Age {user_age} is within eligible range")
        else:
            blockers.append(f"Age {user_age} not in required range ({rules.get('min_age','0')}–{rules.get('max_age','any')})")

    if "gender" in rules:
        max_score += 20
        if user_data.get("gender", "").lower() == rules["gender"].lower():
            score += 20
            reasons.append("Gender eligibility matched")
        else:
            blockers.append(f"This scheme is only for {rules['gender']} applicants")

    if "caste" in rules:
        max_score += 20
        user_caste = user_data.get("caste", "general").lower()
        if user_caste in [c.lower() for c in rules["caste"]]:
            score += 20
            reasons.append(f"Caste category {user_caste.upper()} is eligible")
        else:
            blockers.append(f"Caste must be one of: {', '.join(rules['caste']).upper()}")

    if "area_type" in rules:
        max_score += 10
        user_area = user_data.get("area_type", "urban").lower()
        if user_area in [a.lower() for a in rules["area_type"]]:
            score += 10
            reasons.append(f"{user_area.capitalize()} area is covered")
        else:
            blockers.append(f"This scheme is for {'/'.join(rules['area_type'])} areas only")

    if "occupation" in rules:
        max_score += 15
        user_occ = user_data.get("occupation", "").lower().replace(" ", "_")
        if user_occ in [o.lower() for o in rules["occupation"]]:
            score += 15
            reasons.append(f"Occupation qualifies")
        else:
            blockers.append(f"Occupation must be: {', '.join(rules['occupation'])}")

    if scheme.get("state") not in ["central", None]:
        max_score += 15
        user_state   = user_data.get("state", "").lower().replace(" ", "_")
        scheme_state = scheme["state"].lower()
        if user_state == scheme_state or user_state in scheme_state:
            score += 15
            reasons.append("State scheme available in your state")
        else:
            blockers.append(f"This is a {scheme['state'].capitalize()} state scheme only")

    if rules.get("bpl"):
        max_score += 10
        if user_data.get("bpl_card", "no").lower() == "yes":
            score += 10
            reasons.append("BPL card holder — eligible")
        else:
            blockers.append("BPL card required for this scheme")

    if rules.get("bank_account"):
        max_score += 5
        if user_data.get("has_bank_account", "yes").lower() == "yes":
            score += 5

    if max_score == 0:
        return 50, reasons, blockers

    final_score = int((score / max_score) * 100)
    if blockers and final_score > 40:
        final_score = 25

    return final_score, reasons, blockers


def find_eligible_schemes(user_data):
    schemes = load_schemes()
    results = []

    for scheme in schemes:
        score, reasons, blockers = calculate_match_score(scheme, user_data)
        results.append({**scheme, "match_score": score, "reasons": reasons, "blockers": blockers})

    results.sort(key=lambda x: x["match_score"], reverse=True)
    eligible = [r for r in results if r["match_score"] >= 50]
    explore  = [r for r in results if r["match_score"] <  50]
    return eligible, explore
