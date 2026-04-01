"""
tools/fetch_schemes.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Scrapes schemes from myscheme.gov.in using Selenium
and saves them directly into MongoDB.

Usage:
    python tools/fetch_schemes.py               # fetch 50 schemes
    python tools/fetch_schemes.py --size 100    # fetch 100 schemes
    python tools/fetch_schemes.py --query "women"
    python tools/fetch_schemes.py --state maharashtra
    python tools/fetch_schemes.py --preview     # print without saving
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json
import time
import re
import os
import argparse
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/schemefinder")
LOG_PATH  = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database", "fetch_log.json")

CATEGORY_MAP = {
    "housing": "Housing", "health": "Health", "education": "Education",
    "scholarship": "Education", "employment": "Employment", "skill": "Employment",
    "agriculture": "Agriculture", "farming": "Agriculture", "banking": "Banking",
    "finance": "Banking", "insurance": "Insurance", "pension": "Social Security",
    "social": "Social Security", "women": "Women & Child", "child": "Women & Child",
    "girl": "Women & Child", "energy": "Energy", "business": "Livelihood",
    "entrepreneur": "Livelihood", "msme": "Livelihood", "loan": "Livelihood",
    "tribal": "Social Security", "disability": "Social Security", "minority": "Social Security",
}

OCCUPATION_KEYWORDS = {
    "farmer":         ["farmer", "kisan", "agriculture", "cultivator"],
    "daily_wage":     ["labour", "worker", "mgnrega", "unorganised"],
    "self_employed":  ["self employed", "self-employed", "entrepreneur"],
    "business_owner": ["business", "msme", "enterprise", "startup"],
    "artisan":        ["artisan", "craftsman", "weaver", "handicraft"],
    "student":        ["student", "scholar"],
    "homemaker":      ["homemaker", "housewife"],
    "unemployed":     ["unemployed", "job seeker", "youth"],
}


def get_collection():
    client = MongoClient(MONGO_URI)
    db = client.get_default_database()
    col = db["schemes"]
    col.create_index([("id", ASCENDING)], unique=True)
    return col, client


# ── Scrape ────────────────────────────────────────────────────────────────────
def scrape_with_selenium(query="", state_filter="", max_schemes=50):
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError:
        print("❌ Missing packages. Run: pip install selenium webdriver-manager")
        return []

    print("  🌐 Launching Chrome (headless)...")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    schemes = []

    try:
        url = "https://www.myscheme.gov.in/search"
        if query:
            url += f"?q={query}"
        print(f"  → Opening: {url}")
        driver.get(url)
        time.sleep(5)

        # Scroll to load more results
        prev_count = 0
        for scroll in range(max(5, max_schemes // 8)):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2.5)

            # Try clicking Load More button
            try:
                btn = driver.find_element(By.XPATH,
                    "//button[contains(translate(text(),'abcdefghijklmnopqrstuvwxyz',"
                    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'LOAD MORE') or "
                    "contains(translate(text(),'abcdefghijklmnopqrstuvwxyz',"
                    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'VIEW MORE') or "
                    "contains(translate(text(),'abcdefghijklmnopqrstuvwxyz',"
                    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'SHOW MORE')]"
                )
                driver.execute_script("arguments[0].click();", btn)
                print("  → Clicked Load More")
                time.sleep(3)
            except:
                pass

            # Count current cards
            cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/schemes/']")
            count = len(set(c.get_attribute("href") for c in cards if c.get_attribute("href")))
            print(f"  → Scroll {scroll+1}: {count} scheme links found")
            if count >= max_schemes or count == prev_count:
                break
            prev_count = count

        # First try: intercept API calls from network logs
        print("  → Trying to intercept API calls from network logs...")
        schemes = intercept_api_from_logs(driver)
        if schemes:
            print(f"  ✓ Intercepted {len(schemes)} schemes from API calls")
            return schemes[:max_schemes]

        # Second try: parse scheme links and visit each detail page
        print("  → Parsing scheme links from page...")
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/schemes/']")
        hrefs = list(dict.fromkeys(
            l.get_attribute("href") for l in links
            if l.get_attribute("href") and "/schemes/" in l.get_attribute("href")
        ))
        print(f"  → Found {len(hrefs)} unique scheme links")

        for href in hrefs[:max_schemes]:
            try:
                scheme = scrape_scheme_detail(driver, href)
                if scheme:
                    schemes.append(scheme)
                    print(f"  ✓ [{len(schemes)}] {scheme['name'][:60]}")
                time.sleep(1)
            except Exception as e:
                continue

    except Exception as e:
        print(f"  ❌ Error: {e}")
    finally:
        driver.quit()

    return schemes[:max_schemes]


def scrape_scheme_detail(driver, url):
    """Visit a scheme detail page and extract its data."""
    driver.get(url)
    time.sleep(2)

    try:
        from selenium.webdriver.common.by import By

        # Get scheme name from title or h1
        name = ""
        for sel in ["h1", "h2", "[class*='scheme-name']", "[class*='title']"]:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                name = el.text.strip()
                if name: break
            except: pass

        if not name:
            name = driver.title.replace("| MyScheme", "").replace("MyScheme -", "").strip()

        if not name or len(name) < 5:
            return None

        # Get description from meta or page body
        description = ""
        try:
            desc_el = driver.find_element(By.CSS_SELECTOR, "meta[name='description']")
            description = desc_el.get_attribute("content") or ""
        except: pass

        if not description:
            for sel in ["[class*='description']", "[class*='about']", "p"]:
                try:
                    el = driver.find_element(By.CSS_SELECTOR, sel)
                    description = el.text.strip()
                    if len(description) > 50: break
                except: pass

        if not description:
            return None

        # Extract scheme ID from URL
        clean_id = url.split("/schemes/")[-1].strip("/").split("?")[0]
        clean_id = re.sub(r'[^a-z0-9_-]', '_', clean_id.lower())[:50]

        # Try to get ministry
        ministry = "Government of India"
        for sel in ["[class*='ministry']", "[class*='department']"]:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                txt = el.text.strip()
                if txt: ministry = txt; break
            except: pass

        # Try to get documents
        documents = []
        try:
            doc_section = driver.find_elements(By.CSS_SELECTOR,
                "[class*='document'] li, [class*='required'] li"
            )
            for d in doc_section[:8]:
                txt = d.text.strip()
                if txt: documents.append(txt)
        except: pass

        if not documents:
            documents = ["Aadhaar Card", "Income Certificate", "Bank Account Details"]

        # Tags from page
        tags = []
        try:
            tag_els = driver.find_elements(By.CSS_SELECTOR,
                "[class*='tag'], [class*='badge'], [class*='category']"
            )
            for t in tag_els[:10]:
                txt = t.text.strip().lower()
                if txt and len(txt) < 30: tags.append(txt)
        except: pass

        return build_scheme_dict(clean_id, name, description, tags, url, ministry, documents)

    except Exception as e:
        return None


def intercept_api_from_logs(driver):
    """Parse Chrome performance logs to intercept API responses."""
    schemes = []
    try:
        logs = driver.get_log("performance")
        for log in logs:
            try:
                msg = json.loads(log["message"])["message"]
                if msg.get("method") == "Network.responseReceived":
                    resp_url = msg["params"]["response"]["url"]
                    if "scheme" in resp_url.lower() and "api" in resp_url.lower():
                        request_id = msg["params"]["requestId"]
                        try:
                            body_resp = driver.execute_cdp_cmd(
                                "Network.getResponseBody",
                                {"requestId": request_id}
                            )
                            data = json.loads(body_resp.get("body", "{}"))
                            hits = (
                                data.get("data", {}).get("hits") or
                                data.get("hits") or
                                data.get("schemes") or []
                            )
                            for hit in hits:
                                src  = hit.get("_source", hit)
                                name = src.get("schemeName", "").strip()
                                desc = (src.get("briefDescription") or src.get("description") or "").strip()
                                if name and desc:
                                    sid = re.sub(r'[^a-z0-9_]', '_', name.lower())[:50]
                                    s = build_scheme_dict(
                                        sid, name, desc,
                                        [t.lower() for t in src.get("tags", [])],
                                        f"https://www.myscheme.gov.in/schemes/{sid}",
                                        src.get("nodalMinistryName", "Government of India"),
                                        []
                                    )
                                    schemes.append(s)
                        except: pass
            except: pass
    except Exception as e:
        print(f"  ⚠ Log interception error: {e}")
    return schemes


# ── Eligibility Extractors ────────────────────────────────────────────────────
def extract_income(text):
    if not text: return None
    for pat in [
        r'income[^\d]*(?:less than|upto|below)[^\d]*(\d[\d,]*)\s*(?:lakh|lac)',
        r'(\d[\d,]*)\s*(?:lakh|lac)[^\d]*(?:per annum|per year)',
        r'below\s*(?:rs\.?|₹)?\s*(\d[\d,]*)\s*(?:lakh|lac)',
    ]:
        m = re.search(pat, text.lower())
        if m:
            try:
                val = int(m.group(1).replace(',',''))
                return val * 100000 if 'lakh' in m.group(0) else val
            except: pass
    return None

def extract_gender(text):
    if not text: return None
    t = text.lower()
    if any(w in t for w in ["men only","male only"]): return "male"
    if any(w in t for w in ["women","woman","girl","female","mahila","widow"]): return "female"
    return None

def extract_caste(tags, text):
    combined = " ".join(tags).lower() + " " + (text or "").lower()
    found = []
    if any(w in combined for w in ["scheduled caste"," sc ","dalit"]): found.append("sc")
    if any(w in combined for w in ["scheduled tribe"," st ","tribal"]): found.append("st")
    if any(w in combined for w in ["other backward"," obc "]): found.append("obc")
    return found or None

def extract_area_type(text):
    if not text: return None
    t = text.lower()
    r = any(w in t for w in ["rural","village","gram"])
    u = any(w in t for w in ["urban","city","town"])
    if r and u: return ["rural","urban"]
    if r: return ["rural"]
    if u: return ["urban"]
    return None

def extract_occupation(tags, text):
    combined = " ".join(tags).lower() + " " + (text or "").lower()
    return [o for o, kws in OCCUPATION_KEYWORDS.items() if any(k in combined for k in kws)] or None

def extract_age(text):
    if not text: return None, None
    m = re.search(r'(?:between|aged?)\s*(\d+)\s*(?:to|-)\s*(\d+)', text.lower())
    if m: return int(m.group(1)), int(m.group(2))
    mn = re.search(r'(?:above|minimum age)[^\d]*(\d+)', text.lower())
    mx = re.search(r'(?:below|upto|maximum age)[^\d]*(\d+)', text.lower())
    return (int(mn.group(1)) if mn else None), (int(mx.group(1)) if mx else None)

def detect_category(tags, name):
    combined = " ".join(tags).lower() + " " + name.lower()
    for kw, cat in CATEGORY_MAP.items():
        if kw in combined: return cat
    return "General"


# ── Build Scheme Dict ─────────────────────────────────────────────────────────
def build_scheme_dict(clean_id, name, description, tags, apply_link,
                      ministry="Government of India", documents=None):
    tags = [t.lower() for t in tags]
    category = detect_category(tags, name)
    full_text = description + " " + " ".join(tags)

    max_income       = extract_income(description)
    gender           = extract_gender(description)
    caste_list       = extract_caste(tags, description)
    area_type        = extract_area_type(description)
    occupations      = extract_occupation(tags, description)
    min_age, max_age = extract_age(description)

    eligibility = {}
    if max_income:               eligibility["max_income"] = max_income
    if gender:                   eligibility["gender"]     = gender
    if caste_list:               eligibility["caste"]      = caste_list
    if area_type:                eligibility["area_type"]  = area_type
    if occupations:              eligibility["occupation"] = occupations
    if min_age and min_age > 0:  eligibility["min_age"]    = min_age
    if max_age and max_age < 100:eligibility["max_age"]    = max_age
    if "bpl" in full_text or "below poverty" in full_text:
        eligibility["bpl"] = True

    return {
        "id":          clean_id,
        "name":        name,
        "category":    category,
        "ministry":    ministry,
        "description": description[:300],
        "benefits":    "Please check official portal for benefit details.",
        "eligibility": eligibility,
        "documents":   (documents or ["Aadhaar Card", "Income Certificate", "Bank Account"])[:8],
        "apply_link":  apply_link,
        "apply_steps": [
            "Visit the official portal or nearest CSC centre",
            "Fill the application form with personal details",
            "Upload required documents",
            "Submit and note your application reference number"
        ],
        "state":       "central",
        "tags":        tags[:10],
        "source":      "myscheme.gov.in",
        "fetched_at":  datetime.utcnow().isoformat()
    }


# ── Save to MongoDB ───────────────────────────────────────────────────────────
def save_to_mongo(schemes):
    col, client = get_collection()
    inserted = skipped = 0
    for scheme in schemes:
        try:
            res = col.update_one(
                {"id": scheme["id"]},
                {"$setOnInsert": scheme},
                upsert=True
            )
            if res.upserted_id: inserted += 1
            else: skipped += 1
        except Exception as e:
            print(f"  ⚠ Could not save '{scheme.get('name')}': {e}")
            skipped += 1
    client.close()
    return inserted, skipped


# ── Main ──────────────────────────────────────────────────────────────────────
def run(query="", state_filter="", size=50, preview=False):
    print("\n" + "="*55)
    print("  SchemeSaathi — MyScheme Selenium Scraper")
    print("="*55)

    print("\n📡 Scraping myscheme.gov.in with Selenium...")
    schemes = scrape_with_selenium(query, state_filter, size)

    if not schemes:
        print("\n❌ No schemes scraped.")
        print("   Make sure Google Chrome is installed.")
        print("   Run: pip install selenium webdriver-manager")
        return

    print(f"\n✓ Scraped {len(schemes)} schemes total")

    if preview:
        print("\n👀 Preview (first 3):")
        for s in schemes[:3]:
            print(json.dumps(s, indent=2, ensure_ascii=False))
        print("\n[Preview mode — nothing saved]")
        return

    print("\n💾 Saving to MongoDB...")
    inserted, skipped = save_to_mongo(schemes)

    print("\n" + "="*55)
    print("  ✅ Done!")
    print(f"  Scraped          : {len(schemes)}")
    print(f"  Newly added to DB: {inserted}")
    print(f"  Already existed  : {skipped}")
    print("="*55 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query",   default="",  help="Keyword e.g. 'women'")
    parser.add_argument("--state",   default="",  help="State e.g. 'maharashtra'")
    parser.add_argument("--size",    default=50,  type=int)
    parser.add_argument("--preview", action="store_true")
    args = parser.parse_args()
    run(query=args.query, state_filter=args.state, size=args.size, preview=args.preview)