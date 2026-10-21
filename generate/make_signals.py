"""Generate synthetic B2B signal data from five sources, plus the ground truth.

Every record knows which real person it belongs to (ground_truth.csv), so the
matcher can be scored. The data is messy on purpose, the way real signal data is:

  - nicknames (Robert in the CRM, Bob on LinkedIn)
  - company name variants (Acme Inc, ACME, acme.io)
  - emails with mixed case and stray spaces
  - website form fills that only carry a SHA-256 hash of the email
  - anonymous website visits (company known from IP, person unknown)
  - job changes (a person moves to a new company)
  - name collisions: two different people with the same name at the same company

    python generate/make_signals.py --batch 1      first delivery
    python generate/make_signals.py --batch 2      a later delivery with new signals
All names, companies and emails are fake.
"""
import argparse
import csv
import hashlib
import json
import random
from datetime import date, timedelta
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "landing"

FIRST = [("Robert", "Bob"), ("William", "Bill"), ("Elizabeth", "Liz"), ("Katherine", "Kate"),
         ("Michael", "Mike"), ("James", "Jim"), ("Jennifer", "Jen"), ("Daniel", "Dan"),
         ("Christopher", "Chris"), ("Margaret", "Maggie"), ("Anthony", "Tony"), ("Samantha", "Sam"),
         ("Priya", None), ("Wei", None), ("Fatima", None), ("Carlos", None), ("Aisha", None),
         ("Noah", None), ("Olivia", None), ("Hassan", None), ("Mei", None), ("Lucas", None),
         ("Sofia", None), ("Arjun", None), ("Grace", None), ("Mateo", None), ("Zara", None)]
LAST = ["Nguyen", "Patel", "Garcia", "Kim", "Okafor", "Silva", "Cohen", "Haddad", "Novak", "Rossi",
        "Tanaka", "Murphy", "Ali", "Schmidt", "Dubois", "Kowalski", "Mensah", "Park", "Reyes", "Larsen",
        "Brooks", "Iqbal", "Chen", "Lopez", "Walsh", "Moreau", "Singh", "Adeyemi", "Fischer", "Costa"]
TITLES = ["VP Marketing", "Director of Demand Generation", "Head of Growth", "Marketing Manager",
          "CMO", "Revenue Operations Manager", "Director of Sales", "Account Executive",
          "Data Analyst", "Software Engineer", "Head of Product", "Customer Success Manager"]
COMPANY_STEMS = ["Acme", "Bluefin", "Cobalt", "Dovetail", "Evergreen", "Fathom", "Granite", "Harbor",
                 "Ironwood", "Juniper", "Keystone", "Lumen", "Meridian", "Northwind", "Orchard",
                 "Pinnacle", "Quarry", "Redwood", "Summit", "Tidal", "Upland", "Vantage", "Willow",
                 "Xenon", "Yardley", "Zephyr", "Atlas", "Beacon", "Cascade", "Delta"]
SUFFIX = ["Inc", "Labs", "Software", "Systems", "Group", "AI"]
PAGES = ["/pricing", "/demo", "/product", "/blog/abm-guide", "/case-studies", "/integrations", "/careers"]


def company_list(rng):
    companies = []
    for stem in COMPANY_STEMS:
        suffix = rng.choice(SUFFIX)
        domain = f"{stem.lower()}{rng.choice(['.com', '.io', '.ai'])}"
        variants = [f"{stem} {suffix}", stem.upper(), f"{stem} {suffix}.", domain, f"{stem}"]
        companies.append({"name": f"{stem} {suffix}", "domain": domain, "variants": variants})
    return companies


def sha(email):
    return hashlib.sha256(email.strip().lower().encode()).hexdigest()


def messy_email(email, rng):
    if rng.random() < 0.25:
        email = email.upper() if rng.random() < 0.5 else email.title()
    if rng.random() < 0.15:
        email = f" {email} "
    return email


def build_people(rng, companies, n=420):
    people, used = [], set()
    pid = 0
    for _ in range(n):
        first, nick = rng.choice(FIRST)
        last = rng.choice(LAST)
        comp = rng.choice(companies)
        email = f"{first.lower()}.{last.lower()}@{comp['domain']}"
        if email in used:
            email = f"{first.lower()[0]}{last.lower()}{rng.randint(2, 99)}@{comp['domain']}"
        used.add(email)
        pid += 1
        people.append({"person_id": f"P{pid:04d}", "first": first, "nick": nick, "last": last,
                       "company": comp, "email": email, "title": rng.choice(TITLES)})
    # Name collisions: a second, different person with the same name at the same company
    for p in rng.sample(people, 6):
        pid += 1
        email = f"{p['first'].lower()[0]}{p['last'].lower()}.{rng.randint(100, 999)}@{p['company']['domain']}"
        people.append({**p, "person_id": f"P{pid:04d}", "email": email, "title": rng.choice(TITLES),
                       "collision": True})
    return people


def main(batch):
    rng = random.Random(16)
    companies = company_list(rng)
    people = build_people(rng, companies)
    rng = random.Random(100 + batch)   # each batch has new signals for the same people
    start = date(2026, 9, 1) + timedelta(days=21 * (batch - 1))
    out = OUT / f"batch={batch}"
    out.mkdir(parents=True, exist_ok=True)
    truth = []

    # 1. CRM contacts (batch 1 only; the CRM is the system of record)
    if batch == 1:
        crm = []
        for i, p in enumerate(people):
            if rng.random() < 0.7:
                rid = f"crm-{i:05d}"
                crm.append({"crm_id": rid, "first_name": p["first"], "last_name": p["last"],
                            "email": messy_email(p["email"], rng) if rng.random() < 0.9 else "",
                            "company": rng.choice(p["company"]["variants"][:3]), "title": p["title"]})
                truth.append((rid, "crm", p["person_id"]))
        write_csv(out / "crm_contacts.csv", crm)

    # 2. Website visits (JSON lines). Some are form fills with a hashed email, most are anonymous.
    visits = []
    for i in range(1400):
        p = rng.choice(people)
        rid = f"web-{batch}-{i:05d}"
        known = rng.random() < 0.35
        visits.append({"event_id": rid, "cookie_id": "ck_" + hashlib.md5(f"{p['person_id']}-{batch}".encode()).hexdigest()[:10],
                       "ts": f"{start + timedelta(days=rng.randint(0, 20))}T{rng.randint(8, 19):02d}:{rng.randint(0, 59):02d}:00Z",
                       "page": rng.choice(PAGES), "ip_company_domain": p["company"]["domain"] if rng.random() < 0.8 else None,
                       "email_sha256": sha(p["email"]) if known else None})
        truth.append((rid, "web", p["person_id"] if known else "anonymous"))
    (out / "web_visits.jsonl").write_text("\n".join(json.dumps(v) for v in visits) + "\n")

    # 3. LinkedIn engagement: name (often a nickname) and a free-text company name
    li = []
    for i in range(500):
        p = rng.choice(people)
        rid = f"li-{batch}-{i:05d}"
        first = p["nick"] if p["nick"] and rng.random() < 0.5 else p["first"]
        li.append({"engagement_id": rid, "full_name": f"{first} {p['last']}",
                   "company_name": rng.choice(p["company"]["variants"]),
                   "action": rng.choice(["ad_click", "post_like", "comment", "follow"]),
                   "ts": f"{start + timedelta(days=rng.randint(0, 20))}"})
        truth.append((rid, "linkedin", p["person_id"]))
    write_csv(out / "linkedin_engagement.csv", li)

    # 4. Job changes: a person moves to a different company
    jc = []
    for i, p in enumerate(rng.sample([p for p in people if not p.get("collision")], 12)):
        new = rng.choice([c for c in companies if c["domain"] != p["company"]["domain"]])
        rid = f"jc-{batch}-{i:03d}"
        jc.append({"change_id": rid, "first_name": p["first"], "last_name": p["last"],
                   "old_company": p["company"]["name"], "new_company": new["name"],
                   "new_company_domain": new["domain"], "changed_on": f"{start + timedelta(days=rng.randint(0, 20))}"})
        truth.append((rid, "job_change", p["person_id"]))
    write_csv(out / "job_changes.csv", jc)

    # 5. Data provider delivery (Parquet, the kind of large file that lands on S3)
    prov = []
    for i, p in enumerate(rng.sample(people, 260)):
        rid = f"prov-{batch}-{i:05d}"
        prov.append({"provider_id": rid, "email": p["email"], "first_name": p["first"], "last_name": p["last"],
                     "company_domain": p["company"]["domain"], "title": p["title"],
                     "intent_topic": rng.choice(["abm", "intent data", "linkedin ads", "marketing attribution"]),
                     "intent_score": rng.randint(20, 100)})
        truth.append((rid, "provider", p["person_id"]))
    con = duckdb.connect()
    con.register("prov", __import__("pandas").DataFrame(prov))
    con.execute(f"COPY prov TO '{out / 'provider_delivery.parquet'}' (FORMAT PARQUET)")

    truth_path = ROOT / "data" / "ground_truth.csv"
    mode = "a" if batch > 1 and truth_path.exists() else "w"
    with open(truth_path, mode, newline="") as f:
        w = csv.writer(f)
        if mode == "w":
            w.writerow(["record_id", "source", "person_id"])
        w.writerows(truth)
    print(f"batch {batch}: wrote {len(truth)} records to {out}")


def write_csv(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=1)
    main(ap.parse_args().batch)
