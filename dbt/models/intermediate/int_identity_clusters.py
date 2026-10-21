"""Identity resolution: decide which records belong to the same real person.

Round 1, email: records with the same email (or the same SHA-256 of an email)
are the same person. This is the strongest evidence.

Round 2, name + company: records with the same first name (nicknames mapped to
the full name), last name and company key are linked, with one guard. If the
same name and company already belong to two people with different emails, that
is a name collision, and records without an email are left unresolved instead
of being attached to the wrong person.

Each person gets a stable key based on their email, so the key does not change
between runs as new signals arrive.
"""
import hashlib

import pandas as pd


class UnionFind:
    def __init__(self, ids):
        self.parent = {i: i for i in ids}
        self.emails = {i: set() for i in ids}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return True
        ea, eb = self.emails[ra], self.emails[rb]
        if ea and eb and ea.isdisjoint(eb):
            return False          # guard: two different known people
        self.parent[rb] = ra
        self.emails[ra] |= eb
        return True


def resolve(records: pd.DataFrame) -> pd.DataFrame:
    """records: record_id, source, email_sha256, first_key, last_key, company_key, batch"""
    recs = records.reset_index(drop=True)
    ids = list(recs["record_id"])
    uf = UnionFind(ids)
    rule = {i: set() for i in ids}

    for rid, sha in zip(recs["record_id"], recs["email_sha256"]):
        if isinstance(sha, str) and sha:
            uf.emails[rid].add(sha)

    # Round 1: email
    for sha, grp in recs[recs["email_sha256"].notna()].groupby("email_sha256"):
        first = grp["record_id"].iloc[0]
        for rid in grp["record_id"].iloc[1:]:
            uf.union(first, rid)
        if len(grp) > 1:
            for rid in grp["record_id"]:
                rule[rid].add("email")

    # Round 2: name + company, with the collision guard
    named = recs[recs["first_key"].notna() & recs["last_key"].notna() & recs["company_key"].notna()]
    ambiguous = set()
    for _, grp in named.groupby(["first_key", "last_key", "company_key"]):
        rids = list(grp["record_id"])
        known_people = {uf.find(r) for r in rids if uf.emails[uf.find(r)]}
        if len(known_people) > 1:
            # Same name at the same company, two different emails: a real collision.
            ambiguous.update(r for r in rids if not uf.emails[uf.find(r)])
            continue
        anchor = next((r for r in rids if uf.emails[uf.find(r)]), rids[0])
        for rid in rids:
            if rid != anchor and uf.union(anchor, rid):
                rule[rid].add("name_company")
                rule[anchor].add("name_company")

    out = []
    for rid, src, batch in zip(recs["record_id"], recs["source"], recs["batch"]):
        root = uf.find(rid)
        emails = uf.emails[root]
        if rid in ambiguous and not emails:
            key, how = None, "ambiguous"
        elif emails:
            key = "per_" + hashlib.md5(min(emails).encode()).hexdigest()[:12]
            how = "+".join(sorted(rule[rid])) or "email"
        else:
            # Never linked to a known person: keep it, but do not invent an identity.
            key, how = None, "unresolved"
        out.append({"record_id": rid, "source": src, "person_key": key, "match_rule": how, "batch": batch})
    return pd.DataFrame(out)


def model(dbt, session):
    dbt.config(materialized="table")
    records = dbt.ref("int_identity_records").df()
    return resolve(records)
