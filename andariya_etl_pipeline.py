#!/usr/bin/env python3
"""
Daily sync: Airtable -> Supabase (Postgres)

Pulls all records from an Airtable table and upserts them into a Postgres
table hosted on Supabase, keyed on the Airtable record ID. Designed to run
once a day (e.g. via GitHub Actions cron) after researchers finish entry
for the day — not realtime, no webhooks needed.

Environment variables required:
    AIRTABLE_API_KEY     - Airtable personal access token
    AIRTABLE_BASE_ID     - Airtable base ID (starts with 'app')
    AIRTABLE_TABLE_NAME  - Name (or table ID) of the table to sync
    SUPABASE_DB_URL      - Postgres connection string from Supabase
                           (Project Settings -> Database -> Connection string,
                           "URI" format, with your DB password filled in)

Install dependencies:
    pip install -r requirements.txt

Run manually:
    python airtable_to_supabase_sync.py
"""

import os
import sys
import logging
from datetime import datetime, timezone

from pyairtable import Api
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("airtable_sync")

# Map Airtable field names -> Postgres column names.
# Edit the left-hand side (keys) to match your EXACT Airtable field names,
# including capitalization.
FIELD_MAP = {
    "date": "date",
    "platform": "platform",
    "date_collected": "date_collected",
    "URL": "url",
    "actor": "actor",
    "behaviour": "behaviour",
    "content": "content",
    "degree": "degree",
    "effect": "effect",
    "primary_narrative": "primary_narrative",
    "disarm_techniques": "disarm_techniques",
    "verification_status": "verification_status",
    "author": "author",
    "notes": "notes",
}

TABLE_NAME = "disinfo_entries"  # Postgres table name — see schema.sql


def get_env(name):
    value = os.environ.get(name)
    if not value:
        log.error("Missing required environment variable: %s", name)
        sys.exit(1)
    return value


def fetch_airtable_records():
    api_key = get_env("AIRTABLE_API_KEY")
    base_id = get_env("AIRTABLE_BASE_ID")
    table_name = get_env("AIRTABLE_TABLE_NAME")

    api = Api(api_key)
    table = api.table(base_id, table_name)

    log.info("Fetching records from Airtable table '%s'...", table_name)
    records = table.all()
    log.info("Fetched %d records from Airtable.", len(records))
    return records


def build_rows(records):
    rows = []
    for rec in records:
        fields = rec.get("fields", {})
        row = {"airtable_record_id": rec["id"]}
        for airtable_field, pg_column in FIELD_MAP.items():
            value = fields.get(airtable_field)
            # Multi-select / linked-record fields come back as lists in
            # Airtable's API — flatten to a comma-separated string so a
            # plain text column can hold them.
            if isinstance(value, list):
                value = ", ".join(str(v) for v in value)
            row[pg_column] = value
        rows.append(row)
    return rows


def upsert_rows(rows):
    if not rows:
        log.info("No rows to sync.")
        return

    db_url = get_env("SUPABASE_DB_URL")
    columns = ["airtable_record_id"] + list(FIELD_MAP.values()) + ["synced_at"]
    now = datetime.now(timezone.utc)

    values = [
        tuple(row.get(col) for col in columns[:-1]) + (now,)
        for row in rows
    ]

    update_clause = ", ".join(
        f"{col} = EXCLUDED.{col}" for col in columns if col != "airtable_record_id"
    )

    insert_sql = f"""
        INSERT INTO {TABLE_NAME} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (airtable_record_id) DO UPDATE SET {update_clause}
    """

    log.info("Connecting to Supabase Postgres...")
    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, values)
        log.info("Upserted %d rows into '%s'.", len(values), TABLE_NAME)
    finally:
        conn.close()


def main():
    records = fetch_airtable_records()
    rows = build_rows(records)
    upsert_rows(rows)
    log.info("Sync complete.")


if __name__ == "__main__":
    main()
