-- Run this once in the Supabase SQL editor before the first sync.

create table if not exists disinfo_entries (
    airtable_record_id text primary key,
    date date,
    platform text,
    date_collected date,
    url text,
    actor text,
    behaviour text,
    content text,
    degree text,
    effect text,
    primary_narrative text,
    disarm_techniques text,
    verification_status text,
    author text,
    notes text,
    synced_at timestamptz
);
