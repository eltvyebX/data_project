#!/usr/bin/env python3
"""
Andariya Sudan Digital Resilience - ETL Pipeline
Syncs Google Sheets monitoring data to Postgres every 15 minutes
Two-table architecture: raw_posts (landing) + clean_posts (analysis-ready)
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass

import pandas as pd
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

# ============================================================================
# CONFIGURATION
# ============================================================================

# Google Sheets Config
GOOGLE_SHEETS_CREDENTIALS_PATH = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "credentials.json")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "your-spreadsheet-id-here")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Monitoring Data")

# Postgres Config
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "andariya_dw"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "your-password"),
}

# Sync Config
SYNC_INTERVAL_MINUTES = 15
BATCH_SIZE = 100

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("etl_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class RawPost:
    """Raw data from Google Sheets - immutable landing zone"""
    platform: str
    date_collected: Optional[str]
    url: str
    actor: str
    behaviour: str
    content_type: str
    degree: str
    effect: str
    primary_narrative: str
    disarm_techniques: str
    verification_status: str
    notes: Optional[str]
    sheet_row_number: int
    import_batch_id: str

    def to_tuple(self):
        return (
            self.platform, self.date_collected, self.url, self.actor,
            self.behaviour, self.content_type, self.degree, self.effect,
            self.primary_narrative, self.disarm_techniques,
            self.verification_status, self.notes, self.sheet_row_number,
            self.import_batch_id
        )


@dataclass  
class CleanPost:
    """Transformed data for analysis"""
    platform: str
    date_collected: Optional[datetime]
    url: str
    actor_category: str
    behaviour_code: str
    content_type: str
    degree_numeric: Optional[int]
    degree_qualitative: str
    effect_category: str
    primary_narrative: str
    disarm_tactic: str
    disarm_technique: str
    verification_status: str
    notes: Optional[str]
    source_raw_id: int


# ============================================================================
# GOOGLE SHEETS CLIENT
# ============================================================================

class GoogleSheetsClient:
    """Handles all Google Sheets interactions"""

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ]

    def __init__(self, credentials_path: str, spreadsheet_id: str):
        self.credentials_path = credentials_path
        self.spreadsheet_id = spreadsheet_id
        self.client = None
        self.spreadsheet = None

    def connect(self) -> None:
        """Establish connection to Google Sheets API"""
        try:
            creds = Credentials.from_service_account_file(
                self.credentials_path, scopes=self.SCOPES
            )
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
            logger.info(f"Connected to spreadsheet: {self.spreadsheet.title}")
        except Exception as e:
            logger.error(f"Failed to connect to Google Sheets: {e}")
            raise

    def get_worksheet_data(self, worksheet_name: str) -> pd.DataFrame:
        """Fetch all data from specified worksheet"""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            logger.info(f"Fetched {len(df)} rows from worksheet '{worksheet_name}'")
            return df
        except APIError as e:
            logger.error(f"Google Sheets API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching worksheet data: {e}")
            raise


# ============================================================================
# POSTGRES CLIENT
# ============================================================================

class PostgresClient:
    """Handles all Postgres database operations"""

    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.pool = None

    def connect(self) -> None:
        """Initialize connection pool"""
        try:
            self.pool = ThreadedConnectionPool(
                minconn=1, maxconn=5,
                **self.config
            )
            logger.info("Postgres connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to connect to Postgres: {e}")
            raise

    def get_conn(self):
        """Get connection from pool"""
        return self.pool.getconn()

    def put_conn(self, conn):
        """Return connection to pool"""
        self.pool.putconn(conn)

    def execute(self, query: str, params: tuple = ()) -> List[Dict]:
        """Execute query and return results"""
        conn = self.get_conn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                if cur.description:
                    return cur.fetchall()
                conn.commit()
                return []
        except Exception as e:
            conn.rollback()
            logger.error(f"Query execution error: {e}")
            raise
        finally:
            self.put_conn(conn)

    def execute_many(self, query: str, values: List[tuple]) -> int:
        """Execute batch insert/update"""
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                execute_values(cur, query, values, page_size=BATCH_SIZE)
                conn.commit()
                return cur.rowcount
        except Exception as e:
            conn.rollback()
            logger.error(f"Batch execution error: {e}")
            raise
        finally:
            self.put_conn(conn)


# ============================================================================
# DATABASE SCHEMA SETUP
# ============================================================================

SCHEMA_SQL = """
-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Raw data table: immutable landing zone
CREATE TABLE IF NOT EXISTS raw_posts (
    id SERIAL PRIMARY KEY,
    platform VARCHAR(50) NOT NULL,
    date_collected DATE,
    url TEXT NOT NULL,
    actor TEXT,
    behaviour TEXT,
    content_type VARCHAR(50),
    degree TEXT,
    effect TEXT,
    primary_narrative TEXT,
    disarm_techniques TEXT,
    verification_status VARCHAR(50),
    notes TEXT,
    sheet_row_number INTEGER,
    import_batch_id VARCHAR(64) NOT NULL,
    imported_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(url, import_batch_id)
);

-- Clean data table: analysis-ready
CREATE TABLE IF NOT EXISTS clean_posts (
    id SERIAL PRIMARY KEY,
    platform VARCHAR(50) NOT NULL,
    date_collected DATE,
    url TEXT NOT NULL,
    actor_category VARCHAR(100),
    behaviour_code VARCHAR(50),
    content_type VARCHAR(50),
    degree_numeric INTEGER,
    degree_qualitative VARCHAR(20),
    effect_category VARCHAR(100),
    primary_narrative TEXT,
    disarm_tactic VARCHAR(100),
    disarm_technique VARCHAR(100),
    verification_status VARCHAR(50),
    notes TEXT,
    source_raw_id INTEGER REFERENCES raw_posts(id),
    cleaned_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(url)
);

-- Sync tracking table
CREATE TABLE IF NOT EXISTS sync_log (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    sync_started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    sync_completed_at TIMESTAMP WITH TIME ZONE,
    rows_fetched INTEGER DEFAULT 0,
    rows_inserted_raw INTEGER DEFAULT 0,
    rows_inserted_clean INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'running',
    error_message TEXT
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_raw_posts_batch ON raw_posts(import_batch_id);
CREATE INDEX IF NOT EXISTS idx_raw_posts_url ON raw_posts(url);
CREATE INDEX IF NOT EXISTS idx_clean_posts_platform ON clean_posts(platform);
CREATE INDEX IF NOT EXISTS idx_clean_posts_date ON clean_posts(date_collected);
CREATE INDEX IF NOT EXISTS idx_clean_posts_actor ON clean_posts(actor_category);
CREATE INDEX IF NOT EXISTS idx_clean_posts_disarm ON clean_posts(disarm_tactic);
"""


def setup_database(pg_client: PostgresClient) -> None:
    """Initialize database schema"""
    logger.info("Setting up database schema...")
    pg_client.execute(SCHEMA_SQL)
    logger.info("Database schema ready")


# ============================================================================
# DATA TRANSFORMATION
# ============================================================================

class DataTransformer:
    """Transforms raw data to clean, analysis-ready format"""

    # ABCDE Framework normalization
    ACTOR_CATEGORIES = {
        "individual": "Individual",
        "organization": "Organization", 
        "org": "Organization",
        "state": "State Actor",
        "influencer": "Influencer",
        "media": "Media Outlet",
        "pseudo-media": "Pseudo-Media",
        "anonymous": "Anonymous Account",
        "bot": "Bot/Automated",
    }

    # DISARM Tactic mapping
    DISARM_TACTICS = {
        "content manipulation": "Content Manipulation",
        "fabrication": "Content Manipulation",
        "alteration": "Content Manipulation",
        "amplification": "Amplification & Coordination",
        "coordination": "Amplification & Coordination",
        "bot network": "Amplification & Coordination",
        "narrative laundering": "Narrative Laundering",
        "targeting": "Targeting & Harassment",
        "harassment": "Targeting & Harassment",
        "identity": "Deceptive Identity",
        "impersonation": "Deceptive Identity",
        "suppression": "Information Suppression",
    }

    @staticmethod
    def normalize_actor(actor_raw: str) -> str:
        """Normalize actor to standard category"""
        if not actor_raw:
            return "Unknown"
        actor_lower = actor_raw.lower().strip()
        for key, value in DataTransformer.ACTOR_CATEGORIES.items():
            if key in actor_lower:
                return value
        return actor_raw.strip()[:100]

    @staticmethod
    def parse_degree(degree_raw: str) -> tuple:
        """Parse degree into numeric and qualitative components"""
        if not degree_raw:
            return None, "unknown"

        degree_str = str(degree_raw).lower().strip()

        # Try to extract numbers
        import re
        numbers = re.findall(r'[\d,]+', degree_str)
        numeric = None
        if numbers:
            try:
                numeric = int(numbers[0].replace(',', ''))
            except ValueError:
                pass

        # Qualitative assessment
        qualitative = "unknown"
        if any(word in degree_str for word in ["high", "viral", "massive", "widespread"]):
            qualitative = "high"
        elif any(word in degree_str for word in ["medium", "moderate", "significant"]):
            qualitative = "medium"
        elif any(word in degree_str for word in ["low", "minimal", "limited"]):
            qualitative = "low"

        return numeric, qualitative

    @staticmethod
    def extract_disarm_tactic(techniques_raw: str) -> tuple:
        """Extract DISARM tactic and technique from raw text"""
        if not techniques_raw:
            return "Unknown", "Unknown"

        tech_lower = techniques_raw.lower().strip()

        tactic = "Other"
        for key, value in DataTransformer.DISARM_TACTICS.items():
            if key in tech_lower:
                tactic = value
                break

        # Extract specific technique (first sentence or phrase)
        technique = techniques_raw.strip()[:100]

        return tactic, technique

    @staticmethod
    def transform_row(row: pd.Series, raw_id: int) -> CleanPost:
        """Transform a single raw row to clean format"""
        degree_num, degree_qual = DataTransformer.parse_degree(row.get("Degree", ""))
        tactic, technique = DataTransformer.extract_disarm_tactic(row.get("DISARM Technique(s)", ""))

        # Parse date
        date_collected = None
        date_raw = row.get("Date collected", "")
        if date_raw:
            try:
                date_collected = pd.to_datetime(date_raw).date()
            except:
                pass

        return CleanPost(
            platform=str(row.get("Platform", "")).strip()[:50],
            date_collected=date_collected,
            url=str(row.get("URL", "")).strip(),
            actor_category=DataTransformer.normalize_actor(row.get("Actor", "")),
            behaviour_code=str(row.get("Behaviour", "")).strip()[:50],
            content_type=str(row.get("Content", "")).strip()[:50],
            degree_numeric=degree_num,
            degree_qualitative=degree_qual,
            effect_category=str(row.get("Effect", "")).strip()[:100],
            primary_narrative=str(row.get("Primary Narrative", "")).strip(),
            disarm_tactic=tactic,
            disarm_technique=technique,
            verification_status=str(row.get("Verification Status", "")).strip()[:50],
            notes=str(row.get("Notes", "")).strip() if pd.notna(row.get("Notes", "")) else None,
            source_raw_id=raw_id
        )


# ============================================================================
# ETL PIPELINE ORCHESTRATOR
# ============================================================================

class ETLPipeline:
    """Main ETL orchestrator"""

    EXPECTED_COLUMNS = [
        "Platform", "Date collected", "URL", "Actor", "Behaviour",
        "Content", "Degree", "Effect", "Primary Narrative",
        "DISARM Technique(s)", "Verification Status", "Notes"
    ]

    def __init__(self):
        self.sheets_client = GoogleSheetsClient(GOOGLE_SHEETS_CREDENTIALS_PATH, SPREADSHEET_ID)
        self.pg_client = PostgresClient(POSTGRES_CONFIG)
        self.transformer = DataTransformer()

    def initialize(self) -> None:
        """Initialize all connections"""
        logger.info("Initializing ETL pipeline...")
        self.sheets_client.connect()
        self.pg_client.connect()
        setup_database(self.pg_client)
        logger.info("ETL pipeline initialized successfully")

    def validate_dataframe(self, df: pd.DataFrame) -> bool:
        """Validate incoming data structure"""
        missing_cols = set(self.EXPECTED_COLUMNS) - set(df.columns)
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False

        # Check for empty dataframe
        if df.empty:
            logger.warning("Dataframe is empty - no data to process")
            return False

        # Check for required fields
        null_urls = df["URL"].isna().sum()
        if null_urls > 0:
            logger.warning(f"{null_urls} rows have missing URLs")

        logger.info(f"Data validation passed: {len(df)} rows, {len(df.columns)} columns")
        return True

    def sync(self) -> Dict:
        """Execute full sync cycle"""
        batch_id = hashlib.sha256(
            datetime.now().isoformat().encode()
        ).hexdigest()[:16]

        sync_result = {
            "batch_id": batch_id,
            "status": "success",
            "rows_fetched": 0,
            "rows_inserted_raw": 0,
            "rows_inserted_clean": 0,
            "errors": []
        }

        # Start sync log
        self.pg_client.execute(
            "INSERT INTO sync_log (batch_id, status) VALUES (%s, %s)",
            (batch_id, "running")
        )

        try:
            # Step 1: Fetch from Google Sheets
            logger.info(f"[{batch_id}] Starting sync cycle...")
            df = self.sheets_client.get_worksheet_data(WORKSHEET_NAME)
            sync_result["rows_fetched"] = len(df)

            if not self.validate_dataframe(df):
                sync_result["status"] = "validation_failed"
                return sync_result

            # Step 2: Transform to raw format
            raw_posts = []
            for idx, row in df.iterrows():
                raw_post = RawPost(
                    platform=str(row.get("Platform", "")),
                    date_collected=str(row.get("Date collected", "")) if pd.notna(row.get("Date collected", "")) else None,
                    url=str(row.get("URL", "")),
                    actor=str(row.get("Actor", "")),
                    behaviour=str(row.get("Behaviour", "")),
                    content_type=str(row.get("Content", "")),
                    degree=str(row.get("Degree", "")),
                    effect=str(row.get("Effect", "")),
                    primary_narrative=str(row.get("Primary Narrative", "")),
                    disarm_techniques=str(row.get("DISARM Technique(s)", "")),
                    verification_status=str(row.get("Verification Status", "")),
                    notes=str(row.get("Notes", "")) if pd.notna(row.get("Notes", "")) else None,
                    sheet_row_number=idx + 2,  # +2 for header and 0-indexing
                    import_batch_id=batch_id
                )
                raw_posts.append(raw_post)

            # Step 3: Insert raw data (skip duplicates based on URL + batch)
            raw_query = """
                INSERT INTO raw_posts 
                (platform, date_collected, url, actor, behaviour, content_type, 
                 degree, effect, primary_narrative, disarm_techniques, 
                 verification_status, notes, sheet_row_number, import_batch_id)
                VALUES %s
                ON CONFLICT (url, import_batch_id) DO NOTHING
                RETURNING id
            """
            raw_values = [p.to_tuple() for p in raw_posts]

            # Use individual inserts to get IDs back
            inserted_raw_ids = []
            conn = self.pg_client.get_conn()
            try:
                with conn.cursor() as cur:
                    for post in raw_posts:
                        cur.execute("""
                            INSERT INTO raw_posts 
                            (platform, date_collected, url, actor, behaviour, content_type, 
                             degree, effect, primary_narrative, disarm_techniques, 
                             verification_status, notes, sheet_row_number, import_batch_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (url, import_batch_id) DO NOTHING
                            RETURNING id
                        """, post.to_tuple())
                        result = cur.fetchone()
                        if result:
                            inserted_raw_ids.append(result[0])
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise
            finally:
                self.pg_client.put_conn(conn)

            sync_result["rows_inserted_raw"] = len(inserted_raw_ids)
            logger.info(f"[{batch_id}] Inserted {len(inserted_raw_ids)} new raw rows")

            # Step 4: Transform and insert clean data
            if inserted_raw_ids:
                clean_posts = []
                for raw_id in inserted_raw_ids:
                    # Fetch the raw row
                    raw_row = self.pg_client.execute(
                        "SELECT * FROM raw_posts WHERE id = %s", (raw_id,)
                    )[0]

                    # Transform
                    clean_post = self.transformer.transform_row(
                        pd.Series(raw_row), raw_id
                    )
                    clean_posts.append(clean_post)

                # Upsert clean data
                clean_values = [
                    (
                        cp.platform, cp.date_collected, cp.url, cp.actor_category,
                        cp.behaviour_code, cp.content_type, cp.degree_numeric,
                        cp.degree_qualitative, cp.effect_category, cp.primary_narrative,
                        cp.disarm_tactic, cp.disarm_technique, cp.verification_status,
                        cp.notes, cp.source_raw_id
                    )
                    for cp in clean_posts
                ]

                clean_query = """
                    INSERT INTO clean_posts 
                    (platform, date_collected, url, actor_category, behaviour_code,
                     content_type, degree_numeric, degree_qualitative, effect_category,
                     primary_narrative, disarm_tactic, disarm_technique,
                     verification_status, notes, source_raw_id)
                    VALUES %s
                    ON CONFLICT (url) DO UPDATE SET
                        platform = EXCLUDED.platform,
                        date_collected = EXCLUDED.date_collected,
                        actor_category = EXCLUDED.actor_category,
                        behaviour_code = EXCLUDED.behaviour_code,
                        content_type = EXCLUDED.content_type,
                        degree_numeric = EXCLUDED.degree_numeric,
                        degree_qualitative = EXCLUDED.degree_qualitative,
                        effect_category = EXCLUDED.effect_category,
                        primary_narrative = EXCLUDED.primary_narrative,
                        disarm_tactic = EXCLUDED.disarm_tactic,
                        disarm_technique = EXCLUDED.disarm_technique,
                        verification_status = EXCLUDED.verification_status,
                        notes = EXCLUDED.notes,
                        source_raw_id = EXCLUDED.source_raw_id,
                        cleaned_at = NOW()
                """

                rows_clean = self.pg_client.execute_many(clean_query, clean_values)
                sync_result["rows_inserted_clean"] = rows_clean
                logger.info(f"[{batch_id}] Upserted {rows_clean} clean rows")

            # Update sync log
            self.pg_client.execute(
                """UPDATE sync_log 
                   SET sync_completed_at = NOW(), 
                       rows_fetched = %s,
                       rows_inserted_raw = %s,
                       rows_inserted_clean = %s,
                       status = %s
                   WHERE batch_id = %s""",
                (sync_result["rows_fetched"], sync_result["rows_inserted_raw"],
                 sync_result["rows_inserted_clean"], "success", batch_id)
            )

            logger.info(f"[{batch_id}] Sync completed successfully")

        except Exception as e:
            error_msg = str(e)
            sync_result["status"] = "failed"
            sync_result["errors"].append(error_msg)
            logger.error(f"[{batch_id}] Sync failed: {error_msg}")

            # Update sync log with error
            self.pg_client.execute(
                """UPDATE sync_log 
                   SET sync_completed_at = NOW(), 
                       status = %s,
                       error_message = %s
                   WHERE batch_id = %s""",
                ("failed", error_msg, batch_id)
            )

        return sync_result


# ============================================================================
# SCHEDULER
# ============================================================================

def run_scheduler():
    """Run ETL every 15 minutes"""
    import time

    pipeline = ETLPipeline()
    pipeline.initialize()

    logger.info(f"Starting scheduler: sync every {SYNC_INTERVAL_MINUTES} minutes")

    while True:
        try:
            result = pipeline.sync()
            logger.info(f"Sync result: {json.dumps(result, indent=2, default=str)}")
        except Exception as e:
            logger.error(f"Unhandled error in sync cycle: {e}")

        next_run = datetime.now() + timedelta(minutes=SYNC_INTERVAL_MINUTES)
        logger.info(f"Next sync scheduled at: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(SYNC_INTERVAL_MINUTES * 60)


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Andariya ETL Pipeline")
    parser.add_argument("--run-once", action="store_true", help="Run single sync and exit")
    parser.add_argument("--setup-only", action="store_true", help="Setup database and exit")
    args = parser.parse_args()

    pipeline = ETLPipeline()
    pipeline.initialize()

    if args.setup_only:
        logger.info("Database setup complete")
    elif args.run_once:
        result = pipeline.sync()
        print(json.dumps(result, indent=2, default=str))
    else:
        run_scheduler()
