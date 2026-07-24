#!/usr/bin/env python3
"""
Andariya ETL - Test & Validation Script
Run this to verify your setup before the full pipeline
"""

import os
import sys
from datetime import datetime

def test_google_sheets_connection():
    """Test Google Sheets API connection"""
    print("\n🔍 Testing Google Sheets connection...")
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        creds_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "credentials.json")

        if not os.path.exists(creds_path):
            print(f"❌ Credentials file not found: {creds_path}")
            return False

        creds = Credentials.from_service_account_file(
            creds_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        client = gspread.authorize(creds)

        spreadsheet_id = os.getenv("SPREADSHEET_ID", "")
        if not spreadsheet_id or spreadsheet_id == "your-spreadsheet-id-here"::
            print("❌ SPREADSHEET_ID not configured in .env")
            return False

        spreadsheet = client.open_by_key(spreadsheet_id)
        print(f"✅ Connected to spreadsheet: {spreadsheet.title}")

        worksheet_name = os.getenv("WORKSHEET_NAME", "Monitoring Data")
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            data = worksheet.get_all_records()
            print(f"✅ Found worksheet '{worksheet_name}' with {len(data)} rows")

            if data:
                print(f"   Columns: {list(data[0].keys())}")
            return True
        except Exception as e:
            print(f"❌ Error accessing worksheet: {e}")
            return False

    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        return False
    except Exception as e:
        print(f"❌ Google Sheets connection failed: {e}")
        return False


def test_postgres_connection():
    """Test PostgreSQL connection"""
    print("\n🔍 Testing PostgreSQL connection...")
    try:
        import psycopg2

        config = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "database": os.getenv("POSTGRES_DB", "andariya_dw"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", ""),
        }

        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()

        print(f"✅ Connected to PostgreSQL: {version.split()[0]} {version.split()[1]}")
        return True

    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        return False
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        return False


def validate_google_sheet_structure():
    """Validate Google Sheet has required columns"""
    print("\n🔍 Validating Google Sheet structure...")

    required_columns = [
        "Platform", "Date collected", "URL", "Actor", "Behaviour",
        "Content", "Degree", "Effect", "Primary Narrative",
        "DISARM Technique(s)", "Verification Status"
    ]

    try:
        import gspread
        from google.oauth2.service_account import Credentials

        creds = Credentials.from_service_account_file(
            os.getenv("GOOGLE_SHEETS_CREDENTIALS", "credentials.json"),
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(os.getenv("SPREADSHEET_ID"))
        worksheet = spreadsheet.worksheet(os.getenv("WORKSHEET_NAME", "Monitoring Data"))

        # Get header row
        headers = worksheet.row_values(1)
        headers_lower = [h.strip().lower() for h in headers]

        missing = []
        for col in required_columns:
            if col.lower() not in headers_lower:
                missing.append(col)

        if missing:
            print(f"❌ Missing required columns: {missing}")
            print(f"   Found columns: {headers}")
            return False
        else:
            print(f"✅ All required columns present")
            print(f"   Columns: {headers}")
            return True

    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False


def run_all_tests():
    """Run all tests"""
    print("=" * 60)
    print("  Andariya ETL Pipeline - Setup Validation")
    print("=" * 60)

    results = {
        "Google Sheets API": test_google_sheets_connection(),
        "PostgreSQL": test_postgres_connection(),
        "Sheet Structure": validate_google_sheet_structure(),
    }

    print("\n" + "=" * 60)
    print("  Test Results Summary")
    print("=" * 60)

    all_passed = True
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {test_name:<20} {status}")
        if not passed:
            all_passed = False

    print("=" * 60)

    if all_passed:
        print("\n🎉 All tests passed! You can now run the ETL pipeline:")
        print("   python andariya_etl_pipeline.py --run-once")
    else:
        print("\n⚠️  Some tests failed. Please fix the issues above before running the pipeline.")
        sys.exit(1)


if __name__ == "__main__":
    # Load .env if exists
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    run_all_tests()