#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py
Foreclosure Sales Scraper (One-Time Full Load + Incremental Updates Thereafter)

Environment variables required:
  - SPREADSHEET_ID   (Google Sheets ID)
  - Either:
      - GOOGLE_CREDENTIALS_FILE (GitLab "File" variable path), OR
      - GOOGLE_CREDENTIALS (raw JSON string), OR
      - GOOGLE_CREDENTIALS (a path to a local JSON file)
"""

import os
import re
import sys
import json
import asyncio
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# -----------------------------
# Config
# -----------------------------
BASE_URL = "https://salesweb.civilview.com/"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

TARGET_COUNTIES = [
    {"county_id": "52", "county_name": "Cape May County, NJ"},
    {"county_id": "25", "county_name": "Atlantic County, NJ"},
    {"county_id": "1", "county_name": "Camden County, NJ"},
    {"county_id": "3", "county_name": "Burlington County, NJ"},
    {"county_id": "6", "county_name": "Cumberland County, NJ"},
    {"county_id": "19", "county_name": "Gloucester County, NJ"},
    {"county_id": "20", "county_name": "Salem County, NJ"},
    {"county_id": "15", "county_name": "Union County, NJ"},
    {"county_id": "7", "county_name": "Bergen County, NJ"},
    {"county_id": "2", "county_name": "Essex County, NJ"},
    {"county_id": "23", "county_name": "Montgomery County, PA"},
    {"county_id": "24", "county_name": "New Castle County, DE"},
]

POLITE_DELAY_SECONDS = 1.5
MAX_RETRIES = 5

# -----------------------------
# Credential helpers
# -----------------------------
def load_service_account_info():
    """
    Loads service account JSON from:
      1) GOOGLE_CREDENTIALS_FILE (File variable path) OR
      2) GOOGLE_CREDENTIALS raw JSON string OR
      3) GOOGLE_CREDENTIALS path to local file
    Returns parsed dict or raises ValueError.
    """
    file_env = os.environ.get("GOOGLE_CREDENTIALS_FILE")
    if file_env:
        if os.path.exists(file_env):
            try:
                with open(file_env, "r", encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception as e:
                raise ValueError(f"Failed to read JSON from GOOGLE_CREDENTIALS_FILE ({file_env}): {e}")
        else:
            raise ValueError(f"GOOGLE_CREDENTIALS_FILE is set but file does not exist: {file_env}")

    creds_raw = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_raw:
        raise ValueError("Environment variable GOOGLE_CREDENTIALS (or GOOGLE_CREDENTIALS_FILE) not set.")

    creds_raw_stripped = creds_raw.strip()
    # Case: raw JSON string
    if creds_raw_stripped.startswith("{"):
        try:
            return json.loads(creds_raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"GOOGLE_CREDENTIALS contains invalid JSON: {e}")

    # Case: path to file
    if os.path.exists(creds_raw):
        try:
            with open(creds_raw, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as e:
            raise ValueError(f"GOOGLE_CREDENTIALS is a path but failed to load JSON: {e}")

    raise ValueError("GOOGLE_CREDENTIALS is set but not valid JSON and not an existing file path.")

def init_sheets_service_from_env():
    info = load_service_account_info()
    try:
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        return service
    except Exception as e:
        raise RuntimeError(f"Failed to create Google Sheets client: {e}")

# -----------------------------
# Sheets client wrapper
# -----------------------------
class SheetsClient:
    def __init__(self, spreadsheet_id: str, service):
        self.spreadsheet_id = spreadsheet_id
        self.service = service
        self.svc = self.service.spreadsheets()

    def spreadsheet_info(self):
        try:
            return self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
        except HttpError as e:
            print(f"⚠ Error fetching spreadsheet info: {e}")
            return {}

    def sheet_exists(self, sheet_name: str) -> bool:
        info = self.spreadsheet_info()
        for s in info.get('sheets', []):
            if s['properties']['title'] == sheet_name:
                return True
        return False

    def create_sheet_if_missing(self, sheet_name: str):
        if self.sheet_exists(sheet_name):
            return
        try:
            req = {"addSheet": {"properties": {"title": sheet_name}}}
            self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": [req]}).execute()
            print(f"✓ Created sheet: {sheet_name}")
        except HttpError as e:
            print(f"⚠ create_sheet_if_missing error on '{sheet_name}': {e}")

    def get_values(self, sheet_name: str, rng: str = "A:Z"):
        try:
            res = self.svc.values().get(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!{rng}").execute()
            return res.get("values", [])
        except HttpError as e:
            return []

    def clear(self, sheet_name: str, rng: str = "A:Z"):
        try:
            self.svc.values().clear(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!{rng}").execute()
        except HttpError as e:
            print(f"⚠ clear error on '{sheet_name}': {e}")

    def write_values(self, sheet_name: str, values, start_cell: str = "A1"):
        try:
            self.svc.values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!{start_cell}",
                valueInputOption="USER_ENTERED",
                body={"values": values}
            ).execute()

            # --- Beautify: bold header, freeze row, auto resize ---
            self.svc.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={
                    "requests": [
                        {"repeatCell": {
                            "range": {
                                "sheetId": self._get_sheet_id(sheet_name),
                                "startRowIndex": 1,
                                "endRowIndex": 2
                            },
                            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                            "fields": "userEnteredFormat.textFormat.bold"
                        }},
                        {"updateSheetProperties": {
                            "properties": {"sheetId": self._get_sheet_id(sheet_name),
                                           "gridProperties": {"frozenRowCount": 2}},
                            "fields": "gridProperties.frozenRowCount"
                        }},
                        {"autoResizeDimensions": {
                            "dimensions": {
                                "sheetId": self._get_sheet_id(sheet_name),
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": len(values[0]) if values else 10
                            }
                        }}
                    ]
                }
            ).execute()
        except HttpError as e:
            print(f"✗ write_values error on '{sheet_name}': {e}")
            raise

    def _get_sheet_id(self, sheet_name: str):
        info = self.spreadsheet_info()
        for s in info.get('sheets', []):
            if s['properties']['title'] == sheet_name:
                return s['properties']['sheetId']
        return None

    # --- changed: only prepend *new* snapshot rows ---
    def prepend_snapshot(self, sheet_name: str, header_row, new_rows):
        if not new_rows:
            print(f"✓ No new rows to prepend in '{sheet_name}'")
            return
        snapshot_header = [[f"Snapshot for {datetime.now().strftime('%A - %Y-%m-%d')}"]]
        payload = snapshot_header + [header_row] + new_rows + [[""]]
        existing = self.get_values(sheet_name, "A:Z")
        values = payload + existing
        self.clear(sheet_name, "A:Z")
        self.write_values(sheet_name, values, "A1")
        print(f"✓ Prepended snapshot to '{sheet_name}': {len(new_rows)} new rows")

    # first run = full overwrite
    def overwrite_with_snapshot(self, sheet_name: str, header_row, all_rows):
        snapshot_header = [[f"Snapshot for {datetime.now().strftime('%A - %Y-%m-%d')}"]]
        values = snapshot_header + [header_row] + all_rows + [[""]]
        self.clear(sheet_name, "A:Z")
        self.write_values(sheet_name, values, "A1")
        print(f"✓ Wrote full snapshot to '{sheet_name}' ({len(all_rows)} rows)")

# -----------------------------
# Scrape helpers
# -----------------------------
def norm_text(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()

def extract_property_id_from_href(href: str) -> str:
    try:
        q = parse_qs(urlparse(href).query)
        return q.get("PropertyId", [""])[0]
    except Exception:
        return ""

# -----------------------------
# Scraper
# -----------------------------
class ForeclosureScraper:
    def __init__(self, sheets_client: SheetsClient):
        self.sheets_client = sheets_client

    async def goto_with_retry(self, page, url: str, max_retries=MAX_RETRIES):
        last_exc = None
        for attempt in range(max_retries):
            try:
                resp = await page.goto(url, wait_until="networkidle", timeout=60000)
                if resp and (200 <= resp.status < 300):
                    return resp
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                last_exc = e
                await asyncio.sleep(2 ** attempt)
        if last_exc:
            raise last_exc
        return None

    async def dismiss_banners(self, page):
        selectors = [
            "button:has-text('Accept')", "button:has-text('I Agree')",
            "button:has-text('Close')", "button.cookie-accept",
            "button[aria-label='Close']", ".modal-footer button:has-text('OK')",
        ]
        for sel in selectors:
            try:
                loc = page.locator(sel)
                if await loc.count():
                    await loc.first.click(timeout=1500)
                    await page.wait_for_timeout(200)
            except Exception:
                pass

    async def get_table_columns(self, page, table):
        """Get column mapping based on headers to handle different table structures."""
        try:
            # Try thead first, fallback to first row with th
            headers = table.locator("thead tr th")
            if await headers.count() == 0:
                headers = table.locator("tr").filter(has=page.locator("th")).first.locator("th")
            
            colmap = {}
            count = await headers.count()
            
            for i in range(count):
                try:
                    header_text = (await headers.nth(i).inner_text()).strip().lower()
                    header_text = re.sub(r'[:\s]+', ' ', header_text).strip()
                    
                    if 'address' in header_text:
                        colmap['address'] = i
                    elif 'defendant' in header_text:
                        colmap['defendant'] = i
                    elif 'sale' in header_text and 'date' in header_text:
                        colmap['sales_date'] = i
                    elif 'plaintiff' in header_text:
                        colmap['plaintiff'] = i
                    elif 'township' in header_text:
                        colmap['township'] = i
                        
                except Exception:
                    continue
                    
            return colmap
        except Exception as e:
            print(f"[ERROR] Failed to get column mapping: {e}")
            return {}

    async def get_cell_data(self, row, col_index):
        """Safely extract text from table cell, handling HTML content."""
        if col_index is None:
            return ""
        try:
            # Try to get HTML first for better address formatting
            html_content = await row.locator("td").nth(col_index).inner_html()
            # Clean HTML: replace <br> with spaces, remove other tags
            clean_text = re.sub(r"<br\s*/?>", " ", html_content, flags=re.I)
            clean_text = re.sub(r"<.*?>", "", clean_text)
            return re.sub(r'\s+', ' ', clean_text).strip()
        except Exception:
            try:
                # Fallback to plain text
                text = await row.locator("td").nth(col_index).inner_text()
                return re.sub(r'\s+', ' ', text).strip()
            except Exception:
                return ""

    async def get_details_data(self, page, details_url, list_url, missing_fields):
        """Extract additional data from details page if needed."""
        if not details_url or not missing_fields:
            return {}
            
        extracted = {}
        try:
            await self.goto_with_retry(page, details_url)
            await self.dismiss_banners(page)
            await page.wait_for_selector(".sale-details-list", timeout=15000)
            
            items = page.locator(".sale-details-list .sale-detail-item")
            for i in range(await items.count()):
                try:
                    label = (await items.nth(i).locator(".sale-detail-label").inner_text()).strip().lower()
                    value_el = items.nth(i).locator(".sale-detail-value")
                    value = (await value_el.inner_text()).strip()
                    
                    if 'address' in label and 'address' in missing_fields:
                        try:
                            # Try to get HTML version for better formatting
                            html_val = await value_el.inner_html()
                            html_val = re.sub(r"<br\s*/?>", " ", html_val, flags=re.I)
                            extracted['address'] = re.sub(r"<.*?>", "", html_val).strip()
                        except Exception:
                            extracted['address'] = value
                            
                    elif ('approx' in label and ('judgment' in label or 'upset' in label)) or 'debt amount' in label:
                        if 'approx_judgment' in missing_fields:
                            extracted['approx_judgment'] = value
                            
                    elif 'defendant' in label and 'defendant' in missing_fields:
                        extracted['defendant'] = value
                        
                    elif 'sale' in label and 'date' in label and 'sales_date' in missing_fields:
                        extracted['sales_date'] = value
                        
                except Exception:
                    continue
                    
        except Exception as e:
            print(f"⚠ Details page error: {e}")
        finally:
            # Return to list page
            try:
                await self.goto_with_retry(page, list_url)
                await self.dismiss_banners(page)
                await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
            except Exception:
                pass
                
        return extracted

    async def scrape_county_sales(self, page, county):
        """Main scraping function that handles different table structures dynamically."""
        url = f"{BASE_URL}Sales/SalesSearch?countyId={county['county_id']}"
        print(f"[INFO] Scraping {county['county_name']} -> {url}")
        
        for attempt in range(MAX_RETRIES):
            try:
                await self.goto_with_retry(page, url)
                await self.dismiss_banners(page)
                
                try:
                    await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
                except PlaywrightTimeoutError:
                    print(f"[WARN] No sales found for {county['county_name']}")
                    return []

                table = page.locator("table.table.table-striped").first
                
                # Get dynamic column mapping - handles Township column and other variations
                colmap = await self.get_table_columns(page, table)
                if not colmap:
                    print(f"[WARN] Could not determine table structure for {county['county_name']}")
                    return []

                rows = table.locator("tbody tr")
                results = []
                
                for i in range(await rows.count()):
                    row = rows.nth(i)
                    
                    # Get property link and ID
                    details_a = row.locator("td.hidden-print a")
                    details_href = (await details_a.get_attribute("href")) or ""
                    details_url = details_href if details_href.startswith("http") else urljoin(BASE_URL, details_href)
                    property_id = extract_property_id_from_href(details_href)
                    
                    # Extract data using dynamic column positions
                    sales_date = await self.get_cell_data(row, colmap.get('sales_date'))
                    defendant = await self.get_cell_data(row, colmap.get('defendant'))
                    prop_address = await self.get_cell_data(row, colmap.get('address'))
                    approx_judgment = ""
                    
                    # Determine what data we still need from details page
                    missing_fields = set()
                    if not prop_address:
                        missing_fields.add('address')
                    if not sales_date:
                        missing_fields.add('sales_date')
                    if not defendant:
                        missing_fields.add('defendant')
                    missing_fields.add('approx_judgment')  # Usually only in details
                    
                    # Get additional data from details if needed
                    if details_url and missing_fields:
                        details_data = await self.get_details_data(page, details_url, url, missing_fields)
                        
                        # Use better data from details if available
                        if 'address' in details_data:
                            if not prop_address or len(details_data['address']) > len(prop_address):
                                prop_address = details_data['address']
                        if 'sales_date' in details_data and not sales_date:
                            sales_date = details_data['sales_date']
                        if 'defendant' in details_data and not defendant:
                            defendant = details_data['defendant']
                        if 'approx_judgment' in details_data:
                            approx_judgment = details_data['approx_judgment']
                    
                    results.append({
                        "Property ID": property_id,
                        "Address": prop_address,
                        "Defendant": defendant,
                        "Sales Date": sales_date,
                        "Approx Judgment": approx_judgment,
                        "County": county['county_name'],
                    })
                
                return results
                
            except Exception as e:
                print(f"❌ Error scraping {county['county_name']} (Attempt {attempt+1}/{MAX_RETRIES}): {e}")
                await asyncio.sleep(2 ** attempt)
        
        print(f"[FAIL] Could not get complete data for {county['county_name']}")
        return []
# -----------------------------
# Orchestration
# -----------------------------
async def run():
    start_ts = datetime.now()
    print(f"▶ Starting scrape at {start_ts}")

    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        print("✗ SPREADSHEET_ID env var is required.")
        sys.exit(1)

    # Initialize Sheets service
    try:
        service = init_sheets_service_from_env()
        print("✓ Google Sheets API client initialized.")
    except Exception as e:
        print(f"✗ Error initializing Google Sheets client: {e}")
        raise SystemExit(1)

    sheets = SheetsClient(spreadsheet_id, service)
    ALL_DATA_SHEET = "All Data"
    first_run = not sheets.sheet_exists(ALL_DATA_SHEET)
    print(f"ℹ First run? {'YES' if first_run else 'NO'}")

    all_data_rows = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        scraper = ForeclosureScraper(sheets)

        for county in TARGET_COUNTIES:
            county_tab = county["county_name"][:30]
            try:
                county_records = await scraper.scrape_county_sales(page, county)
                if not county_records:
                    print(f"⚠ No data for {county['county_name']}")
                    await asyncio.sleep(POLITE_DELAY_SECONDS)
                    continue

                df_county = pd.DataFrame(county_records, columns=["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"])

                if first_run or not sheets.sheet_exists(county_tab):
                    sheets.create_sheet_if_missing(county_tab)
                    header = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment"]
                    rows = df_county.drop(columns=["County"]).astype(str).values.tolist()
                    sheets.overwrite_with_snapshot(county_tab, header, rows)
                else:
                    existing = sheets.get_values(county_tab, "A:Z")
                    existing_ids = set()
                    if existing:
                        header_idx = None
                        for idx, row in enumerate(existing[:5]):
                            if row and row[0].lower().replace(" ", "") in {"propertyid", "property id"}:
                                header_idx = idx
                                break
                        if header_idx is None:
                            header_idx = 1 if len(existing) > 1 else 0
                        for r in existing[header_idx + 1:]:
                            if not r or (len(r) == 1 and r[0].strip() == ""):
                                continue
                            pid = (r[0] or "").strip()
                            if pid:
                                existing_ids.add(pid)
                    new_df = df_county[~df_county["Property ID"].isin(existing_ids)].copy()
                    if new_df.empty:
                        print(f"✓ No new rows for {county['county_name']}")
                    else:
                        header = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment"]
                        new_rows = new_df.drop(columns=["County"]).astype(str).values.tolist()
                        sheets.prepend_snapshot(county_tab, header, new_rows)

                all_data_rows.extend(df_county.astype(str).values.tolist())
                print(f"✓ Completed {county['county_name']}: {len(df_county)} records")
                await asyncio.sleep(POLITE_DELAY_SECONDS)
            except Exception as e:
                print(f"❌ Failed county '{county['county_name']}': {e}")
                continue

        await browser.close()

    # Update All Data sheet
    try:
        header_all = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"]
        if not all_data_rows:
            print("⚠ No data scraped across all counties. Skipping 'All Data'.")
        else:
            sheets.create_sheet_if_missing(ALL_DATA_SHEET)
            if first_run:
                sheets.overwrite_with_snapshot(ALL_DATA_SHEET, header_all, all_data_rows)
            else:
                existing = sheets.get_values(ALL_DATA_SHEET, "A:Z")
                existing_pairs = set()
                if existing:
                    header_idx = None
                    for idx, row in enumerate(existing[:5]):
                        if row and row[0].lower().replace(" ", "") in {"propertyid", "property id"}:
                            header_idx = idx
                            break
                    if header_idx is None:
                        header_idx = 1 if len(existing) > 1 else 0
                    for r in existing[header_idx + 1:]:
                        if not r or (len(r) == 1 and r[0].strip() == ""):
                            continue
                        pid = (r[0] if len(r) > 0 else "").strip()
                        cty = (r[5] if len(r) > 5 else "").strip()
                        if pid and cty:
                            existing_pairs.add((cty, pid))

                new_rows = []
                for r in all_data_rows:
                    pid = (r[0] if len(r) > 0 else "").strip()
                    cty = (r[5] if len(r) > 5 else "").strip()
                    if pid and cty and (cty, pid) not in existing_pairs:
                        new_rows.append(r)

                if not new_rows:
                    print("✓ No new rows for 'All Data'")
                else:
                    sheets.prepend_snapshot(ALL_DATA_SHEET, header_all, new_rows)
                    print(f"✓ All Data updated: {len(new_rows)} new rows")
    except Exception as e:
        print(f"✗ Error updating 'All Data': {e}")

    print("■ Finished at", datetime.now())

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except Exception as e:
        print("Fatal error:", e)
        sys.exit(1)







# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# main.py
# Foreclosure Sales Scraper (One-Time Full Load + Incremental Updates Thereafter)

# Environment variables required:
#   - SPREADSHEET_ID   (Google Sheets ID)
#   - Either:
#       - GOOGLE_CREDENTIALS_FILE (GitLab "File" variable path), OR
#       - GOOGLE_CREDENTIALS (raw JSON string), OR
#       - GOOGLE_CREDENTIALS (a path to a local JSON file)
# """

# import os
# import re
# import sys
# import json
# import asyncio
# import pandas as pd
# from datetime import datetime
# from urllib.parse import urljoin, urlparse, parse_qs

# from google.oauth2 import service_account
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError

# from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# # -----------------------------
# # Config
# # -----------------------------
# BASE_URL = "https://salesweb.civilview.com/"
# SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# TARGET_COUNTIES = [
#     {"county_id": "52", "county_name": "Cape May County, NJ"},
#     {"county_id": "25", "county_name": "Atlantic County, NJ"},
#     {"county_id": "1", "county_name": "Camden County, NJ"},
#     {"county_id": "3", "county_name": "Burlington County, NJ"},
#     {"county_id": "6", "county_name": "Cumberland County, NJ"},
#     {"county_id": "19", "county_name": "Gloucester County, NJ"},
#     {"county_id": "20", "county_name": "Salem County, NJ"},
#     {"county_id": "15", "county_name": "Union County, NJ"}
# ]

# POLITE_DELAY_SECONDS = 1.5
# MAX_RETRIES = 3

# # -----------------------------
# # Credential helpers
# # -----------------------------
# def load_service_account_info():
#     file_env = os.environ.get("GOOGLE_CREDENTIALS_FILE")
#     if file_env:
#         if os.path.exists(file_env):
#             try:
#                 with open(file_env, "r", encoding="utf-8") as fh:
#                     return json.load(fh)
#             except Exception as e:
#                 raise ValueError(f"Failed to read JSON from GOOGLE_CREDENTIALS_FILE ({file_env}): {e}")
#         else:
#             raise ValueError(f"GOOGLE_CREDENTIALS_FILE is set but file does not exist: {file_env}")

#     creds_raw = os.environ.get("GOOGLE_CREDENTIALS")
#     if not creds_raw:
#         raise ValueError("Environment variable GOOGLE_CREDENTIALS (or GOOGLE_CREDENTIALS_FILE) not set.")

#     creds_raw_stripped = creds_raw.strip()
#     if creds_raw_stripped.startswith("{"):
#         try:
#             return json.loads(creds_raw)
#         except json.JSONDecodeError as e:
#             raise ValueError(f"GOOGLE_CREDENTIALS contains invalid JSON: {e}")

#     if os.path.exists(creds_raw):
#         try:
#             with open(creds_raw, "r", encoding="utf-8") as fh:
#                 return json.load(fh)
#         except Exception as e:
#             raise ValueError(f"GOOGLE_CREDENTIALS is a path but failed to load JSON: {e}")

#     raise ValueError("GOOGLE_CREDENTIALS is set but not valid JSON and not an existing file path.")

# def init_sheets_service_from_env():
#     info = load_service_account_info()
#     try:
#         creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
#         service = build('sheets', 'v4', credentials=creds)
#         return service
#     except Exception as e:
#         raise RuntimeError(f"Failed to create Google Sheets client: {e}")

# # -----------------------------
# # Sheets client wrapper
# # -----------------------------
# class SheetsClient:
#     def __init__(self, spreadsheet_id: str, service):
#         self.spreadsheet_id = spreadsheet_id
#         self.service = service
#         self.svc = self.service.spreadsheets()

#     def spreadsheet_info(self):
#         try:
#             return self.svc.get(spreadsheetId=self.spreadsheet_id).execute()
#         except HttpError as e:
#             print(f"⚠ Error fetching spreadsheet info: {e}")
#             return {}

#     def sheet_exists(self, sheet_name: str) -> bool:
#         info = self.spreadsheet_info()
#         for s in info.get('sheets', []):
#             if s['properties']['title'] == sheet_name:
#                 return True
#         return False

#     def create_sheet_if_missing(self, sheet_name: str):
#         if self.sheet_exists(sheet_name):
#             return
#         try:
#             req = {"addSheet": {"properties": {"title": sheet_name}}}
#             self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": [req]}).execute()
#             print(f"✓ Created sheet: {sheet_name}")
#         except HttpError as e:
#             print(f"⚠ create_sheet_if_missing error on '{sheet_name}': {e}")

#     def get_values(self, sheet_name: str, rng: str = "A:Z"):
#         try:
#             res = self.svc.values().get(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!{rng}").execute()
#             return res.get("values", [])
#         except HttpError:
#             return []

#     def clear(self, sheet_name: str, rng: str = "A:Z"):
#         try:
#             self.svc.values().clear(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!{rng}").execute()
#         except HttpError as e:
#             print(f"⚠ clear error on '{sheet_name}': {e}")

#     def append_value_row(self, sheet_name: str, row):
#         try:
#             self.svc.values().append(
#                 spreadsheetId=self.spreadsheet_id,
#                 range=f"'{sheet_name}'!A1",
#                 valueInputOption="USER_ENTERED",
#                 insertDataOption="INSERT_ROWS",
#                 body={"values": [row]}
#             ).execute()
#         except HttpError as e:
#             print(f"⚠ append_value_row error on '{sheet_name}': {e}")

#     def _get_sheet_id(self, sheet_name: str):
#         info = self.spreadsheet_info()
#         for s in info.get('sheets', []):
#             if s['properties']['title'] == sheet_name:
#                 return s['properties']['sheetId']
#         return None

#     # --- styling helpers (kept from your version) ---
#     def _apply_styling_for_block(self, sheet_name: str, start_row_idx: int, header_labels_len: int, data_rows_len: int):
#         sheet_id = self._get_sheet_id(sheet_name)
#         if sheet_id is None:
#             return

#         snapshot_title_row = start_row_idx
#         header_row = start_row_idx + 1
#         data_start_row = start_row_idx + 2
#         data_end_row = data_start_row + max(data_rows_len, 0)
#         has_data = data_rows_len > 0

#         requests = []

#         requests.append({
#             "mergeCells": {
#                 "range": {
#                     "sheetId": sheet_id,
#                     "startRowIndex": snapshot_title_row,
#                     "endRowIndex": snapshot_title_row + 1,
#                     "startColumnIndex": 0,
#                     "endColumnIndex": max(header_labels_len, 1)
#                 },
#                 "mergeType": "MERGE_ALL"
#             }
#         })

#         requests.append({
#             "repeatCell": {
#                 "range": {
#                     "sheetId": sheet_id,
#                     "startRowIndex": snapshot_title_row,
#                     "endRowIndex": snapshot_title_row + 1,
#                     "startColumnIndex": 0,
#                     "endColumnIndex": max(header_labels_len, 1)
#                 },
#                 "cell": {
#                     "userEnteredFormat": {
#                         "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 0.98},
#                         "horizontalAlignment": "LEFT",
#                         "textFormat": {"bold": True, "fontSize": 12}
#                     }
#                 },
#                 "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
#             }
#         })

#         requests.append({
#             "repeatCell": {
#                 "range": {
#                     "sheetId": sheet_id,
#                     "startRowIndex": header_row,
#                     "endRowIndex": header_row + 1,
#                     "startColumnIndex": 0,
#                     "endColumnIndex": max(header_labels_len, 1)
#                 },
#                 "cell": {
#                     "userEnteredFormat": {
#                         "textFormat": {"bold": True},
#                         "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}
#                     }
#                 },
#                 "fields": "userEnteredFormat(textFormat,backgroundColor)"
#             }
#         })

#         requests.append({
#             "updateSheetProperties": {
#                 "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 2}},
#                 "fields": "gridProperties.frozenRowCount"
#             }
#         })

#         if has_data:
#             requests.append({
#                 "addBanding": {
#                     "bandedRange": {
#                         "range": {
#                             "sheetId": sheet_id,
#                             "startRowIndex": data_start_row,
#                             "endRowIndex": data_end_row,
#                             "startColumnIndex": 0,
#                             "endColumnIndex": max(header_labels_len, 1)
#                         },
#                         "rowProperties": {
#                             "headerColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
#                             "firstBandColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
#                             "secondBandColor": {"red": 0.98, "green": 0.98, "blue": 0.98}
#                         }
#                     }
#                 }
#             })

#         requests.append({
#             "autoResizeDimensions": {
#                 "dimensions": {
#                     "sheetId": sheet_id,
#                     "dimension": "COLUMNS",
#                     "startIndex": 0,
#                     "endIndex": max(header_labels_len, 1)
#                 }
#             }
#         })

#         try:
#             self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": requests}).execute()
#         except HttpError as e:
#             print(f"⚠ styling error on '{sheet_name}': {e}")

#     def _insert_rows_top(self, sheet_name: str, num_rows: int):
#         sheet_id = self._get_sheet_id(sheet_name)
#         if sheet_id is None or num_rows <= 0:
#             return
#         try:
#             self.svc.batchUpdate(
#                 spreadsheetId=self.spreadsheet_id,
#                 body={
#                     "requests": [
#                         {
#                             "insertDimension": {
#                                 "range": {
#                                     "sheetId": sheet_id,
#                                     "dimension": "ROWS",
#                                     "startIndex": 0,
#                                     "endIndex": num_rows
#                                 },
#                                 "inheritFromBefore": False
#                             }
#                         }
#                     ]
#                 }
#             ).execute()
#         except HttpError as e:
#             print(f"⚠ insert rows error on '{sheet_name}': {e}")

#     def write_values_at(self, sheet_name: str, start_cell: str, values):
#         try:
#             self.svc.values().update(
#                 spreadsheetId=self.spreadsheet_id,
#                 range=f"'{sheet_name}'!{start_cell}",
#                 valueInputOption="USER_ENTERED",
#                 body={"values": values}
#             ).execute()
#         except HttpError as e:
#             print(f"✗ write_values_at error on '{sheet_name}': {e}")
#             raise

#     def write_snapshot_block(self, sheet_name: str, header_row, data_rows, note_if_empty=True):
#         has_data = bool(data_rows)
#         title = f"Snapshot for {datetime.now().strftime('%A - %Y-%m-%d')}"
#         if not has_data and note_if_empty:
#             title += " — No new data available"

#         payload = []
#         payload.append([title])
#         header_len = len(header_row) if header_row else 1
#         if has_data:
#             payload.append(header_row)
#             payload.extend(data_rows)
#         payload.append([""])

#         rows_to_insert = len(payload)
#         self._insert_rows_top(sheet_name, rows_to_insert)
#         self.write_values_at(sheet_name, "A1", payload)
#         data_rows_len = len(data_rows) if has_data else 0
#         header_labels_len = header_len if has_data else max(header_len, 1)
#         self._apply_styling_for_block(sheet_name, start_row_idx=0, header_labels_len=header_labels_len, data_rows_len=data_rows_len)
#         print(f"✓ Prepended snapshot to '{sheet_name}': {len(data_rows)} new rows" if has_data else f"✓ Prepended snapshot to '{sheet_name}': No new data available")

#     def overwrite_with_snapshot(self, sheet_name: str, header_row, all_rows):
#         self.clear(sheet_name, "A:Z")
#         # Create a single snapshot block (title + header + data + blank)
#         title = f"Snapshot for {datetime.now().strftime('%A - %Y-%m-%d')}"
#         payload = [[title]]
#         if all_rows:
#             payload.append(header_row)
#             payload.extend(all_rows)
#         payload.append([""])
#         self.write_values_at(sheet_name, "A1", payload)
#         data_rows_len = len(all_rows) if all_rows else 0
#         header_labels_len = len(header_row) if all_rows else max(len(header_row), 1)
#         self._apply_styling_for_block(sheet_name, start_row_idx=0, header_labels_len=header_labels_len, data_rows_len=data_rows_len)
#         print(f"✓ Wrote full snapshot to '{sheet_name}' ({len(all_rows)} rows)")

#     # ---- initialization registry helpers (_INIT) ----
#     def _init_sheet_name(self):
#         return "_INIT"

#     def get_initialized_counties(self):
#         name = self._init_sheet_name()
#         if not self.sheet_exists(name):
#             # create init sheet so it exists
#             self.create_sheet_if_missing(name)
#             return set()
#         vals = self.get_values(name, "A:A")
#         return set([r[0] for r in vals if r and r[0].strip()])

#     def add_initialized_county(self, county_sheet_name: str):
#         inits = self.get_initialized_counties()
#         if county_sheet_name in inits:
#             return
#         self.create_sheet_if_missing(self._init_sheet_name())
#         self.append_value_row(self._init_sheet_name(), [county_sheet_name])

# # -----------------------------
# # Scrape helpers
# # -----------------------------
# def norm_text(s: str) -> str:
#     if not s:
#         return ""
#     return re.sub(r"\s+", " ", s).strip()

# def extract_property_id_from_href(href: str) -> str:
#     try:
#         q = parse_qs(urlparse(href).query)
#         return q.get("PropertyId", [""])[0]
#     except Exception:
#         return ""

# # -----------------------------
# # Scraper
# # -----------------------------
# class ForeclosureScraper:
#     def __init__(self, sheets_client: SheetsClient):
#         self.sheets_client = sheets_client

#     async def goto_with_retry(self, page, url: str, max_retries=MAX_RETRIES):
#         last_exc = None
#         for attempt in range(max_retries):
#             try:
#                 resp = await page.goto(url, wait_until="networkidle", timeout=60000)
#                 if resp and (200 <= resp.status < 300):
#                     return resp
#                 await asyncio.sleep(2 ** attempt)
#             except Exception as e:
#                 last_exc = e
#                 await asyncio.sleep(2 ** attempt)
#         if last_exc:
#             raise last_exc
#         return None

#     async def dismiss_banners(self, page):
#         selectors = [
#             "button:has-text('Accept')", "button:has-text('I Agree')",
#             "button:has-text('Close')", "button.cookie-accept",
#             "button[aria-label='Close']", ".modal-footer button:has-text('OK')",
#         ]
#         for sel in selectors:
#             try:
#                 loc = page.locator(sel)
#                 if await loc.count():
#                     await loc.first.click(timeout=1500)
#                     await page.wait_for_timeout(200)
#             except Exception:
#                 pass

#     async def scrape_county_sales(self, page, county):
#         url = f"{BASE_URL}Sales/SalesSearch?countyId={county['county_id']}"
#         print(f"[INFO] Scraping {county['county_name']} -> {url}")
#         for attempt in range(MAX_RETRIES):
#             try:
#                 await self.goto_with_retry(page, url)
#                 await self.dismiss_banners(page)
#                 try:
#                     await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
#                 except PlaywrightTimeoutError:
#                     print(f"[WARN] No sales found for {county['county_name']}")
#                     return []

#                 rows = page.locator("table.table.table-striped tbody tr")
#                 n = await rows.count()
#                 results = []
#                 for i in range(n):
#                     row = rows.nth(i)
#                     details_a = row.locator("td.hidden-print a")
#                     details_href = (await details_a.get_attribute("href")) or ""
#                     details_url = details_href if details_href.startswith("http") else urljoin(BASE_URL, details_href)
#                     property_id = extract_property_id_from_href(details_href)

#                     try:
#                         sales_date = norm_text(await row.locator("td").nth(2).inner_text())
#                     except Exception:
#                         sales_date = ""
#                     try:
#                         defendant = norm_text(await row.locator("td").nth(4).inner_text())
#                     except Exception:
#                         defendant = ""
#                     try:
#                         tds = row.locator("td")
#                         td_count = await tds.count()
#                         if td_count >= 6:
#                             prop_address = norm_text(await tds.nth(5).inner_text())
#                         else:
#                             prop_address = ""
#                     except Exception:
#                         prop_address = ""

#                     approx_judgment = ""
#                     if details_url:
#                         try:
#                             await self.goto_with_retry(page, details_url)
#                             await self.dismiss_banners(page)
#                             await page.wait_for_selector(".sale-details-list", timeout=15000)
#                             items = page.locator(".sale-details-list .sale-detail-item")
#                             for j in range(await items.count()):
#                                 label = norm_text(await items.nth(j).locator(".sale-detail-label").inner_text())
#                                 val = norm_text(await items.nth(j).locator(".sale-detail-value").inner_text())
#                                 if ("Address" in label or "Property Address" in label):
#                                     try:
#                                         val_html = await items.nth(j).locator(".sale-detail-value").inner_html()
#                                         val_html = re.sub(r"<br\s*/?>", " ", val_html)
#                                         val_clean = re.sub(r"<.*?>", "", val_html).strip()
#                                         details_address = norm_text(val_clean)
#                                         if not prop_address or len(details_address) > len(prop_address):
#                                             prop_address = details_address
#                                     except Exception:
#                                         if not prop_address:
#                                             prop_address = norm_text(val)
#                                 elif ("Approx. Judgment" in label or "Approx. Upset" in label
#                                       or "Approximate Judgment:" in label or "Approx Judgment*" in label):
#                                     approx_judgment = val
#                                 elif "Defendant" in label and not defendant:
#                                     defendant = val
#                                 elif "Sale Date" in label and not sales_date:
#                                     sales_date = val
#                         except Exception as e:
#                             print(f"⚠ Details page error for {county['county_name']} (PropertyId={property_id}): {e}")
#                         finally:
#                             try:
#                                 await self.goto_with_retry(page, url)
#                                 await self.dismiss_banners(page)
#                                 await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
#                             except Exception:
#                                 pass

#                     results.append({
#                         "Property ID": property_id,
#                         "Address": prop_address,
#                         "Defendant": defendant,
#                         "Sales Date": sales_date,
#                         "Approx Judgment": approx_judgment,
#                         "County": county['county_name'],
#                     })
#                 return results
#             except Exception as e:
#                 print(f" Error scraping {county['county_name']} (Attempt {attempt+1}/{MAX_RETRIES}): {e}")
#                 await asyncio.sleep(2 ** attempt)
#         print(f"[FAIL] Could not get complete data for {county['county_name']}")
#         return []

# # -----------------------------
# # Orchestration
# # -----------------------------
# async def run():
#     start_ts = datetime.now()
#     print(f"▶ Starting scrape at {start_ts}")

#     spreadsheet_id = os.environ.get("SPREADSHEET_ID")
#     if not spreadsheet_id:
#         print("✗ SPREADSHEET_ID env var is required.")
#         sys.exit(1)

#     try:
#         service = init_sheets_service_from_env()
#         print("✓ Google Sheets API client initialized.")
#     except Exception as e:
#         print(f"✗ Error initializing Google Sheets client: {e}")
#         raise SystemExit(1)

#     sheets = SheetsClient(spreadsheet_id, service)
#     ALL_DATA_SHEET = "All Data"
#     first_run = not sheets.sheet_exists(ALL_DATA_SHEET)
#     print(f"ℹ First run? {'YES' if first_run else 'NO'}")

#     # read initialized counties from _INIT (ensures each county gets full dataset exactly once)
#     initialized_counties = sheets.get_initialized_counties()

#     all_data_rows = []

#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=True)
#         page = await browser.new_page()
#         scraper = ForeclosureScraper(sheets)

#         for county in TARGET_COUNTIES:
#             county_tab = county["county_name"][:30]
#             try:
#                 county_records = await scraper.scrape_county_sales(page, county)
#                 if not county_records:
#                     print(f"⚠ No data for {county['county_name']}")
#                     await asyncio.sleep(POLITE_DELAY_SECONDS)
#                     continue

#                 df_county = pd.DataFrame(county_records, columns=["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"])

#                 # Ensure sheet exists
#                 sheets.create_sheet_if_missing(county_tab)
#                 header = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment"]
#                 rows_county_all = df_county.drop(columns=["County"]).astype(str).values.tolist()

#                 # If county never initialized (not present in _INIT), write full dataset once and mark it.
#                 if county_tab not in initialized_counties:
#                     sheets.overwrite_with_snapshot(county_tab, header, rows_county_all)
#                     sheets.add_initialized_county(county_tab)
#                     initialized_counties.add(county_tab)
#                 else:
#                     # Subsequent runs: compute new rows based on existing property IDs and prepend snapshot
#                     existing = sheets.get_values(county_tab, "A:Z")
#                     existing_ids = set()
#                     if existing:
#                         header_idx = None
#                         for idx, row in enumerate(existing[:10]):
#                             if row and (row[0].lower().replace(" ", "") in {"propertyid", "property id"}):
#                                 header_idx = idx
#                                 break
#                         if header_idx is None:
#                             header_idx = 1 if len(existing) > 1 else 0
#                         for r in existing[header_idx + 1:]:
#                             if not r or (len(r) == 1 and r[0].strip() == ""):
#                                 continue
#                             pid = (r[0] or "").strip()
#                             if pid:
#                                 existing_ids.add(pid)

#                     new_df = df_county[~df_county["Property ID"].isin(existing_ids)].copy()
#                     if new_df.empty:
#                         sheets.write_snapshot_block(county_tab, header_row=header, data_rows=[], note_if_empty=True)
#                         print(f"✓ No new rows for {county['county_name']} (snapshot added 'No new data available').")
#                     else:
#                         new_rows = new_df.drop(columns=["County"]).astype(str).values.tolist()
#                         sheets.write_snapshot_block(county_tab, header_row=header, data_rows=new_rows, note_if_empty=True)

#                 all_data_rows.extend(df_county.astype(str).values.tolist())
#                 print(f"✓ Completed {county['county_name']}: {len(df_county)} records")
#                 await asyncio.sleep(POLITE_DELAY_SECONDS)
#             except Exception as e:
#                 print(f" Failed county '{county['county_name']}': {e}")
#                 continue

#         await browser.close()

#     # Update All Data sheet: first run = full dataset; subsequent runs = OVERWRITE with only new rows
#     try:
#         header_all = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"]
#         sheets.create_sheet_if_missing(ALL_DATA_SHEET)

#         if not all_data_rows:
#             if first_run:
#                 sheets.overwrite_with_snapshot(ALL_DATA_SHEET, header_all, [])
#             else:
#                 # overwrite with a single-note snapshot
#                 sheets.overwrite_with_snapshot(ALL_DATA_SHEET, header_all, [["NO NEW DATA AVAILABLE"]])
#             print("⚠ No data scraped across all counties.")
#         else:
#             if first_run:
#                 # first run: full snapshot with everything
#                 sheets.overwrite_with_snapshot(ALL_DATA_SHEET, header_all, all_data_rows)
#             else:
#                 # subsequent runs: compare to existing All Data and collect only truly new rows (county,property)
#                 existing = sheets.get_values(ALL_DATA_SHEET, "A:Z")
#                 existing_pairs = set()
#                 if existing:
#                     header_idx = None
#                     for idx, row in enumerate(existing[:10]):
#                         if row and row[0].lower().replace(" ", "") in {"propertyid", "property id"}:
#                             header_idx = idx
#                             break
#                     if header_idx is None:
#                         header_idx = 1 if len(existing) > 1 else 0
#                     for r in existing[header_idx + 1:]:
#                         if not r or (len(r) == 1 and r[0].strip() == ""):
#                             continue
#                         pid = (r[0] if len(r) > 0 else "").strip()
#                         cty = (r[5] if len(r) > 5 else "").strip()
#                         if pid and cty:
#                             existing_pairs.add((cty, pid))

#                 new_rows_for_all = []
#                 for r in all_data_rows:
#                     pid = (r[0] if len(r) > 0 else "").strip()
#                     cty = (r[5] if len(r) > 5 else "").strip()
#                     if pid and cty and (cty, pid) not in existing_pairs:
#                         new_rows_for_all.append(r)

#                 if not new_rows_for_all:
#                     sheets.overwrite_with_snapshot(ALL_DATA_SHEET, header_all, [["NO NEW DATA AVAILABLE"]])
#                     print("✓ 'All Data' updated: NO NEW DATA AVAILABLE")
#                 else:
#                     # overwrite All Data with only this run's new rows
#                     sheets.overwrite_with_snapshot(ALL_DATA_SHEET, header_all, new_rows_for_all)
#                     print(f"✓ All Data updated: {len(new_rows_for_all)} new rows (All Data now contains only this run's new rows).")
#     except Exception as e:
#         print(f"✗ Error updating 'All Data': {e}")

#     print("■ Finished at", datetime.now())

# if __name__ == "__main__":
#     try:
#         asyncio.run(run())
#     except Exception as e:
#         print("Fatal error:", e)
#         sys.exit(1)
