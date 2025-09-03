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
from typing import Dict, List, Any, Optional

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
def load_service_account_info() -> Dict[str, Any]:
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
        service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
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

    def spreadsheet_info(self) -> Dict[str, Any]:
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

    def get_values(self, sheet_name: str, rng: str = "A:Z") -> List[List[str]]:
        try:
            res = self.svc.values().get(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!{rng}").execute()
            return res.get("values", [])
        except HttpError:
            return []

    def clear(self, sheet_name: str, rng: str = "A:Z"):
        try:
            self.svc.values().clear(spreadsheetId=self.spreadsheet_id, range=f"'{sheet_name}'!{rng}").execute()
        except HttpError as e:
            print(f"⚠ clear error on '{sheet_name}': {e}")

    def write_values(self, sheet_name: str, values: List[List[Any]], start_cell: str = "A1"):
        try:
            self.svc.values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!{start_cell}",
                valueInputOption="USER_ENTERED",
                body={"values": values}
            ).execute()

            sheet_id = self._get_sheet_id(sheet_name)
            if sheet_id is None:
                return

            # Beautify: bold header row (row 2), freeze first two rows, auto-resize
            requests = [
                {"repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": 2
                    },
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold"
                }},
                {"updateSheetProperties": {
                    "properties": {"sheetId": sheet_id,
                                   "gridProperties": {"frozenRowCount": 2}},
                    "fields": "gridProperties.frozenRowCount"
                }},
                {"autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": len(values[1]) if len(values) > 1 else 10
                    }
                }}
            ]
            self.svc.batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": requests}).execute()
        except HttpError as e:
            print(f"✗ write_values error on '{sheet_name}': {e}")
            raise

    def _get_sheet_id(self, sheet_name: str) -> Optional[int]:
        info = self.spreadsheet_info()
        for s in info.get('sheets', []):
            if s['properties']['title'] == sheet_name:
                return s['properties']['sheetId']
        return None

    def prepend_snapshot(self, sheet_name: str, header_row: List[str], new_rows: List[List[Any]]):
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

    def overwrite_with_snapshot(self, sheet_name: str, header_row: List[str], all_rows: List[List[Any]]):
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

    async def get_table_columns(self, page) -> Dict[str, int]:
        try:
            header_ths = page.locator("table.table.table-striped thead tr th")
            if await header_ths.count() == 0:
                header_ths = page.locator("table.table.table-striped tr").first.locator("th")

            colmap: Dict[str, int] = {}
            count = await header_ths.count()
            for i in range(count):
                try:
                    htxt = (await header_ths.nth(i).inner_text()).strip().lower()
                    if "sale" in htxt and "date" in htxt:
                        colmap["sales_date"] = i
                    elif "defendant" in htxt:
                        colmap["defendant"] = i
                    elif "address" in htxt:
                        colmap["address"] = i
                except Exception:
                    continue
            return colmap
        except Exception as e:
            print(f"[ERROR] Failed to get column mapping: {e}")
            return {}

    async def safe_get_cell_text(self, row, colmap: Dict[str, int], colname: str) -> str:
        """Safely extract text from table cell by column name."""
        try:
            idx = colmap.get(colname)
            if idx is None:
                return ""
            cell = row.locator("td").nth(idx)
            # Ensure the cell exists
            if await cell.count() == 0:
                return ""
            txt = await cell.inner_text()
            return re.sub(r"\s+", " ", txt).strip()
        except Exception:
            return ""

    async def get_details_data(self, page, details_url: str, list_url: str, county: Dict[str, str], current_data: Dict[str, str]) -> Dict[str, str]:
        extracted = {
            "approx_judgment": "",
            "sale_type": "",
            "address": current_data.get("address", ""),
            "defendant": current_data.get("defendant", ""),
            "sales_date": current_data.get("sales_date", "")
        }

        if not details_url:
            return extracted

        try:
            await self.goto_with_retry(page, details_url)
            await self.dismiss_banners(page)
            await page.wait_for_selector(".sale-details-list, .sale-detail-item", timeout=15000)

            items = page.locator(".sale-details-list .sale-detail-item")
            # Fallback if container class differs
            if await items.count() == 0:
                items = page.locator(".sale-detail-item")

            count = await items.count()
            for j in range(count):
                try:
                    label_loc = items.nth(j).locator(".sale-detail-label")
                    value_loc = items.nth(j).locator(".sale-detail-value")
                    label = (await label_loc.inner_text()).strip() if await label_loc.count() else ""
                    val = (await value_loc.inner_text()).strip() if await value_loc.count() else ""
                    label_low = label.lower()

                    if "address" in label_low:
                        try:
                            val_html = await value_loc.inner_html()
                            val_html = re.sub(r"<br\s*/?>", " ", val_html, flags=re.I)
                            val_clean = re.sub(r"<.*?>", "", val_html).strip()
                            if not extracted["address"] or len(val_clean) > len(extracted["address"]):
                                extracted["address"] = val_clean
                        except Exception:
                            if not extracted["address"]:
                                extracted["address"] = val

                    elif ("Approx. Judgment" in label or "Approx. Upset" in label
                        or "Approximate Judgment:" in label or "Approx Judgment*" in label 
                        or "Approx. Upset*" in label or "Debt Amount" in label):
                        extracted["approx_judgment"] = val

                    elif "defendant" in label_low and not extracted["defendant"]:
                        extracted["defendant"] = val

                    elif "sale" in label_low and "date" in label_low and not extracted["sales_date"]:
                        extracted["sales_date"] = val

                    elif county["county_id"] == "24" and "sale type" in label_low:
                        extracted["sale_type"] = val

                except Exception:
                    continue

        except Exception as e:
            print(f"⚠ Details page error for {county['county_name']}: {e}")
        finally:
            try:
                await self.goto_with_retry(page, list_url)
                await self.dismiss_banners(page)
                await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
            except Exception:
                pass

        return extracted

    async def scrape_county_sales(self, page, county: Dict[str, str]) -> List[Dict[str, str]]:
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

                colmap = await self.get_table_columns(page)
                if not colmap:
                    print(f"[WARN] Could not determine table structure for {county['county_name']}")
                    return []

                rows = page.locator("table.table.table-striped tbody tr")
                n = await rows.count()
                results: List[Dict[str, str]] = []

                for i in range(n):
                    row = rows.nth(i)

                    # Get details link safely
                    details_href = ""
                    try:
                        hidden_print_td = row.locator("td.hidden-print")
                        if await hidden_print_td.count():
                            a = hidden_print_td.locator("a")
                            if await a.count():
                                details_href = await a.first.get_attribute("href") or ""
                    except Exception:
                        details_href = ""

                    details_url = ""
                    if details_href:
                        details_url = details_href if details_href.startswith("http") else urljoin(BASE_URL, details_href)
                    property_id = extract_property_id_from_href(details_href)

                    sales_date = await self.safe_get_cell_text(row, colmap, "sales_date")
                    defendant = await self.safe_get_cell_text(row, colmap, "defendant")
                    prop_address = await self.safe_get_cell_text(row, colmap, "address")

                    current_data = {"address": prop_address, "defendant": defendant, "sales_date": sales_date}
                    details_data = await self.get_details_data(page, details_url, url, county, current_data)

                    row_data: Dict[str, str] = {
                        "Property ID": property_id,
                        "Address": details_data["address"],
                        "Defendant": details_data["defendant"],
                        "Sales Date": details_data["sales_date"],
                        "Approx Judgment": details_data["approx_judgment"],
                        "County": county['county_name'],
                    }
                    if county["county_id"] == "24":
                        row_data["Sale Type"] = details_data["sale_type"]

                    results.append({k: norm_text(v) for k, v in row_data.items()})

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

    all_data_rows: List[List[str]] = []
    all_data_headers_seen: Optional[List[str]] = None  # Will be set later

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

                df_county = pd.DataFrame(county_records)

                # Dynamic headers based on actual columns present
                county_columns = [col for col in df_county.columns if col != "County"]
                county_header = county_columns  # includes "Sale Type" if present
                print(f"[INFO] {county['county_name']} columns: {county_header}")

                # Prepare rows for the sheet (exclude County column in per-county sheet)
                rows = df_county.drop(columns=["County"]).astype(str).values.tolist()

                if first_run or not sheets.sheet_exists(county_tab):
                    sheets.create_sheet_if_missing(county_tab)
                    sheets.overwrite_with_snapshot(county_tab, county_header, rows)
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
                        new_rows = new_df.drop(columns=["County"]).astype(str).values.tolist()

                        # Check if sheet structure needs updating for new columns
                        existing_header = existing[1] if len(existing) > 1 else (existing[0] if existing else [])
                        if set(county_header) != set(existing_header or []):
                            print(f"[INFO] Updating {county_tab} sheet structure for new columns")
                            # Gather all existing data (skip snapshot separator rows)
                            all_existing_data = []
                            if existing:
                                # Try to locate header row similarly
                                header_idx = None
                                for idx, row in enumerate(existing[:5]):
                                    if row and row[0].lower().replace(" ", "") in {"propertyid", "property id"}:
                                        header_idx = idx
                                        break
                                if header_idx is None:
                                    header_idx = 1 if len(existing) > 1 else 0
                                for r in existing[header_idx + 1:]:
                                    if r and not (len(r) == 1 and r[0].strip() == ""):
                                        all_existing_data.append(r)
                            combined_data = all_existing_data + new_rows
                            sheets.overwrite_with_snapshot(county_tab, county_header, combined_data)
                        else:
                            sheets.prepend_snapshot(county_tab, county_header, new_rows)

                # Keep rows for All Data
                all_data_rows.extend(df_county.astype(str).values.tolist())
                print(f"✓ Completed {county['county_name']}: {len(df_county)} records")
                await asyncio.sleep(POLITE_DELAY_SECONDS)
            except Exception as e:
                print(f"❌ Failed county '{county['county_name']}': {e}")
                continue

        await browser.close()

    # Build All Data sheet with dynamic header that includes Sale Type if present
    try:
        if not all_data_rows:
            print("⚠ No data scraped across all counties. Skipping 'All Data'.")
        else:
            # Determine if any row has 7 columns (i.e., includes Sale Type)
            # Our per-county df had columns including County; standard is 6, New Castle adds Sale Type => 7
            has_sale_type = any(len(r) == 7 for r in all_data_rows)
            if has_sale_type:
                header_all = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "Sale Type", "County"]
                # Normalize rows to 7 columns (insert empty Sale Type for rows with 6 columns)
                normalized_rows: List[List[str]] = []
                for r in all_data_rows:
                    if len(r) == 6:
                        normalized_rows.append([r[0], r[1], r[2], r[3], r[4], "", r[5]])
                    else:
                        normalized_rows.append(r)
                all_data_rows = normalized_rows
            else:
                header_all = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"]

            sheets.create_sheet_if_missing(ALL_DATA_SHEET)
            if first_run:
                sheets.overwrite_with_snapshot(ALL_DATA_SHEET, header_all, all_data_rows)
            else:
                existing = sheets.get_values(ALL_DATA_SHEET, "A:Z")
                existing_pairs = set()
                has_new_castle = has_sale_type
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
                        county_col_idx = 6 if has_new_castle else 5
                        cty = (r[county_col_idx] if len(r) > county_col_idx else "").strip()
                        if pid and cty:
                            existing_pairs.add((cty, pid))

                new_rows = []
                for r in all_data_rows:
                    pid = (r[0] if len(r) > 0 else "").strip()
                    county_col_idx = 6 if has_sale_type else 5
                    cty = (r[county_col_idx] if len(r) > county_col_idx else "").strip()
                    if pid and cty and (cty, pid) not in existing_pairs:
                        new_rows.append(r)

                if not new_rows:
                    print("✓ No new rows for 'All Data'")
                else:
                    existing_header = existing[1] if len(existing) > 1 else (existing[0] if existing else [])
                    if set(header_all) != set(existing_header or []):
                        print("[INFO] Updating All Data sheet structure for new columns")
                        all_existing_data = []
                        if existing:
                            header_idx = None
                            for idx, row in enumerate(existing[:5]):
                                if row and row[0].lower().replace(" ", "") in {"propertyid", "property id"}:
                                    header_idx = idx
                                    break
                            if header_idx is None:
                                header_idx = 1 if len(existing) > 1 else 0
                            for r in existing[header_idx + 1:]:
                                if r and not (len(r) == 1 and r[0].strip() == ""):
                                    all_existing_data.append(r)
                        combined_data = all_existing_data + new_rows
                        sheets.overwrite_with_snapshot(ALL_DATA_SHEET, header_all, combined_data)
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
