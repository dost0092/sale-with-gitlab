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
    {"county_id": "15", "county_name": "Union County, NJ"}
]

POLITE_DELAY_SECONDS = 1.5
MAX_RETRIES = 3

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
            # return empty if sheet missing
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
        except HttpError as e:
            print(f"✗ write_values error on '{sheet_name}': {e}")
            raise

    def prepend_snapshot(self, sheet_name: str, header_row, new_rows):
        existing = self.get_values(sheet_name, "A:Z")
        snapshot_header = [[f"Snapshot for {datetime.now().strftime('%A - %Y-%m-%d')}"]]
        payload = snapshot_header + [header_row] + new_rows + [[""]]
        values = payload + existing
        self.clear(sheet_name, "A:Z")
        self.write_values(sheet_name, values, "A1")
        print(f"✓ Prepended snapshot to '{sheet_name}': {len(new_rows)} rows")

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

    async def scrape_county_sales(self, page, county):
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

                rows = page.locator("table.table.table-striped tbody tr")
                n = await rows.count()
                results = []
                for i in range(n):
                    row = rows.nth(i)
                    details_a = row.locator("td.hidden-print a")
                    details_href = (await details_a.get_attribute("href")) or ""
                    details_url = details_href if details_href.startswith("http") else urljoin(BASE_URL, details_href)
                    property_id = extract_property_id_from_href(details_href)

                    try:
                        sales_date = norm_text(await row.locator("td").nth(2).inner_text())
                    except Exception:
                        sales_date = ""
                    try:
                        defendant = norm_text(await row.locator("td").nth(4).inner_text())
                    except Exception:
                        defendant = ""
                    try:
                        tds = row.locator("td")
                        td_count = await tds.count()
                        if td_count >= 6:
                            prop_address = norm_text(await tds.nth(5).inner_text())
                        else:
                            prop_address = ""
                    except Exception:
                        prop_address = ""

                    approx_judgment = ""
                    if details_url:
                        try:
                            await self.goto_with_retry(page, details_url)
                            await self.dismiss_banners(page)
                            await page.wait_for_selector(".sale-details-list", timeout=15000)
                            items = page.locator(".sale-details-list .sale-detail-item")
                            for j in range(await items.count()):
                                label = norm_text(await items.nth(j).locator(".sale-detail-label").inner_text())
                                val = norm_text(await items.nth(j).locator(".sale-detail-value").inner_text())
                                if ("Address" in label or "Property Address" in label):
                                    try:
                                        val_html = await items.nth(j).locator(".sale-detail-value").inner_html()
                                        val_html = re.sub(r"<br\s*/?>", " ", val_html)
                                        val_clean = re.sub(r"<.*?>", "", val_html).strip()
                                        details_address = norm_text(val_clean)
                                        if not prop_address or len(details_address) > len(prop_address):
                                            prop_address = details_address
                                    except Exception:
                                        if not prop_address:
                                            prop_address = norm_text(val)
                                elif ("Approx. Judgment" in label or "Approx. Upset" in label
                                      or "Approximate Judgment:" in label or "Approx Judgment*" in label):
                                    approx_judgment = val
                                elif "Defendant" in label and not defendant:
                                    defendant = val
                                elif "Sale Date" in label and not sales_date:
                                    sales_date = val
                        except Exception as e:
                            print(f"⚠ Details page error for {county['county_name']} (PropertyId={property_id}): {e}")
                        finally:
                            # return to list page
                            try:
                                await self.goto_with_retry(page, url)
                                await self.dismiss_banners(page)
                                await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
                            except Exception:
                                pass

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
