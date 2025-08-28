#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Foreclosure Sales Scraper (One-Time Full Load + Incremental Updates Thereafter)

- Runs headless with Playwright.
- Writes to Google Sheets using a Service Account.
- First-ever run (sheet "All Data" missing) -> full scrape for all TARGET_COUNTIES.
- Subsequent runs -> incremental updates (only new Property IDs) with a dated snapshot header.
- Robust retries & error handling so one county's failure won't stop the rest.
- Designed to run as a one-off script in CI/CD (no web server).

ENV VARS REQUIRED:
  - GOOGLE_CREDENTIALS  (the raw JSON of your Google Service Account)
  - SPREADSHEET_ID      (target Google Sheet ID)
"""

import os
import re
import sys
import json
import time
import asyncio
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ==============================
# Configuration
# ==============================
BASE_URL = "https://salesweb.civilview.com/"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_FILE_PATH = "service_account.json"

TARGET_COUNTIES = [
    {"county_id": "52", "county_name": "Cape May County, NJ"},
    # {"county_id": "25", "county_name": "Atlantic County, NJ"},
    {"county_id": "1",  "county_name": "Camden County, NJ"},
    {"county_id": "3",  "county_name": "Burlington County, NJ"},
    {"county_id": "6",  "county_name": "Cumberland County, NJ"},
    {"county_id": "19", "county_name": "Gloucester County, NJ"},
    {"county_id": "20", "county_name": "Salem County, NJ"},
    {"county_id": "15", "county_name": "Union County, NJ"},
]

# How long to wait between counties (be polite)
POLITE_DELAY_SECONDS = 1.5

# Max retries for page nav & scraping
MAX_RETRIES = 3

# ==============================
# Helpers
# ==============================
def create_service_account_file():
    """Creates the service account file from a JSON string environment variable."""
    credentials_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not credentials_json:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not set.")
    try:
        with open(CREDENTIALS_FILE_PATH, "w", encoding="utf-8") as f:
            f.write(credentials_json)
        print("✓ Service account file created from environment variable.")
    except Exception as e:
        print(f"✗ Error creating service account file: {e}")
        raise

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

def today_header_label(prefix="Snapshot for"):
    return f"{prefix} {datetime.now().strftime('%A - %Y-%m-%d')}"

def safe_sheet_title(name: str) -> str:
    # Google Sheets tab title limit is 100, but let's keep it short & safe
    return name[:30]

# ==============================
# Google Sheets Client
# ==============================
class SheetsClient:
    def __init__(self, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self.service = None
        self._init_client()

    def _init_client(self):
        try:
            creds = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE_PATH, scopes=SCOPES)
            self.service = build('sheets', 'v4', credentials=creds)
            print("✓ Google Sheets API client initialized.")
        except Exception as e:
            print(f"✗ Error initializing Google Sheets client: {e}")
            self.service = None

    def _svc(self):
        if not self.service:
            raise RuntimeError("Google Sheets service not initialized")
        return self.service.spreadsheets()

    def spreadsheet_info(self):
        try:
            return self._svc().get(spreadsheetId=self.spreadsheet_id).execute()
        except HttpError as e:
            print(f"✗ Error fetching spreadsheet info: {e}")
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
            self._svc().batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": [req]}).execute()
            print(f"✓ Created new sheet: {sheet_name}")
        except HttpError as e:
            # If the sheet was created by a concurrent run, ignore "already exists" type errors
            print(f"⚠ create_sheet_if_missing error on '{sheet_name}': {e}")

    def get_values(self, sheet_name: str, rng: str = "A:Z"):
        try:
            res = self._svc().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!{rng}"
            ).execute()
            return res.get("values", [])
        except HttpError as e:
            if "Unable to parse range" in str(e) or "not found" in str(e):
                return []
            print(f"⚠ get_values error on '{sheet_name}': {e}")
            return []

    def clear(self, sheet_name: str, rng: str = "A:Z"):
        try:
            self._svc().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!{rng}"
            ).execute()
        except HttpError as e:
            print(f"⚠ clear error on '{sheet_name}': {e}")

    def write_values(self, sheet_name: str, values, start_cell: str = "A1"):
        try:
            self._svc().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!{start_cell}",
                valueInputOption="USER_ENTERED",
                body={"values": values}
            ).execute()
        except HttpError as e:
            print(f"✗ write_values error on '{sheet_name}': {e}")
            raise

    def prepend_snapshot(self, sheet_name: str, header_row, new_rows):
        """
        Prepend a dated snapshot (header + header_row + new_rows + blank) ABOVE existing contents.
        If there are no existing values, this just writes the snapshot.
        """
        existing = self.get_values(sheet_name, "A:Z")
        snapshot_header = [[today_header_label()]]
        payload = snapshot_header + [header_row] + new_rows + [[""]]
        values = payload + existing
        self.clear(sheet_name, "A:Z")
        self.write_values(sheet_name, values, "A1")
        print(f"✓ Prepended snapshot to '{sheet_name}': {len(new_rows)} new rows")

    def overwrite_with_snapshot(self, sheet_name: str, header_row, all_rows):
        """
        Overwrite the entire sheet with a single snapshot (full dataset).
        """
        snapshot_header = [[today_header_label()]]
        values = snapshot_header + [header_row] + all_rows + [[""]]
        self.clear(sheet_name, "A:Z")
        self.write_values(sheet_name, values, "A1")
        print(f"✓ Wrote full snapshot to '{sheet_name}': {len(all_rows)} rows")

# ==============================
# Scraper
# ==============================
class ForeclosureScraper:
    def __init__(self, sheets: SheetsClient):
        self.sheets = sheets

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
        """Scrape one county's sales table (single page)."""
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
                    # Address might shift; guard by td count
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

                    # Deep dive into details page for better address / fields
                    if details_url:
                        try:
                            await self.goto_with_retry(page, details_url)
                            await self.dismiss_banners(page)
                            await page.wait_for_selector(".sale-details-list", timeout=15000)
                            items = page.locator(".sale-details-list .sale-detail-item")
                            items_count = await items.count()
                            for j in range(items_count):
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
                            # Go back to county list page
                            try:
                                await self.goto_with_retry(page, url)
                                await self.dismiss_banners(page)
                                await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)
                            except Exception as e:
                                print(f"⚠ Failed to return to list page for {county['county_name']}: {e}")

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

        print(f"[FAIL] Could not get complete data for {county['county_name']} after {MAX_RETRIES} attempts.")
        return []

# ==============================
# Run Logic: First-Time vs Incremental
# ==============================
async def run():
    start_ts = datetime.now()
    print(f"▶ Starting scrape at {start_ts}")

    # 1) Auth file
    create_service_account_file()

    # 2) Sheets client
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        print("✗ SPREADSHEET_ID environment variable not set.")
        sys.exit(1)
    sheets = SheetsClient(spreadsheet_id)

    # Determine FIRST RUN by presence of "All Data" sheet
    ALL_DATA_SHEET = "All Data"
    first_run = not sheets.sheet_exists(ALL_DATA_SHEET)
    print(f"ℹ First run? {'YES' if first_run else 'NO'}")

    all_data_rows = []  # rows: [Property ID, Address, Defendant, Sales Date, Approx Judgment, County]

    # 3) Playwright browser
    async with async_playwright() as p:
        # In CI, Chromium is recommended; ensure browsers are installed in your pipeline beforehand.
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        scraper = ForeclosureScraper(sheets)

        for county in TARGET_COUNTIES:
            county_tab = safe_sheet_title(county["county_name"])
            try:
                print(f"—" * 60)
                print(f"▶ County: {county['county_name']} (tab: {county_tab})")

                # Scrape this county
                county_records = await scraper.scrape_county_sales(page, county)
                if not county_records:
                    print(f"⚠ No data scraped for {county['county_name']}")
                    await asyncio.sleep(POLITE_DELAY_SECONDS)
                    continue

                # Convert to DataFrame (consistent column order)
                df_county = pd.DataFrame(
                    county_records,
                    columns=["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"]
                )

                # Ensure county sheet exists (created later if needed)
                # Decide behavior: full write vs incremental
                if first_run or not sheets.sheet_exists(county_tab):
                    # FULL SNAPSHOT WRITE for this county
                    sheets.create_sheet_if_missing(county_tab)
                    header = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment"]
                    # County tab does NOT store the County column (to keep columns compact)
                    rows = df_county.drop(columns=["County"]).astype(str).values.tolist()
                    sheets.overwrite_with_snapshot(county_tab, header, rows)
                else:
                    # INCREMENTAL for this county: only new Property IDs
                    existing = sheets.get_values(county_tab, "A:Z")
                    # Extract existing header + rows into DataFrame to get existing IDs
                    existing_ids = set()
                    if existing:
                        # Find header row (first row after the "Snapshot for..." line)
                        # existing looks like: [ ["Snapshot for..."], [header...], [rows...], [""], [maybe older snapshot header], ... ]
                        # We'll scan for the first header line that matches our expected header.
                        header_idx = None
                        for idx, row in enumerate(existing[:5]):  # search the top few lines
                            if row and row[0].lower() in {"property id", "propertyid"}:
                                header_idx = idx
                                break
                        if header_idx is None:
                            # fallback: try to find the most common header placement (second row)
                            header_idx = 1 if len(existing) > 1 else 0
                        # Build set from all rows (skip snapshot headers and blanks)
                        for r in existing[header_idx + 1:]:
                            if not r or (len(r) == 1 and r[0].strip() == ""):
                                continue
                            pid = (r[0] or "").strip()
                            if pid:
                                existing_ids.add(pid)

                    new_df = df_county[~df_county["Property ID"].isin(existing_ids)].copy()
                    if new_df.empty:
                        print(f"✓ No new rows for {county['county_name']}. Skipping write.")
                    else:
                        header = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment"]
                        new_rows = new_df.drop(columns=["County"]).astype(str).values.tolist()
                        sheets.prepend_snapshot(county_tab, header, new_rows)

                # Accumulate for All Data
                all_data_rows.extend(df_county.astype(str).values.tolist())

                print(f"✓ Completed {county['county_name']}: {len(df_county)} records")
                await asyncio.sleep(POLITE_DELAY_SECONDS)

            except Exception as e:
                print(f"❌ Failed county '{county['county_name']}': {e}")
                # continue to next county
                continue

        await browser.close()

    # 4) Write "All Data"
    try:
        header_all = ["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"]
        if not all_data_rows:
            print("⚠ No data scraped across all counties. 'All Data' not updated.")
        else:
            sheets.create_sheet_if_missing(ALL_DATA_SHEET)
            if first_run:
                # Full overwrite with snapshot
                sheets.overwrite_with_snapshot(ALL_DATA_SHEET, header_all, all_data_rows)
            else:
                # Incremental: dedupe against existing "All Data" by (County, Property ID)
                existing = sheets.get_values(ALL_DATA_SHEET, "A:Z")
                existing_pairs = set()
                if existing:
                    # Find first header row
                    header_idx = None
                    for idx, row in enumerate(existing[:5]):
                        if row and row[0].lower() in {"property id", "propertyid"}:
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
                    print("✓ No new rows for 'All Data'. Skipping write.")
                else:
                    sheets.prepend_snapshot(ALL_DATA_SHEET, header_all, new_rows)
                    print(f"✓ All Data updated: {len(new_rows)} new rows")
    except Exception as e:
        print(f"✗ Error updating 'All Data': {e}")

    end_ts = datetime.now()
    print(f"■ Finished scrape at {end_ts} (duration: {end_ts - start_ts})")

# ==============================
# Entrypoint
# ==============================
if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("Interrupted by user.")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
