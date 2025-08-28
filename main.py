import os
import re
import asyncio
import pandas as pd
import argparse
import sys
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import json
from fastapi import FastAPI
import uvicorn

# Configuration
BASE_URL = "https://salesweb.civilview.com/"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
LOCAL_CSV_PATH = "foreclosure_sales.csv"
CREDENTIALS_FILE_PATH = "service_account.json"

TARGET_COUNTIES = [
    {"county_id": "52", "county_name": "Cape May County, NJ"},
    # {"county_id": "25", "county_name": "Atlantic County, NJ"}, # Uncomment to include Atlantic County
    {"county_id": "1", "county_name": "Camden County, NJ"},
    {"county_id": "3", "county_name": "Burlington County, NJ"},
    {"county_id": "6", "county_name": "Cumberland County, NJ"},
    {"county_id": "19", "county_name": "Gloucester County, NJ"},
    {"county_id": "20", "county_name": "Salem County, NJ"},
    {"county_id": "15", "county_name": "Union County, NJ"}
]

def create_service_account_file():
    """Creates the service account file from a JSON string environment variable."""
    credentials_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not credentials_json:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not set.")
    try:
        with open(CREDENTIALS_FILE_PATH, "w") as f:
            f.write(credentials_json)
        print("Service account file created from environment variable.")
    except Exception as e:
        print(f"Error creating service account file: {e}")
        raise

class ForeclosureScraper:
    def __init__(self, scrape_mode="initial"):
        self.credentials = None
        self.service = None
        self.spreadsheet_id = os.environ.get("SPREADSHEET_ID")
        self.scrape_mode = scrape_mode  # "initial", "update", or "emergency"
        self.setup_google_credentials()
        
    def setup_google_credentials(self):
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                CREDENTIALS_FILE_PATH, scopes=SCOPES)
            self.service = build('sheets', 'v4', credentials=self.credentials)
            print("Google Sheets API client initialized successfully")
        except Exception as e:
            print(f"Error initializing Google Sheets client: {e}")
            self.service = None

    def norm_text(self, s: str) -> str:
        if not s:
            return ""
        return re.sub(r"\s+", " ", s).strip()

    def extract_property_id_from_href(self, href: str) -> str:
        try:
            q = parse_qs(urlparse(href).query)
            return q.get("PropertyId", [""])[0]
        except Exception:
            return ""

    async def goto_with_retry(self, page, url: str, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = await page.goto(url, wait_until="networkidle", timeout=60000)
                if response and response.status == 200:
                    return response
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                await asyncio.sleep(2 ** attempt)
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

    async def get_existing_property_ids(self, county_name):
        """Get existing property IDs from Google Sheet to avoid duplicates in update mode."""
        if self.scrape_mode == "initial":
            return set()  # For initial scrape, we want all data
            
        if not self.service or not self.spreadsheet_id:
            print("✗ Google Sheets service not initialized. Cannot fetch existing IDs.")
            return set()
        
        try:
            sheet_name = county_name[:30]
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!A:A"
            ).execute()
            values = result.get('values', [])
            
            # Skip header row and extract property IDs
            existing_ids = set()
            if values and len(values) > 1:
                for row in values[1:]:  # Skip header
                    if row and row[0]:  # Check if row has data
                        existing_ids.add(row[0])
            
            print(f"Found {len(existing_ids)} existing property IDs for {county_name}")
            return existing_ids
            
        except HttpError as e:
            if "not found" in str(e).lower():
                print(f"Sheet '{sheet_name}' not found. Will scrape all data.")
            else:
                print(f"Error fetching existing IDs for {county_name}: {e}")
            return set()

    async def scrape_county_sales(self, page, county, max_retries=3):
        url = f"{BASE_URL}Sales/SalesSearch?countyId={county['county_id']}"
        print(f"[INFO] Scraping {county['county_name']} -> {url} (Mode: {self.scrape_mode})")

        # Get existing property IDs for update mode
        existing_ids = await self.get_existing_property_ids(county['county_name'])

        for attempt in range(max_retries):
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
                new_records = 0
                skipped_records = 0

                for i in range(n):
                    row = rows.nth(i)
                    details_a = row.locator("td.hidden-print a")
                    details_href = await details_a.get_attribute("href") or ""
                    details_url = details_href if details_href.startswith("http") else urljoin(BASE_URL, details_href)
                    property_id = self.extract_property_id_from_href(details_href)

                    # Skip existing records in update mode
                    if self.scrape_mode == "update" and property_id in existing_ids:
                        skipped_records += 1
                        continue

                    try:
                        sales_date = self.norm_text(await row.locator("td").nth(2).inner_text())
                    except Exception:
                        sales_date = ""
                    try:
                        defendant = self.norm_text(await row.locator("td").nth(4).inner_text())
                    except Exception:
                        defendant = ""
                        
                    try:
                        tds = row.locator("td")
                        td_count = await tds.count()
                        if td_count >= 6:
                            prop_address = self.norm_text(await tds.nth(5).inner_text())
                        else:
                            prop_address = ""
                    except Exception as e:
                        prop_address = ""

                    approx_judgment = ""

                    if details_url:
                        try:
                            await self.goto_with_retry(page, details_url)
                            await self.dismiss_banners(page)
                            await page.wait_for_selector(".sale-details-list", timeout=15000)
                            items = page.locator(".sale-details-list .sale-detail-item")
                            for j in range(await items.count()):
                                label = self.norm_text(await items.nth(j).locator(".sale-detail-label").inner_text())
                                val = self.norm_text(await items.nth(j).locator(".sale-detail-value").inner_text())
                                if ("Address" in label or "Property Address" in label):
                                    try:
                                        val_html = await items.nth(j).locator(".sale-detail-value").inner_html()
                                        val_html = re.sub(r"<br\s*/?>", " ", val_html)
                                        val_clean = re.sub(r"<.*?>", "", val_html).strip()
                                        details_address = self.norm_text(val_clean)
                                        if not prop_address or len(details_address) > len(prop_address):
                                            prop_address = details_address
                                    except Exception:
                                        if not prop_address:
                                            prop_address = self.norm_text(val)
                                elif ("Approx. Judgment" in label or "Approx. Upset" in label or "Approximate Judgment:" in label or "Approx Judgment*" in label):
                                    approx_judgment = val
                                elif "Defendant" in label and not defendant:
                                    defendant = val
                                elif "Sale Date" in label and not sales_date:
                                    sales_date = val

                        except Exception as e:
                            print(f"Error scraping details for {county['county_name']}: {str(e)}")
                        finally:
                            await self.goto_with_retry(page, url)
                            await self.dismiss_banners(page)
                            await page.wait_for_selector("table.table.table-striped tbody tr, .no-sales, #noData", timeout=30000)

                    results.append({
                        "Property ID": property_id,
                        "Address": prop_address,
                        "Defendant": defendant,
                        "Sales Date": sales_date,
                        "Approx Judgment": approx_judgment,
                        "County": county['county_name']
                    })
                    new_records += 1

                print(f"[INFO] {county['county_name']}: {new_records} new records, {skipped_records} skipped")
                return results
            except Exception as e:
                print(f"Error scraping {county['county_name']} (Attempt {attempt+1}/{max_retries}): {str(e)}")
                await asyncio.sleep(2 ** attempt)
                continue
        print(f"[FAIL] Could not get complete data for {county['county_name']} after {max_retries} attempts.")
        return results

    async def update_google_sheet_tab(self, df, sheet_name, snapshot=False):
        if not self.service or not self.spreadsheet_id:
            print("✗ Google Sheets service not initialized. Skipping update.")
            return

        try:
            sheet = self.service.spreadsheets()
            
            # For update mode, we want to append new data, not replace everything
            if self.scrape_mode == "update" and len(df) > 0:
                await self.append_to_google_sheet(df, sheet_name)
                return
            
            # For initial mode, do the full snapshot approach
            values = [list(df.columns)] + df.astype(str).values.tolist()

            if snapshot:
                # Prepend snapshot header with Day + Date + Mode
                today = datetime.now().strftime("%A - %Y-%m-%d")
                mode_text = f"({self.scrape_mode.upper()} SCRAPE)"
                snapshot_header = [[f"Snapshot for {today} {mode_text}"]]
                values = snapshot_header + values + [[""]]  # add blank row at bottom

            # Check if sheet exists
            sheet_exists = False
            try:
                result = sheet.get(spreadsheetId=self.spreadsheet_id).execute()
                for s in result.get('sheets', []):
                    if s['properties']['title'] == sheet_name:
                        sheet_exists = True
                        break
            except HttpError as e:
                print(f"Error checking for sheets: {e}")
                return

            if not sheet_exists:
                add_sheet_request = {"addSheet": {"properties": {"title": sheet_name}}}
                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={"requests": [add_sheet_request]}
                ).execute()
                print(f"Created new sheet: {sheet_name}")

            if snapshot and self.scrape_mode == "initial":
                # For initial mode, append new snapshot on top of existing sheet
                existing = sheet.values().get(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"'{sheet_name}'!A:Z"
                ).execute()
                old_values = existing.get("values", [])
                values = values + old_values  # push down existing content

            # Clear & update (only for initial mode)
            if self.scrape_mode == "initial":
                sheet.values().clear(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"'{sheet_name}'!A:Z"
                ).execute()
                
            sheet.values().update(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!A1",
                valueInputOption="USER_ENTERED",
                body={"values": values}
            ).execute()

            print(f"✓ Updated Google Sheet tab: {sheet_name} ({len(df)} rows, mode={self.scrape_mode})")
        except Exception as e:
            print(f"✗ Google Sheets update error on {sheet_name}: {e}")

    async def append_to_google_sheet(self, df, sheet_name):
        """Append new records to existing Google Sheet (for update mode)."""
        try:
            sheet = self.service.spreadsheets()
            
            # Get current data range to append after
            existing = sheet.values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{sheet_name}'!A:Z"
            ).execute()
            existing_rows = len(existing.get("values", []))
            
            # Prepare new data (without headers)
            new_values = df.astype(str).values.tolist()
            
            # Add timestamp comment for new batch
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            new_values.insert(0, [f"--- UPDATE {timestamp} ---", "", "", "", "", ""])
            
            # Append to sheet
            append_range = f"'{sheet_name}'!A{existing_rows + 1}"
            sheet.values().append(
                spreadsheetId=self.spreadsheet_id,
                range=append_range,
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": new_values}
            ).execute()
            
            print(f"✓ Appended {len(df)} new records to {sheet_name}")
            
        except Exception as e:
            print(f"✗ Error appending to Google Sheet {sheet_name}: {e}")

    async def scrape_and_update(self):
        print(f"Starting {self.scrape_mode.upper()} scrape at {datetime.now()}")
        create_service_account_file()
        
        all_data = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            for county in TARGET_COUNTIES:
                try:
                    print(f"▶ Scraping {county['county_name']} (Mode: {self.scrape_mode})...")
                    county_data = await self.scrape_county_sales(page, county)
                    
                    if county_data:
                        all_data.extend(county_data)
                        
                        # Convert to DataFrame
                        df_county = pd.DataFrame(
                            county_data, 
                            columns=["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"]
                        )
                        
                        # Update county tab based on mode
                        await self.update_google_sheet_tab(
                            df_county.drop(columns=["County"]), 
                            county["county_name"][:30], 
                            snapshot=(self.scrape_mode == "initial")
                        )
                        print(f"✓ Completed {county['county_name']}: {len(county_data)} records")
                    else:
                        print(f"⚠ No new data for {county['county_name']}")

                    await asyncio.sleep(2)  # polite delay
                except Exception as e:
                    print(f"❌ Failed to scrape {county['county_name']}: {str(e)}")
                    continue
            
            await browser.close()
        
        # Update "All Data" tab
        if all_data:
            df_all = pd.DataFrame(
                all_data, 
                columns=["Property ID", "Address", "Defendant", "Sales Date", "Approx Judgment", "County"]
            )
            await self.update_google_sheet_tab(
                df_all, 
                "All Data", 
                snapshot=(self.scrape_mode == "initial")
            )
            print(f"✓ All Data tab updated: {len(all_data)} records total (Mode: {self.scrape_mode})")
        else:
            print(f"⚠ No data scraped across all counties (Mode: {self.scrape_mode}).")
            
        print(f"Finished {self.scrape_mode.upper()} scrape at {datetime.now()}")
        return {"status": "success", "message": f"Scraped {len(all_data)} records in {self.scrape_mode} mode."}

# FastAPI setup (unchanged for API usage)
app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Foreclosure Scraper API. Go to /scrape to run the scraper."}

@app.get("/scrape")
async def run_scrape():
    scraper = ForeclosureScraper()
    return await scraper.scrape_and_update()

# CLI interface for GitLab CI/CD
async def main():
    parser = argparse.ArgumentParser(description='Foreclosure Scraper')
    parser.add_argument('--mode', choices=['initial', 'update', 'emergency'], 
                       default='initial', help='Scrape mode')
    
    args = parser.parse_args()
    
    print(f"Running scraper in {args.mode.upper()} mode")
    scraper = ForeclosureScraper(scrape_mode=args.mode)
    result = await scraper.scrape_and_update()
    
    if result["status"] == "success":
        print("✅ Scraping completed successfully!")
        sys.exit(0)
    else:
        print("❌ Scraping failed!")
        sys.exit(1)

if __name__ == "__main__":
    # Check if running as CLI or API
    if len(sys.argv) > 1:
        # Running as CLI (GitLab CI/CD)
        asyncio.run(main())
    else:
        # Running as API
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))