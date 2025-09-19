import os
import json
import base64
import logging
import time
from datetime import datetime
import csv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

import gspread
from google.oauth2.service_account import Credentials

# ----------------- Logging Setup -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ----------------- Helper Functions -----------------
def init_driver():
    logger.info("====== WebDriver manager ======")
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def export_csv(data, filename="output.csv"):
    if not data:
        logger.warning("⚠️ No data to export in CSV")
        return
    keys = data[0].keys()
    with open(filename, "w", newline="", encoding="utf-8") as f:
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    logger.info("CSV export done ✅")

def connect_google_sheet():
    try:
        # Get Base64 encoded JSON from GitHub secrets
        service_json_base64 = os.getenv("SERVICE_JSON")
        if not service_json_base64:
            raise Exception("SERVICE_JSON not found in environment variables")

        # Decode Base64 → JSON
        service_json_str = base64.b64decode(service_json_base64).decode("utf-8")
        service_account_info = json.loads(service_json_str)

        # Authenticate with gspread
        creds = Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)

        sheet_url = os.getenv("SHEET_URL")
        if not sheet_url:
            raise Exception("SHEET_URL not found in environment variables")

        sheet = client.open_by_url(sheet_url).sheet1
        logger.info("Google Sheets connected ✅")
        return sheet
    except Exception as e:
        logger.error(f"Google Sheets setup failed: {e}")
        return None

# ----------------- Main Scraper -----------------
def run_scraper():
    driver = None
    try:
        driver = init_driver()
        driver.get("https://example.com")  # replace with actual site

        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            logger.error("Timeout while waiting for page element")

        # Example scraped data
        data = [{"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "status": "OK"}]

        # Export to CSV
        export_csv(data)

        # Export to Google Sheets
        sheet = connect_google_sheet()
        if sheet:
            for row in data:
                sheet.append_row(list(row.values()))
            logger.info("Data pushed to Google Sheets ✅")

    except Exception as e:
        logger.error(f"Scraper failed: {e}")

    finally:
        if driver:
            driver.quit()
        logger.info("Sleeping 5 minutes before next cycle...")
        time.sleep(300)


if __name__ == "__main__":
    while True:
        run_scraper()
