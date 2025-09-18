from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import colorama
colorama.init(autoreset=True)
import time
import csv
import os
import random
from datetime import datetime
import re
import logging

# === Google Sheets imports ===
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    from gspread_formatting import *
    SHEETS_ENABLED = True
except ImportError:
    SHEETS_ENABLED = False

# === Setup logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# === Config ===
SHEET_URL = "https://docs.google.com/spreadsheets/d/xxxx/edit"
WORKSHEET_NAME = "Scraped_Data"
PROFILES_SHEET = "Profiles"
ANALYTICS_SHEET = "Analytics"
MAX_PAGES = 2               # test run small pages
LOOP_INTERVAL_MINUTES = 5   # repeat interval

# === Google Sheets Setup ===
def setup_sheets():
    if not SHEETS_ENABLED:
        logging.warning("Google Sheets libraries not installed. Falling back to CSV only.")
        return None, None
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(SHEET_URL)

        # create or open worksheet
        try:
            ws = sheet.worksheet(WORKSHEET_NAME)
        except:
            ws = sheet.add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="20")

        # profiles sheet
        try:
            profiles_ws = sheet.worksheet(PROFILES_SHEET)
        except:
            profiles_ws = sheet.add_worksheet(title=PROFILES_SHEET, rows="1000", cols="10")
            profiles_ws.append_row(["Nick", "Gender", "City"])

        return sheet, ws
    except Exception as e:
        logging.error(f"Google Sheets setup failed: {e}")
        return None, None

# === Dummy Analytics (placeholder, expand later) ===
def update_analytics_sheet(sheet, data):
    if not SHEETS_ENABLED or not sheet:
        return
    try:
        try:
            analytics_ws = sheet.worksheet(ANALYTICS_SHEET)
        except:
            analytics_ws = sheet.add_worksheet(title=ANALYTICS_SHEET, rows="1000", cols="10")

        analytics_ws.clear()
        analytics_ws.append_row(["Metric", "Value"])
        analytics_ws.append_row(["Total posts scraped", str(len(data))])
        analytics_ws.append_row(["Last run", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
    except Exception as e:
        logging.error(f"Analytics update failed: {e}")

# === Scraper main ===
def run_scraper():
    logging.info("🚀 Starting DamaDam scraper run")

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    scraped_data = []

    try:
        driver.get("https://damadam.pk/login")

        # Dummy login
        user_id = "test_user"
        password = "test_pass"

        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.NAME, "id"))).send_keys(user_id)
        driver.find_element(By.NAME, "password").send_keys(password)
        driver.find_element(By.NAME, "submit").click()

        time.sleep(5)
        if "dashboard" in driver.current_url:
            logging.info("Login success ✅")
        else:
            logging.warning("Login failed ❌")

        # placeholder scraping
        for i in range(1, MAX_PAGES + 1):
            scraped_data.append([f"Post {i}", f"Content {i}", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            time.sleep(1)

    except TimeoutException:
        logging.error("Timeout while waiting for page element")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        driver.quit()

    # === Output to CSV ===
    with open("damadam_posts.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Post", "Content", "Scraped At"])
        writer.writerows(scraped_data)
    logging.info("CSV export done ✅")

    # === Output to Google Sheets ===
    sheet, ws = setup_sheets()
    if sheet and ws:
        try:
            ws.clear()
            ws.append_row(["Post", "Content", "Scraped At"])
            ws.append_rows(scraped_data)
            logging.info("Google Sheets updated ✅")
            update_analytics_sheet(sheet, scraped_data)
        except Exception as e:
            logging.error(f"Sheets update failed: {e}")

# === Loop runner ===
def main():
    while True:
        run_scraper()
        logging.info(f"Sleeping {LOOP_INTERVAL_MINUTES} minutes before next cycle...")
        time.sleep(LOOP_INTERVAL_MINUTES * 60)

if __name__ == "__main__":
    main()
