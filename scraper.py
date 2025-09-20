"""
DamaDam Scraper - GitHub Actions Compatible Version
Optimized for headless operation with environment-based configuration
"""

import os
import json
import base64
import logging
import time
from datetime import datetime, timedelta
import csv
import re
import hashlib
import random
from collections import defaultdict, Counter

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime

# Google Sheets imports
import gspread
from google.oauth2.service_account import Credentials

# ----------------- Configuration -----------------
LOGIN_URL = "https://damadam.pk/login/"
BASE = "https://damadam.pk"
START_URL_TEMPLATE = "https://damadam.pk/text/fresh-list/?page={page}"

# Environment variables (GitHub Secrets)
USERNAME = os.getenv("DD_USERNAME")
PASSWORD = os.getenv("DD_PASSWORD")
SHEET_URL = os.getenv("SHEET_URL")
SERVICE_JSON_B64 = os.getenv("SERVICE_JSON")

# Settings
MAX_PAGES = int(os.getenv("MAX_PAGES", "5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
PAGE_TIMEOUT = int(os.getenv("PAGE_TIMEOUT", "5"))
MIN_DELAY = float(os.getenv("MIN_DELAY", "1.2"))
MAX_DELAY = float(os.getenv("MAX_DELAY", "1.6"))

# Sheet names
WORKSHEET_NAME = "Text-Post2"
PROFILES_SHEET = "Profiles"
ANALYTICS_SHEET = "User-Analytics"

# CSV backup
CSV_FILE = "posts_backup_new.csv"

# ----------------- Logging Setup -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ----------------- Headers Structure -----------------
# >>> CHANGE: Added "SCRAPE_TIME" as the first column so every inserted row starts with scrape timestamp.
HEADERS = [
    "SCRAPE_TIME",   # >>> CHANGE: current scrape time (YYYY-mm-dd HH:MM:SS)
    "A_IMAGE",       # =IMAGE(URL,4,35,35)
    "B_NICKNAME",    # Nickname 
    "C_PAGE#",       # Page number
    "D_TEXT-P",      # Post text (trimmed)
    "E_GENDER",      # From Profiles sheet
    "F_CITY",        # From Profiles sheet
    "G_EXPIRY",      # Time icon
    "H_REPLY",       # Reply count (like 5)
    "I_R-ON",        # Reply on/off status
    "J_COM1",        # Comment 1
    "K_COM2",        # Comment 2
    "L_COM3",        # Comment 3
    "M_PRO-L",       # Profile link
    "N_POST-L",      # Post link
    "O_COM1-L",      # Comment 1 link
    "P_COM2-L",      # Comment 2 link
    "Q_COM3-L",      # Comment 3 link
    "R_IMAGE-L"      # Image source URL
]

# ----------------- Statistics Tracking -----------------
class ScrapingStats:
    def __init__(self):
        self.reset()

    def reset(self):
        self.session_start_time = datetime.now()
        self.total_pages = 0
        self.current_page = 0
        self.posts_scraped = 0
        self.posts_updated = 0
        self.posts_new = 0
        self.profiles_loaded = 0
        self.analytics_users = 0
        self.errors = 0
        self.api_calls = 0

    def add_posts(self, new_count, updated_count):
        self.posts_new += new_count
        self.posts_updated += updated_count
        self.posts_scraped += (new_count + updated_count)

    def error(self):
        self.errors += 1

    def api_call(self):
        self.api_calls += 1

    def duration(self):
        return datetime.now() - self.session_start_time

    def success_rate(self):
        total_attempts = self.posts_scraped + self.errors
        if total_attempts == 0:
            return 100.0
        return (self.posts_scraped / total_attempts) * 100

    def posts_per_min(self):
        mins = max(self.duration().total_seconds() / 60.0, 1e-9)
        return self.posts_scraped / mins

stats = ScrapingStats()

# Global analytics data
analytics_data = defaultdict(lambda: {
    'total_posts': 0,
    'total_comments': 0,
    'commented_on': set(),
    'commenters': defaultdict(int),
    'posts_links': [],
    'gender': '',
    'city': '',
    'daily_activity': defaultdict(int)
})

# ----------------- Helper Functions -----------------
def setup_driver():
    """Setup Chrome driver optimized for GitHub Actions"""
    logger.info("Setting up Chrome WebDriver for headless operation...")
    
    options = webdriver.ChromeOptions()
    # GitHub Actions optimized options
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-images")  # Speed up loading
    # NOTE: disabling JS can break site rendering; if you face empty pages, remove the next line
    options.add_argument("--disable-javascript")  # If not needed
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--user-agent=Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        logger.info("Chrome WebDriver initialized successfully")
        return driver
    except Exception as e:
        logger.error(f"Failed to setup WebDriver: {e}")
        raise

def connect_google_sheet():
    """Connect to Google Sheets using environment variables"""
    try:
        if not SERVICE_JSON_B64:
            raise Exception("SERVICE_JSON environment variable not found")
        
        # Decode Base64 → JSON
        service_json_str = base64.b64decode(SERVICE_JSON_B64).decode("utf-8")
        service_account_info = json.loads(service_json_str)
        
        # Authenticate with gspread
        creds = Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)
        
        if not SHEET_URL:
            raise Exception("SHEET_URL environment variable not found")
        
        sheet = client.open_by_url(SHEET_URL)
        
        try:
            worksheet = sheet.worksheet(WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            logger.info(f"Creating new worksheet: {WORKSHEET_NAME}")
            worksheet = sheet.add_worksheet(title=WORKSHEET_NAME, rows=2000, cols=len(HEADERS))
        
        # Set headers if needed
        existing_headers = worksheet.row_values(1) if worksheet.row_count > 0 else []
        if existing_headers[:len(HEADERS)] != HEADERS:
            worksheet.append_row(HEADERS)
            logger.info("Headers updated in worksheet")
        
        logger.info("Google Sheets connected successfully")
        return worksheet
        
    except Exception as e:
        logger.error(f"Google Sheets setup failed: {e}")
        return None

def human_delay():
    """Random delay to mimic human behavior"""
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

def clean_text(text):
    """Clean and normalize text"""
    if not text:
        return ""
    cleaned = re.sub(r'[\n\r]+', ' ', text)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def text_hash(text):
    """Generate hash for text comparison"""
    if not text:
        return ""
    return hashlib.md5(clean_text(text).encode()).hexdigest()[:12]

def to_abs_url(path):
    """Convert to absolute URL"""
    if not path or path.startswith("http"):
        return path
    return BASE + (path if path.startswith("/") else "/" + path)

def export_csv(data, filename=CSV_FILE):
    """Export data to CSV as backup"""
    if not data:
        logger.warning("No data to export to CSV")
        return
    
    try:
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=HEADERS)
            writer.writeheader()
            for row in data:
                # Convert row list to dict using headers
                if isinstance(row, list):
                    row_dict = {HEADERS[i]: row[i] if i < len(row) else "" for i in range(len(HEADERS))}
                    writer.writerow(row_dict)
                else:
                    writer.writerow(row)
        logger.info(f"CSV backup saved: {filename}")
    except Exception as e:
        logger.error(f"CSV export failed: {e}")

# ----------------- Profile and Analytics Functions -----------------
def load_profiles_data(worksheet=None):
    """Load profiles data from Google Sheets"""
    profiles = {}
    if not worksheet:
        return profiles
    
    logger.info("Loading user profiles from Google Sheets...")
    
    try:
        sheet = worksheet.spreadsheet
        profiles_ws = sheet.worksheet(PROFILES_SHEET)
        records = profiles_ws.get_all_records()
        stats.api_call()
        
        for record in records:
            nickname = record.get('NICKNAME', '').strip()
            if nickname:
                profiles[nickname] = {
                    'gender': record.get('GENDER', '').strip(),
                    'city': record.get('CITY', '').strip()
                }
        
        stats.profiles_loaded = len(profiles)
        logger.info(f"Loaded {len(profiles)} user profiles")
        
    except Exception as e:
        logger.warning(f"Could not load profiles data: {e}")
    
    return profiles

def generate_analytics_data():
    """Generate analytics summary"""
    today = datetime.now().strftime("%Y-%m-%d")
    analytics_summary = []
    
    # Headers for analytics sheet
    analytics_headers = [
        "NICKNAME", "TOTAL_POSTS", "TOTAL_COMMENTS", "MOST_ACTIVE_COMMENTER", 
        "COMMENT_DIVERSITY", "GENDER", "CITY", "TODAY_ACTIVITY", "POST_LINKS"
    ]
    analytics_summary.append(analytics_headers)
    
    # Sort users by total activity
    sorted_users = sorted(analytics_data.items(), 
                         key=lambda x: x[1]['total_posts'] + x[1]['total_comments'], 
                         reverse=True)
    
    for nickname, data in sorted_users:
        if data['total_posts'] > 0 or data['total_comments'] > 0:
            # Find most active commenter
            most_active_commenter = ""
            if data['commenters']:
                most_active_commenter = max(data['commenters'].items(), key=lambda x: x[1])[0]
            
            comment_diversity = len(data['commenters'])
            today_activity = data['daily_activity'][today]
            post_links = " | ".join(data['posts_links'][:3])
            
            row = [
                nickname,
                data['total_posts'],
                data['total_comments'], 
                most_active_commenter,
                comment_diversity,
                data['gender'],
                data['city'],
                today_activity,
                post_links
            ]
            analytics_summary.append(row)
    
    return analytics_summary

def update_analytics_sheet(worksheet):
    """Update analytics worksheet"""
    if not worksheet:
        return
    
    logger.info("Updating user analytics...")
    
    try:
        sheet = worksheet.spreadsheet
        
        try:
            analytics_ws = sheet.worksheet(ANALYTICS_SHEET)
        except gspread.WorksheetNotFound:
            logger.info("Creating analytics worksheet...")
            analytics_ws = sheet.add_worksheet(title=ANALYTICS_SHEET, rows=1000, cols=10)
        
        analytics_ws.clear()
        analytics_data_list = generate_analytics_data()
        
        if analytics_data_list:
            analytics_ws.append_rows(analytics_data_list, value_input_option="USER_ENTERED")
            record_count = len(analytics_data_list) - 1
            logger.info(f"Analytics updated: {record_count} user records")
            stats.api_call()
        
    except Exception as e:
        logger.error(f"Analytics update failed: {e}")

# ----------------- Data Extraction Functions -----------------
def extract_reply_count(article):
    """Extract reply count from article"""
    try:
        comment_count_elem = article.find_element(By.CSS_SELECTOR, "[itemprop='commentCount']")
        count_text = comment_count_elem.text.strip()
        match = re.search(r'(\d+)', count_text)
        if match:
            return int(match.group(1))
    except NoSuchElementException:
        pass
    
    try:
        comments = article.find_elements(By.CSS_SELECTOR, "[itemprop='comment']")
        return len(comments)
    except:
        pass
    
    return 0

def extract_reply_status(article):
    """Check reply status"""
    try:
        if article.find_elements(By.XPATH, ".//div[contains(text(),'REPLIES OFF')]"):
            return "OFF"
        if article.find_elements(By.XPATH, ".//mark[contains(text(),'FOLLOW TO REPLY')]"):
            return "FOLLOW"
        reply_forms = article.find_elements(By.CSS_SELECTOR, "form[action*='direct-response']")
        if reply_forms:
            return "ON"
    except:
        pass
    return "ON"

def extract_post_data(article, page_num, profiles_data):
    """Extract post data with new structure"""
    data = {header: "" for header in HEADERS}
    data["C_PAGE#"] = f"Page {page_num}"
    
    try:
        # Author info
        try:
            author_elem = article.find_element(By.CSS_SELECTOR, "[itemprop='author'] a")
            nickname = clean_text(author_elem.text)
            data["B_NICKNAME"] = nickname
            data["M_PRO-L"] = to_abs_url(author_elem.get_attribute("href"))
            
            # Profile lookup
            if nickname in profiles_data:
                data["E_GENDER"] = profiles_data[nickname]['gender']
                data["F_CITY"] = profiles_data[nickname]['city']
        except NoSuchElementException:
            pass

        # Profile image
        try:
            img = article.find_element(By.CSS_SELECTOR, "img")
            img_src = img.get_attribute("data-src") or img.get_attribute("src")
            image_url = to_abs_url(img_src) if img_src else f"{BASE}/static/img/default-avatar-min.jpg"
            
            data["R_IMAGE-L"] = image_url
            data["A_IMAGE"] = f'=IMAGE("{image_url}",4,35,35)'
        except NoSuchElementException:
            default_img = f"{BASE}/static/img/default-avatar-min.jpg"
            data["R_IMAGE-L"] = default_img
            data["A_IMAGE"] = f'=IMAGE("{default_img}",4,35,35)'

        # Post text
        try:
            text_elem = article.find_element(By.CSS_SELECTOR, "[itemprop='text']")
            data["D_TEXT-P"] = clean_text(text_elem.text)
        except NoSuchElementException:
            pass

        # Expiry detection
        try:
            if (article.find_elements(By.CSS_SELECTOR, "img[src*='clock.svg']") or 
                article.find_elements(By.XPATH, ".//span[contains(@class,'tooltiptext') and contains(text(),'Expiring')]")):
                data["G_EXPIRY"] = "⏳"
        except:
            pass

        # Reply count and status
        reply_count = extract_reply_count(article)
        data["H_REPLY"] = str(reply_count) if reply_count > 0 else "0"
        data["I_R-ON"] = extract_reply_status(article)

        # Comments and analytics
        try:
            comments = article.find_elements(By.CSS_SELECTOR, "[itemprop='comment']")[:3]
            comment_data = []
            comment_links = []
            commenter_names = []
            
            for comment in comments:
                try:
                    author_link = comment.find_element(By.CSS_SELECTOR, "[itemprop='author'] a")
                    text_elem = comment.find_element(By.CSS_SELECTOR, "[itemprop='text']")
                    
                    comment_text = clean_text(text_elem.text)
                    author_url = to_abs_url(author_link.get_attribute("href"))
                    commenter_name = clean_text(author_link.text)
                    
                    comment_data.append(comment_text)
                    comment_links.append(author_url)
                    commenter_names.append(commenter_name)
                except:
                    comment_data.append("")
                    comment_links.append("")
                    commenter_names.append("")
            
            # Fill comment columns
            data["J_COM1"] = comment_data[0] if len(comment_data) > 0 else ""
            data["K_COM2"] = comment_data[1] if len(comment_data) > 1 else ""
            data["L_COM3"] = comment_data[2] if len(comment_data) > 2 else ""
            data["O_COM1-L"] = comment_links[0] if len(comment_links) > 0 else ""
            data["P_COM2-L"] = comment_links[1] if len(comment_links) > 1 else ""
            data["Q_COM3-L"] = comment_links[2] if len(comment_links) > 2 else ""
            
            # Update analytics
            author = data["B_NICKNAME"]
            if author:
                today = datetime.now().strftime("%Y-%m-%d")
                analytics_data[author]['total_posts'] += 1
                analytics_data[author]['gender'] = data["E_GENDER"]
                analytics_data[author]['city'] = data["F_CITY"]
                analytics_data[author]['daily_activity'][today] += 1
                
                for commenter in commenter_names:
                    if commenter:
                        analytics_data[author]['commenters'][commenter] += 1
                        analytics_data[commenter]['commented_on'].add(author)
                        analytics_data[commenter]['total_comments'] += 1
        except:
            pass

        # Post link
        if data["D_TEXT-P"]:
            post_hash = text_hash(data["D_TEXT-P"])
            data["N_POST-L"] = f"{BASE}/comments/text/{post_hash}"
            
            if data["B_NICKNAME"]:
                analytics_data[data["B_NICKNAME"]]['posts_links'].append(data["N_POST-L"])

    except Exception as e:
        logger.error(f"Error extracting data: {e}")

    return data

# ----------------- Authentication -----------------
def login(driver):
    """Login to DamaDam"""
    logger.info("Attempting login...")
    
    if not USERNAME or not PASSWORD:
        logger.error("Username or password not provided in environment variables")
        return False
    
    try:
        driver.get(LOGIN_URL)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "nick")))
        
        driver.find_element(By.ID, "nick").send_keys(USERNAME)
        driver.find_element(By.ID, "pass").send_keys(PASSWORD)
        driver.find_element(By.CSS_SELECTOR, "form button, form input[type='submit']").click()
        
        time.sleep(3)
        
        if "login" not in driver.current_url.lower():
            logger.info("Login successful!")
            return True
        else:
            logger.error("Login failed - check credentials")
            return False
    except Exception as e:
        logger.error(f"Login process failed: {e}")
        return False

# ----------------- Data Storage -----------------
def get_existing_posts_sheets(worksheet):
    """Get existing posts from Google Sheets"""
    existing = {}
    try:
        all_values = worksheet.get_all_records()
        for idx, row in enumerate(all_values, start=2):
            text = row.get("D_TEXT-P", "").strip()
            if text:
                existing[text_hash(text)] = {"row": idx, "data": row}
        logger.info(f"Found {len(existing)} existing posts")
        stats.api_call()
    except Exception as e:
        logger.error(f"Error reading from Google Sheets: {e}")
    return existing

def update_batch_in_sheets(worksheet, batch_data, existing_posts):
    """Update batch data in sheets"""
    if not worksheet:
        return False
    
    try:
        new_posts = 0
        updated_posts = 0
        insert_rows = []
        
        logger.info(f"Processing {len(batch_data)} posts...")
        
        # Separate new posts from updates
        for data in batch_data:
            text = data.get("D_TEXT-P", "").strip()
            if not text:
                continue
                
            hash_key = text_hash(text)

            # >>> CHANGE: Inject SCRAPE_TIME at the moment of preparing the row for insertion.
            # This ensures the sheet's first column contains exact time when we pushed the row.
            data["SCRAPE_TIME"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Build row values following HEADERS order
            row_values = [data.get(h, "") for h in HEADERS]
            
            if hash_key not in existing_posts:
                insert_rows.append(row_values)
                new_posts += 1
            # For GitHub Actions, we'll focus on new posts only to keep it simple
        
        # Insert new rows at the top (batch insert for speed)
        if insert_rows:
            logger.info(f"Inserting {len(insert_rows)} new posts (batch)...")
            try:
                # insert_rows inserts multiple rows at once; position at row=2 keeps header on top
                worksheet.insert_rows(insert_rows, row=2, value_input_option="USER_ENTERED")
            except Exception as e:
                # Fallback: some gspread versions may not support insert_rows with value_input_option.
                logger.warning(f"Batch insert failed, falling back to row-by-row insert: {e}")
                for row_data in insert_rows:
                    worksheet.insert_row(row_data, index=2, value_input_option="USER_ENTERED")
            stats.api_call()
        
        logger.info(f"Batch complete: {new_posts} new posts added")
        return True
        
    except Exception as e:
        logger.error(f"Batch update failed: {e}")
        return False

# ----------------- Main Scraping Logic -----------------
def scrape_batch(driver, page_num, profiles_data):
    """Scrape a single page with detailed logging"""
    url = START_URL_TEMPLATE.format(page=page_num)
    logger.info(f"Scraping page {page_num}: {url}")
    
    try:
        logger.info(f"Loading URL: {url}")
        driver.get(url)
        human_delay()
        
        logger.info("Waiting for articles to load...")
        WebDriverWait(driver, PAGE_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "article.mbl"))
        )
        logger.info("Articles container found, waiting for content...")
        time.sleep(1)  # Wait for lazy loading
        
    except TimeoutException:
        logger.warning(f"Timeout on page {page_num} - no articles found")
        logger.info(f"Current URL: {driver.current_url}")
        logger.info(f"Page title: {driver.title}")
        return []
    except Exception as e:
        logger.error(f"Error loading page {page_num}: {e}")
        logger.info(f"Current URL: {driver.current_url}")
        return []
    
    # Find articles
    articles = driver.find_elements(By.CSS_SELECTOR, "article.mbl.bas-sh, article.mbl")
    if not articles:
        logger.warning(f"No articles found on page {page_num}")
        logger.info(f"Current URL: {driver.current_url}")
        logger.info(f"Page title: {driver.title}")
        
        # Try alternative selectors
        all_articles = driver.find_elements(By.CSS_SELECTOR, "article")
        logger.info(f"Found {len(all_articles)} total article elements")
        
        # Log page source info for debugging
        page_source_snippet = driver.page_source[:500] if driver.page_source else "No page source"
        logger.info(f"Page source snippet: {page_source_snippet}")
        
        return []
    
    logger.info(f"Found {len(articles)} articles on page {page_num}")
    
    batch_data = []
    for idx, article in enumerate(articles, 1):
        try:
            logger.info(f"Processing article {idx}/{len(articles)}")
            data = extract_post_data(article, page_num, profiles_data)
            
            if data.get("D_TEXT-P"):
                batch_data.append(data)
                logger.info(f"Article {idx}: Successfully extracted post data")
                # Log first few words of the post for verification
                post_text = data.get("D_TEXT-P", "")
                preview = post_text[:50] + "..." if len(post_text) > 50 else post_text
                logger.info(f"Article {idx} preview: {preview}")
            else:
                logger.warning(f"Article {idx}: No text content found")
            
            if idx % 5 == 0:  # Progress logging
                logger.info(f"Processed {idx}/{len(articles)} posts on page {page_num}")
            
            human_delay()
        except Exception as e:
            logger.error(f"Error processing article {idx}: {e}")
            stats.error()
    
    logger.info(f"Page {page_num} complete: {len(batch_data)} valid posts extracted from {len(articles)} articles")
    return batch_data

def run_scraper():
    """Main scraper function"""
    logger.info("====== DamaDam Scraper Started ======")
    logger.info(f"Configuration: {MAX_PAGES} pages, {BATCH_SIZE} batch size")
    logger.info(f"Target URL template: {START_URL_TEMPLATE}")
    logger.info(f"Username configured: {'Yes' if USERNAME else 'No'}")
    logger.info(f"Password configured: {'Yes' if PASSWORD else 'No'}")
    
    # Reset analytics for this run
    global analytics_data
    analytics_data.clear()
    analytics_data = defaultdict(lambda: {
        'total_posts': 0,
        'total_comments': 0,
        'commented_on': set(),
        'commenters': defaultdict(int),
        'posts_links': [],
        'gender': '',
        'city': '',
        'daily_activity': defaultdict(int)
    })
    
    # Connect to Google Sheets
    worksheet = connect_google_sheet()
    if not worksheet:
        logger.error("Cannot proceed without Google Sheets access")
        return
    
    # Load profiles and existing posts
    profiles_data = load_profiles_data(worksheet)
    existing_posts = get_existing_posts_sheets(worksheet)
    
    driver = None
    try:
        # Setup browser and login
        logger.info("Initializing Chrome driver...")
        driver = setup_driver()
        
        logger.info("Attempting login to DamaDam...")
        if not login(driver):
            logger.warning("Login failed - continuing with limited access")
        else:
            logger.info("Login successful - proceeding with authenticated scraping")
        
        all_scraped_data = []
        total_new = 0
        total_updated = 0
        
        # Process each page
        for page in range(1, MAX_PAGES + 1):
            try:
                logger.info(f"Starting to scrape page {page}/{MAX_PAGES}")
                batch_data = scrape_batch(driver, page, profiles_data)
                
                if not batch_data:
                    logger.warning(f"No data extracted from page {page} - this might indicate a problem")
                    continue
                
                logger.info(f"Successfully extracted {len(batch_data)} posts from page {page}")
                all_scraped_data.extend(batch_data)
                
                # Count new posts
                new_count = 0
                for data in batch_data:
                    text = data.get("D_TEXT-P", "")
                    if text and text_hash(text) not in existing_posts:
                        new_count += 1
                
                logger.info(f"Page {page}: {new_count} new posts identified")
                
                # Save batch to Google Sheets
                logger.info(f"Saving {len(batch_data)} posts to Google Sheets...")
                success = update_batch_in_sheets(worksheet, batch_data, existing_posts)
                if success:
                    stats.add_posts(new_count, 0)  # For simplicity, treating all as new
                    total_new += new_count
                    logger.info(f"Page {page}: {new_count} new posts saved successfully")
                else:
                    logger.error(f"Failed to save data for page {page}")
                
                human_delay()
                
            except Exception as e:
                logger.error(f"Page {page} processing failed: {e}")
                stats.error()
        
        # Update analytics
        stats.analytics_users = len(analytics_data)
        update_analytics_sheet(worksheet)
        
        # Save CSV backup
        if all_scraped_data:
            export_csv(all_scraped_data)
        
        # Final summary
        duration = stats.duration()
        logger.info("====== Scraping Complete ======")
        logger.info(f"Results: {total_new} new posts, {stats.analytics_users} users analyzed")
        logger.info(f"Duration: {str(duration).split('.')[0]}")
        logger.info(f"Success rate: {stats.success_rate():.1f}%")
        logger.info(f"Speed: {stats.posts_per_min():.1f} posts/min")
        
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
    finally:
        if driver:
            driver.quit()
            logger.info("Browser closed")

if __name__ == "__main__":
    run_scraper()
