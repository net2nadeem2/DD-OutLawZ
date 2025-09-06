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
import json
import os
import random
from datetime import datetime
import re
import logging

# ==== NEW: Fancy terminal display ====
import colorama
from colorama import Fore, Style
colorama.init(autoreset=True)

# 🚀 Clean logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | 🚀 %(message)s",
    datefmt="[%H:%M:%S]"
)
logger = logging.getLogger(__name__)


# Google Sheets (optional - install if needed)
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    GOOGLE_SHEETS_AVAILABLE = True
    logger.info("Google Sheets integration available")
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False
    logger.info("Google Sheets integration not available (install gspread and oauth2client)")

# === CONFIGURATION ===
LOGIN_URL = "https://damadam.pk/login/"
ONLINE_USERS_URL = "https://damadam.pk/online_kon/"
USERNAME = "0utLawZ"
PASSWORD = "@Brandex1999"
COOKIES_FILE = "damadam_cookies.json"
CSV_OUTPUT = "damadam_profiles.csv"

# Google Sheets Configuration (optional)
SERVICE_JSON = "damadam-scraper-credentials.json"  # Your service account file
SHEET_URL = "https://docs.google.com/spreadsheets/d/1WGd1EZKGoJMNzPgOUpuVmOuA7qOR-bLxyPoG-6dtbnY/edit"  # Your Google Sheet URL
EXPORT_TO_GOOGLE_SHEETS = True  # Set to False to skip Google Sheets

# Delays (in seconds)
MIN_DELAY = 1
MAX_DELAY = 1
LOGIN_DELAY = 3

# === CSV FIELDNAMES ===
FIELDNAMES = [
    "DATE", "TIME", "NICKNAME", "TAGS", "CITY", "GENDER", "MARRIED",
    "AGE", "JOINED", "FOLLOWERS", "POSTS", "PLINK", "PIMAGE", "INTRO"
]

# ==== NEW: Stats + Display helpers ====
class ScrapingStats:
    def __init__(self):
        self.reset()

    def reset(self):
        self.session_start_time = datetime.now()
        self.total_users = 0
        self.current_user_index = 0
        self.successful_scrapes = 0
        self.errors = 0
        self.phase = "Initializing"

    def success(self):
        self.successful_scrapes += 1

    def error(self):
        self.errors += 1

    def duration(self):
        return datetime.now() - self.session_start_time

    def success_rate(self):
        total_done = max(self.current_user_index, 1)
        return (self.successful_scrapes / total_done) * 100

    def speed_per_min(self):
        mins = max(self.duration().total_seconds() / 60.0, 1e-9)
        return self.successful_scrapes / mins

stats = ScrapingStats()

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header():
    clear_screen()
    print(Fore.CYAN + "="*70 + Style.RESET_ALL)
    print(Fore.YELLOW + "🚀 DamaDam Scraper — Live Session" + Style.RESET_ALL)
    print(Fore.CYAN + "="*70 + Style.RESET_ALL)

def log_message(msg, level="INFO"):
    colors = {
        "INFO": Fore.WHITE,
        "SUCCESS": Fore.GREEN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "DEBUG": Fore.CYAN,
    }
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"{colors.get(level, Fore.WHITE)}[{ts}] {level}: {msg}{Style.RESET_ALL}")

def print_progress():
    total = max(stats.total_users, 1)
    pct = (stats.current_user_index / total) * 100
    bar_len = 34
    filled = int(bar_len * pct / 100)
    bar = "█"*filled + "░"*(bar_len - filled)
    dur = str(stats.duration()).split(".")[0]
    print("")
    print(f"{Fore.MAGENTA}Phase       : {Fore.GREEN}{stats.phase}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Progress    : [{Fore.BLUE}{bar}{Style.RESET_ALL}] "
          f"{stats.current_user_index}/{stats.total_users} ({pct:.1f}%)")
    print(f"{Fore.MAGENTA}Speed       : {Fore.YELLOW}{stats.speed_per_min():.1f} profiles/min{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}SuccessRate : {Fore.GREEN}{stats.success_rate():.1f}%{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Errors      : {Fore.RED}{stats.errors}{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}Duration    : {Fore.CYAN}{dur}{Style.RESET_ALL}")
    print(Fore.CYAN + "-"*70 + Style.RESET_ALL)

# === SETUP BROWSER ===
def setup_browser():
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    driver.maximize_window()
    return driver

# === COOKIE MANAGEMENT ===
def save_cookies(driver, file_path):
    with open(file_path, "w") as f:
        json.dump(driver.get_cookies(), f)
    logger.info("Cookies saved")

def load_cookies(driver, file_path):
    if os.path.exists(file_path):
        driver.get("https://damadam.pk")
        with open(file_path, "r") as f:
            cookies = json.load(f)
        for cookie in cookies:
            try:
                driver.add_cookie(cookie)
            except Exception as e:
                logger.warning(f"Cookie load error: {e}")
        driver.refresh()
        logger.info("Cookies loaded")
        return True
    return False

#=== Dashboard function =========
def generate_dashboard(scraped_profiles):
    if not scraped_profiles:
        logger.info("No profiles scraped. Dashboard not generated.")
        return

    try:
        total_profiles = len(scraped_profiles)
        unique_nicks = len({p['NICKNAME'] for p in scraped_profiles})
        male_count = sum(1 for p in scraped_profiles if p['GENDER'] == 'Male')
        female_count = sum(1 for p in scraped_profiles if p['GENDER'] == 'Female')

        logger.info(f"\n--- Dashboard ---")
        logger.info(f"Total Profiles: {total_profiles}")
        logger.info(f"Unique Profiles: {unique_nicks}")
        logger.info(f"Male: {male_count}")
        logger.info(f"Female: {female_count}")

    except Exception as e:
        logger.error(f"Error generating dashboard: {e}")

# === LOGIN FUNCTION ===
def login(driver):
    logger.info("Logging in...")
    driver.get(LOGIN_URL)
    time.sleep(2)

    try:
        nick_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "nick"))
        )
        nick_input.clear()
        nick_input.send_keys(USERNAME)

        pass_input = driver.find_element(By.ID, "pass")
        pass_input.clear()
        pass_input.send_keys(PASSWORD)

        login_btn = driver.find_element(By.CSS_SELECTOR, "form button")
        login_btn.click()

        time.sleep(LOGIN_DELAY)

        if "login" not in driver.current_url.lower():
            logger.info("Login successful")
            save_cookies(driver, COOKIES_FILE)
            return True
        else:
            logger.warning("Login failed")
            return False

    except Exception as e:
        logger.error(f"Login error: {e}")
        return False

# === GET ONLINE USERS ===
def get_online_users(driver):
    logger.info("Getting online users list...")
    driver.get(ONLINE_USERS_URL)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li bdi"))
        )

        nick_elements = driver.find_elements(By.CSS_SELECTOR, "li bdi")
        nicknames = [elem.text.strip() for elem in nick_elements if elem.text.strip()]
        
        logger.info(f"Found {len(nicknames)} online users")
        return nicknames
    except Exception as e:
        logger.error(f"Error getting users: {e}")
        return []

# === DATA CLEANING FUNCTIONS ===
def clean_intro(intro):
    if not intro or intro.strip().lower() in ['not set', 'no set']:
        return ''
    return intro.strip()

def clean_city(city):
    if not city or city.strip().lower() in ['no city', 'not set']:
        return ''
    return city.strip()

def clean_generic_field(value):
    if not value or value.strip().lower() in ['not set', 'no set']:
        return ''
    return value.strip()

def clean_joined(joined):
    if not joined or joined.strip().lower() in ['not set', 'no set']:
        return ''
    
    numbers = re.findall(r'\d+', joined)
    if numbers:
        return ', '.join(numbers)
    return joined.strip()

def clean_posts_count(posts):
    if not posts:
        return ''
    return posts.replace('+', '').strip()

def clean_data(value):
    if not value:
        return ''
    value = value.strip()
    value = value.replace('Not set', '').replace('No city', '').replace('+', '')
    return value if value else ''

def create_profile_link(nickname):
    return f"https://damadam.pk/users/{nickname}"

# === SCRAPE SINGLE PROFILE ===
def scrape_profile(driver, nickname):
    profile_url = f"https://damadam.pk/users/{nickname}/"
    logger.info(f"Scraping: {nickname}")
    
    try:
        driver.get(profile_url)
        
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1.cxl.clb.lsp"))
        )

        now = datetime.now()
        current_date = now.strftime("%d-%b-%Y")
        current_time = now.strftime("%I:%M %p")

        # Fixed field structure
        profile_data = {
            'DATE': current_date,
            'TIME': current_time,
            'NICKNAME': nickname,
            'TAGS': '',        # blank for now
            'CITY': '',
            'GENDER': '',
            'MARRIED': '',
            'AGE': '',
            'JOINED': '',
            'FOLLOWERS': '',
            'POSTS': '',
            'PLINK': profile_url,
            'PIMAGE': '',
            'INTRO': ''
        }

        # Intro
        try:
            intro_elem = driver.find_element(By.CSS_SELECTOR, ".ow span.nos")
            profile_data['INTRO'] = clean_intro(intro_elem.text.strip())
        except Exception as e:
            logger.warning(f"Could not get intro: {e}")

        # Extract profile fields
        field_mapping = {
            'City:': 'CITY',
            'Gender:': 'GENDER', 
            'Married:': 'MARRIED',
            'Age:': 'AGE',
            'Joined:': 'JOINED'
        }

        for field_name, data_key in field_mapping.items():
            try:
                xpath = f"//b[contains(text(), '{field_name}')]/following-sibling::span[1]"
                elem = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, xpath))
                )
                value = elem.text.strip().replace('\xa0', ' ')

                if data_key == 'CITY':
                    value = clean_city(value)
                elif data_key == 'JOINED':
                    value = clean_joined(value)
                else:
                    value = clean_generic_field(value)

                profile_data[data_key] = value
            except Exception as e:
                logger.warning(f"Could not find {field_name}: {e}")
                continue

        # Followers (normal followers, not just verified)
        try:
            followers_elem = driver.find_element(By.CSS_SELECTOR, "span.cl.sp.clb")
            followers_text = followers_elem.text
            match = re.search(r'(\d+)', followers_text)
            if match:
                profile_data['FOLLOWERS'] = match.group(1)
        except Exception as e:
            logger.warning(f"Could not get followers: {e}")

        # Posts count
        try:
            posts_elem = driver.find_element(By.CSS_SELECTOR, "a[href*='/profile/public/'] button div:first-child")
            posts_count = posts_elem.text.strip()
            profile_data['POSTS'] = clean_posts_count(posts_count)
        except Exception as e:
            logger.warning(f"Could not get posts count: {e}")

        # Profile image
        try:
            img_elem = driver.find_element(By.CSS_SELECTOR, "img[src*='avatar-imgs']")
            profile_data['PIMAGE'] = img_elem.get_attribute('src')
        except Exception as e:
            logger.warning(f"Could not get profile image: {e}")

        logger.info(
            f"Scraped {nickname}: {profile_data['CITY']} | {profile_data['AGE']} | Posts: {profile_data['POSTS']}"
        )
        return profile_data

    except Exception as e:
        logger.error(f"Error scraping {nickname}: {e}")
        return None


# === FINAL EXPORT TO GOOGLE SHEETS FUNCTION (CLEAN + TAGGING) ===
def export_to_google_sheets(new_data):
    if not GOOGLE_SHEETS_AVAILABLE:
        logger.warning("⚠️ Google Sheets libraries not available. Skipping export.")
        return False

    if not EXPORT_TO_GOOGLE_SHEETS:
        logger.info("ℹ️ Google Sheets export disabled in config.")
        return False

    try:
        logger.info("📤 Exporting to Google Sheets...")

        # Auth
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_JSON, scope)
        client = gspread.authorize(creds)

        # Open or create workbook
        try:
            book = client.open("DD_Profiles")
        except:
            book = client.create("DD_Profiles")

        # Open or create worksheet
        try:
            worksheet = book.worksheet("Profiles")
        except:
            worksheet = book.add_worksheet(title="Profiles", rows=1000, cols=20)

        # --- TagList Sheet Load ---
        try:
            taglist_sheet = book.worksheet("TagList")
            tag_data = taglist_sheet.get_all_values()

            list1 = {row[0].strip() for row in tag_data[1:] if row[0]}  # Col A
            list2 = {row[1].strip() for row in tag_data[1:] if len(row) > 1 and row[1]}  # Col B
            list3 = {row[2].strip() for row in tag_data[1:] if len(row) > 2 and row[2]}  # Col C
        except:
            list1, list2, list3 = set(), set(), set()
            logger.warning("⚠️ TagList sheet missing or empty.")

        def get_tags_for_nick(nick):
            tags = []
            if nick in list1:
                tags.append("🐾")  # List1
            if nick in list2:
                tags.append("🌟")  # List2
            if nick in list3:
                tags.append("🔖")  # List3
            return ",".join(tags) if tags else ""

        # Headers
        headers = [
            "DATE", "TIME", "NICKNAME", "TAGS", "CITY", "GENDER", "MARRIED",
            "AGE", "JOINED", "FOLLOWERS", "POSTS", "PLINK", "PIMAGE", "INTRO", "SCOUNT"
        ]

        # Setup headers if not correct
        existing_headers = worksheet.row_values(1)
        if existing_headers != headers:
            worksheet.clear()
            worksheet.append_row(headers, value_input_option="USER_ENTERED")

            # Header formatting
            worksheet.format("A1:O1", {
                "backgroundColor": {"red": 0.0, "green": 0.7647, "blue": 0.8627},  # #00c3dc
                "textFormat": {
                    "bold": True,
                    "fontFamily": "Lexend"
                },
                "verticalAlignment": "MIDDLE",
                "wrapStrategy": "CLIP"
            })

            worksheet.freeze(1)  # Freeze header row
            logger.info("✅ Headers initialized & formatted.")

        # Prepare new rows
        rows_to_add = []
        for profile in new_data:
            nickname = profile.get("NICKNAME", "")
            row = [
                profile.get("DATE", ""),
                profile.get("TIME", ""),
                nickname,
                get_tags_for_nick(nickname),  # 🔹 Col D (Tags)
                profile.get("CITY", ""),
                profile.get("GENDER", ""),
                profile.get("MARRIED", ""),
                profile.get("AGE", ""),
                profile.get("JOINED", ""),
                profile.get("FOLLOWERS", ""),
                profile.get("POSTS", ""),
                profile.get("PLINK", ""),
                profile.get("PIMAGE", ""),
                clean_data(profile.get("INTRO", "")),
            ]
            rows_to_add.append(row)

        # Existing nicknames
        existing_nicks = worksheet.col_values(3)

        for row in rows_to_add:
            nickname = row[2]
            if nickname in existing_nicks:
                # Duplicate → update Seen Count
                row_index = existing_nicks.index(nickname) + 1
                count_cell = f"O{row_index}"  # Col O = SEEN_COUNT
                old_value = worksheet.acell(count_cell).value
                new_value = str(int(old_value) + 1) if old_value and old_value.isdigit() else "1"
                worksheet.update_acell(count_cell, new_value)
                logger.info(f"🔁 Duplicate: {nickname} → Seen Count updated to {new_value}")
            else:
                # New profile → append row with Seen Count = 1
                row_with_count = row + ["1"]
                worksheet.append_row(row_with_count, value_input_option="USER_ENTERED")
                logger.info(f"🆕 Added new profile: {nickname} (Seen Count=1)")

        return True

    except Exception as e:
        logger.error(f"❌ Error exporting to Google Sheets: {e}")
        return False

# === MAIN EXECUTION ===
def main():
    driver = None
    try:
        # Headless mode setup
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        
        # Display boot
        stats.reset()
        stats.phase = "Authenticating"
        print_header()
        print_progress()

        # Login or load cookies
        if not load_cookies(driver, COOKIES_FILE):
            if not login(driver):
                logger.error("Failed to login. Exiting.")
                return
        
        # Scrape online users and profiles
        stats.phase = "Fetching Online Users"
        print_header(); print_progress()
        online_users = get_online_users(driver)
        stats.total_users = len(online_users)
        if not online_users:
            logger.error("No online users found. Exiting.")
            return
        
        # Prepare CSV and Google Sheets export
        csv_exists = os.path.exists(CSV_OUTPUT)
        scraped_profiles = []

        with open(CSV_OUTPUT, 'a', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
            if not csv_exists:
                writer.writeheader()
            
            stats.phase = "Scraping Profiles"
            for i, nickname in enumerate(online_users, 1):
                stats.current_user_index = i
                print_header(); print_progress()
                log_message(f"Scraping [{i}/{len(online_users)}]: {nickname}", "INFO")
                
                profile_data = scrape_profile(driver, nickname)
                if profile_data:
                    writer.writerow(profile_data)
                    scraped_profiles.append(profile_data)
                    stats.success()
                else:
                    stats.error()
                
                # Export in batches to avoid quota issues
                if i % 10 == 0 and scraped_profiles:
                    log_message("Exporting batch to Google Sheets...", "DEBUG")
                    export_to_google_sheets(scraped_profiles[-10:])
                    time.sleep(5)  # Add delay after exporting
                
                # Random delay between requests
                delay = random.randint(MIN_DELAY, MAX_DELAY)
                log_message(f"Waiting {delay} seconds...", "DEBUG")
                time.sleep(delay)
            
            # Export any remaining profiles
            if scraped_profiles:
                export_to_google_sheets(scraped_profiles)
        
        stats.phase = "Completed"
        print_header(); print_progress()
        logger.info(f"\nScraping completed! Data saved to: {CSV_OUTPUT}")
        logger.info(f"Total profiles scraped: {len(scraped_profiles)}")

    except KeyboardInterrupt:
        stats.phase = "Interrupted"
        print_header(); print_progress()
        log_message("🛑 Scraping interrupted by user (Ctrl+C). Exiting cleanly...", "WARNING")
    except Exception as e:
        stats.phase = "Crashed"
        print_header(); print_progress()
        logger.error(f"\nUnexpected error: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
            logger.info("Browser closed.")

#======loop======
def run_loop():
    while True:
        logger.info("\nStarting new scraping loop...")
        main()
        logger.info("Loop completed. Waiting for next iteration...")
        try:
            for s in range(8*60, 0, -1):
                print(f"{Fore.YELLOW}Next run in {s} sec... (Ctrl+C to stop){Style.RESET_ALL}", end='\r')
                time.sleep(1)
            print(" "*60, end="\r")
        except KeyboardInterrupt:
            log_message("🛑 Loop stopped by user (Ctrl+C). Goodbye!👋", "WARNING")
            break

if __name__ == "__main__":
    run_loop()

