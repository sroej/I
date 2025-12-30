import aiohttp
import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import asyncio
import time
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import json
import logging
import os
import pytz
import hashlib
from io import StringIO
import firebase_admin
from firebase_admin import credentials, db
import shutil

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Firebase setup
cred = credentials.Certificate("smsotp-f8aa7-firebase-adminsdk-fbsvc-edf7c62e28.json")
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://smsotp-f8aa7-default-rtdb.firebaseio.com'
})
numbers_ranges_ref = db.reference('numbers_ranges')
seen_sms_ref = db.reference('seen_sms')

# Telegram bot setup
BOT_TOKEN = "8575184957:AAEZ4Wz-NQDQVc2SNqjpjmhlU56sMsVRih0"
CHAT_ID = "--1002621856407"
ADMIN_CHAT_ID = "--1002621856407"
ADMIN_USER_IDS = [7008926454]
ADMINS_FILE = "admins.json"
bot = telegram.Bot(token=BOT_TOKEN)

# SMS service configuration
LOGIN_URL = "https://www.ivasms.com/login"
SMS_LIST_URL = "https://www.ivasms.com/portal/sms/received/getsms/number"
SMS_DETAILS_URL = "https://www.ivasms.com/portal/sms/received/getsms/number/sms"
RETURN_ALL_URL = "https://www.ivasms.com/portal/numbers/return/allnumber/bluck"
EMAIL = "ninjaxxdenki@gmail.com"
PASSWORD = "denkidev4"

# SMS headers
SMS_HEADERS = {
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.ivasms.com/portal/sms/received",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Accept": "text/html, */*; q=0.01",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Track processed SMS
seen_sms = set()
TIME_THRESHOLD = timedelta(minutes=60)

# Pending ranges for /add_range
pending_ranges = {}  # {chat_id: {"range": str, "numbers": list}}

# Debug log directory
DEBUG_LOG_DIR = "debug_logs"
if not os.path.exists(DEBUG_LOG_DIR):
    os.makedirs(DEBUG_LOG_DIR)

# Bot state
bot_running = True

# Admin persistence
def load_admins():
    global ADMIN_USER_IDS
    if os.path.exists(ADMINS_FILE):
        try:
            with open(ADMINS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                ADMIN_USER_IDS = data.get("admin_ids", ADMIN_USER_IDS)
            logger.info(f"Loaded admin IDs: {ADMIN_USER_IDS}")
        except Exception as e:
            logger.error(f"Error loading admins file: {e}")
    else:
        logger.info("Admins file not found, using default admins")

def save_admins():
    try:
        with open(ADMINS_FILE, "w", encoding="utf-8") as f:
            json.dump({"admin_ids": ADMIN_USER_IDS}, f, ensure_ascii=False, indent=4)
        logger.info("Admin IDs saved")
    except Exception as e:
        logger.error(f"Error saving admins file: {e}")

load_admins()

# Auto-clean bot.log and debug_logs
async def auto_clean_log():
    while True:
        try:
            # Clean bot.log
            if os.path.exists('bot.log'):
                with open('bot.log', 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                if len(lines) > 1000:
                    with open('bot.log', 'w', encoding='utf-8') as f:
                        f.writelines(lines[-1000:])
                    logger.info("Truncated bot.log to last 1000 lines")
                else:
                    logger.debug("bot.log under 1000 lines, no truncation needed")

            # Clean debug_logs directory
            if os.path.exists(DEBUG_LOG_DIR):
                current_time = time.time()
                for filename in os.listdir(DEBUG_LOG_DIR):
                    file_path = os.path.join(DEBUG_LOG_DIR, filename)
                    try:
                        file_mtime = os.path.getmtime(file_path)
                        # Delete files older than 1 hour (3600 seconds)
                        if current_time - file_mtime > 3600:
                            os.remove(file_path)
                            logger.info(f"Deleted old debug log: {file_path}")
                    except Exception as e:
                        logger.error(f"Error deleting debug log {file_path}: {str(e)}")
                # Remove directory if empty
                try:
                    os.rmdir(DEBUG_LOG_DIR)
                    logger.info("Removed empty debug_logs directory")
                    os.makedirs(DEBUG_LOG_DIR)  # Recreate directory
                except OSError:
                    pass  # Directory not empty
            else:
                os.makedirs(DEBUG_LOG_DIR)
                logger.info("Recreated debug_logs directory")

            await asyncio.sleep(120)  # Run every 2 minutes
        except Exception as e:
            logger.error(f"Error in auto_clean_log: {str(e)}")
            await asyncio.sleep(120)

async def send_startup_alert(chat_id=CHAT_ID):
    try:
        current_time = datetime.now(pytz.timezone('Asia/Dhaka')).strftime("%d-%m-%Y %H:%M:%S")
        startup_msg = (
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "🌟 **BOT INITIALIZED** 🌟\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"🕒 **Started At**: {current_time} (Asia/Dhaka)\n"
            "📡 **Status**: Operational\n"
            "🔧 **Service Provider**: Seven1Tel / IVASMS\n"
            "🔑 **Functionality**: OTP Detection Active\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "👨‍💻 **Developed by**: BAPPY KHAN\n"
            "━━━━━━━━━━━━━━━━━━━━━"
        )
        keyboard = [
            [InlineKeyboardButton("📞 Contact Owner", url="https://t.me/jadenafrix")],
            [InlineKeyboardButton("📢 Join Backup Channel", url="https://t.me/mrafrix")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await bot.send_message(
            chat_id=chat_id,
            text=startup_msg,
            parse_mode="Markdown",
            reply_markup=reply_markup
        )
        logger.info("Startup alert sent")
    except Exception as e:
        logger.error(f"Error sending startup alert: {str(e)}")
        await send_telegram_message(f"❌ Error sending startup alert: {str(e)}", chat_id)

def initialize_firebase_data():
    try:
        if not numbers_ranges_ref.get():
            numbers_ranges_ref.set({"ranges": []})
            logger.info("Initialized numbers_ranges in Firebase")
        if not seen_sms_ref.get():
            seen_sms_ref.set({"sms_ids": [], "last_updated": datetime.now().isoformat()})
            logger.info("Initialized seen_sms in Firebase")
    except Exception as e:
        logger.error(f"Error initializing Firebase data: {str(e)}")
        asyncio.create_task(send_telegram_message(f"❌ Error initializing Firebase data: {str(e)}", CHAT_ID))

def load_seen_sms():
    global seen_sms
    initialize_firebase_data()
    try:
        data = seen_sms_ref.get() or {"sms_ids": []}
        seen_sms = set(data.get("sms_ids", []))
        logger.info(f"Loaded {len(seen_sms)} SMS IDs from Firebase")
    except Exception as e:
        logger.error(f"Error loading seen_sms from Firebase: {str(e)}")
        seen_sms = set()

def save_seen_sms():
    try:
        seen_sms_ref.set({
            "sms_ids": list(seen_sms),
            "last_updated": datetime.now().isoformat()
        })
        logger.info("Saved SMS IDs to Firebase")
    except Exception as e:
        error_msg = f"❌ Error saving seen_sms to Firebase: {str(e)}"
        logger.error(error_msg)
        asyncio.create_task(send_telegram_message(error_msg, CHAT_ID))

def load_numbers_ranges():
    initialize_firebase_data()
    try:
        data = numbers_ranges_ref.get() or {"ranges": []}
        numbers = []
        ranges = []
        for range_item in data.get("ranges", []):
            range_value = str(range_item.get("range", "")).strip()
            range_numbers = range_item.get("numbers", [])
            if range_value and range_value not in ranges:
                ranges.append(range_value)
            for num in range_numbers:
                if num not in numbers:
                    numbers.append(str(num))
        logger.info(f"Loaded {len(numbers)} numbers and {len(ranges)} ranges from Firebase")
        return numbers, ranges
    except Exception as e:
        error_msg = f"❌ Error loading from Firebase: {str(e)}"
        logger.error(error_msg)
        asyncio.create_task(send_telegram_message(error_msg, CHAT_ID))
        return [], []

def save_numbers_ranges(ranges_data):
    try:
        if not isinstance(ranges_data, list):
            logger.error(f"Invalid ranges_data type: {type(ranges_data)}")
            raise ValueError("ranges_data must be a list")
        
        cleaned_ranges = []
        seen_ranges = set()
        for range_item in ranges_data:
            if not isinstance(range_item, dict) or "range" not in range_item or "numbers" not in range_item:
                logger.warning(f"Skipping invalid range item: {range_item}")
                continue
            range_value = str(range_item["range"]).strip()
            if range_value in seen_ranges:
                logger.warning(f"Duplicate range {range_value}, skipping")
                continue
            seen_ranges.add(range_value)
            numbers = [str(num).strip() for num in set(range_item.get("numbers", []))]
            cleaned_ranges.append({"range": range_value, "numbers": numbers})
        
        numbers_ranges_ref.set({"ranges": cleaned_ranges})
        logger.info(f"Saved {len(cleaned_ranges)} ranges to Firebase")
    except Exception as e:
        error_msg = f"❌ Error saving to Firebase: {str(e)}"
        logger.error(error_msg)
        asyncio.create_task(send_telegram_message(error_msg, CHAT_ID))

async def return_numbers(session, csrf_token, range_value=None):
    try:
        headers = SMS_HEADERS.copy()
        headers["x-csrf-token"] = csrf_token
        headers["Accept"] = "application/json"
        
        if range_value:
            url = f"https://www.ivasms.com/portal/numbers/return/range/{range_value}"
            payload = {"range": range_value}
            log_msg = f"Returning numbers for range: {range_value}"
        else:
            url = RETURN_ALL_URL
            payload = {}
            log_msg = "Returning all numbers"
        
        logger.info(f"{log_msg} | URL: {url}")
        async with session.post(url, headers=headers, data=payload, timeout=10) as response:
            logger.info(f"Return API response status code: {response.status}")
            text = await response.text()
            logger.debug(f"Return API response text: {text[:1000]}...")
            
            if response.status == 200:
                try:
                    response_data = await response.json()
                    logger.info(f"Return API response: {json.dumps(response_data, indent=2)[:500]}...")
                    if response_data.get("success", False) or (response_data.get("NumberDoneRemove") and len(response_data["NumberDoneRemove"]) > 0):
                        logger.info(f"{log_msg} successful")
                        return True
                    else:
                        logger.error(f"{log_msg} failed: {response_data.get('message', 'Unknown error')}")
                        return False
                except ValueError:
                    logger.error(f"Failed to parse return API response as JSON: {text[:500]}...")
                    return "success" in text.lower()
            elif response.status in [401, 403]:
                logger.error(f"{log_msg} failed due to authentication error (status code: {response.status})")
                return "SESSION_EXPIRED"
            else:
                logger.error(f"{log_msg} failed with status code: {response.status}, Response: {text[:500]}...")
                return False
    except Exception as e:
        logger.error(f"Error returning numbers: {str(e)}")
        return False

async def send_telegram_message(message, chat_id=CHAT_ID, reply_markup=None, auto_delete_seconds=None):
    logger.info(f"Sending Telegram message to chat_id {chat_id}: {message[:100]}...")
    try:
        sent_message = await bot.send_message(
            chat_id=chat_id,
            text=message,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
        logger.info(f"Message sent: Message ID {sent_message.message_id}")
        
        if auto_delete_seconds:
            await asyncio.sleep(auto_delete_seconds)
            try:
                await bot.delete_message(
                    chat_id=chat_id,
                    message_id=sent_message.message_id
                )
                logger.info(f"Message deleted: Message ID {sent_message.message_id}")
            except telegram.error.TelegramError as e:
                logger.error(f"Error deleting message: Message ID {sent_message.message_id}, Error: {str(e)}")
        return sent_message
    except telegram.error.BadRequest as e:
        logger.error(f"Telegram Bad Request error: {str(e)}")
        try:
            sent_message = await bot.send_message(
                chat_id=chat_id,
                text=message,
                reply_markup=reply_markup,
                parse_mode=None
            )
            logger.info(f"Message sent without Markdown: {message[:100]}...")
            if auto_delete_seconds:
                await asyncio.sleep(auto_delete_seconds)
                try:
                    await bot.delete_message(
                        chat_id=chat_id,
                        message_id=sent_message.message_id
                    )
                    logger.info(f"Message deleted: Message ID {sent_message.message_id}")
                except telegram.error.TelegramError as e:
                    logger.error(f"Error deleting message: Message ID {sent_message.message_id}, Error: {str(e)}")
            return sent_message
        except Exception as e2:
            logger.error(f"Retry Telegram message error: {str(e2)}")
    except telegram.error.InvalidToken:
        logger.error("Invalid Telegram bot token. Check with BotFather.")
    except telegram.error.NetworkError as e:
        logger.error(f"Telegram network error: {str(e)}")
    except Exception as e:
        logger.error(f"Error sending Telegram message: {str(e)}")
    return None

async def login_and_get_csrf(session, max_retries=3):
    for attempt in range(max_retries):
        try:
            logger.info(f"Login attempt {attempt + 1}/{max_retries}...")
            async with session.get(LOGIN_URL, headers=SMS_HEADERS, timeout=10) as response:
                logger.info(f"Login page status code: {response.status}")
                text = await response.text()
                soup = BeautifulSoup(text, 'html.parser')
                csrf_input = soup.find('input', {'name': '_token'})
                if not csrf_input:
                    logger.error("CSRF token not found!")
                    return None, None
                csrf_token = csrf_input['value']
                logger.info(f"CSRF token: {csrf_token}")
                
                payload = {
                    "_token": csrf_token,
                    "email": EMAIL,
                    "password": PASSWORD
                }
                logger.info("Sending login request...")
                async with session.post(LOGIN_URL, data=payload, headers=SMS_HEADERS, timeout=10) as login_response:
                    logger.info(f"Login response status code: {login_response.status}")
                    text = await login_response.text()
                    if login_response.status != 200 or "Dashboard" not in text:
                        error_msg = f"❌ Login failed! Attempt {attempt + 1}/{max_retries}"
                        logger.error(error_msg)
                        await send_telegram_message(error_msg, CHAT_ID)
                        if attempt < max_retries - 1:
                            await asyncio.sleep(5)
                        continue
                    logger.info("Login successful!")
                    return session, csrf_token
        except Exception as e:
            error_msg = f"❌ Login error (attempt {attempt + 1}/{max_retries}): {str(e)}"
            logger.error(error_msg)
            await send_telegram_message(error_msg, CHAT_ID)
            if attempt < max_retries - 1:
                await asyncio.sleep(5)
    return None, None

async def sync_numbers_from_api(session, csrf_token, chat_id=CHAT_ID):
    try:
        start_time = time.time()
        logger.info("Starting API synchronization for numbers and ranges...")

        base_url = "https://www.ivasms.com/portal/numbers"
        page_size = 1000
        start = 0
        all_ranges_data = {}
        headers = SMS_HEADERS.copy()
        headers.update({
            "x-csrf-token": csrf_token,
            "Accept": "application/json",
            "sec-ch-ua-platform": "\"Android\"",
            "sec-ch-ua": "\"Chromium\";v=\"136\", \"Android WebView\";v=\"136\", \"Not.A/Brand\";v=\"99\"",
            "sec-ch-ua-mobile": "?1",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "accept-language": "en-US,en;q=0.9",
            "priority": "u=1, i"
        })

        while True:
            params = {
                "draw": 2,
                "columns[0][data]": "number_id",
                "columns[0][name]": "id",
                "columns[0][orderable]": "false",
                "columns[1][data]": "Number",
                "columns[2][data]": "range",
                "columns[3][data]": "A2P",
                "columns[4][data]": "P2P",
                "columns[5][data]": "LimitA2P",
                "columns[6][data]": "limit_cli_a2p",
                "columns[7][data]": "limit_did_a2p",
                "columns[8][data]": "limit_cli_did_a2p",
                "columns[9][data]": "LimitP2P",
                "columns[10][data]": "limit_cli_p2p",
                "columns[11][data]": "limit_did_p2p",
                "columns[12][data]": "limit_cli_did_p2p",
                "columns[13][data]": "action",
                "columns[13][searchable]": "false",
                "columns[13][orderable]": "false",
                "order[0][column]": 1,
                "order[0][dir]": "desc",
                "start": start,
                "length": page_size,
                "search[value]": "",
                "_": int(time.time() * 1000)
            }

            api_start = time.time()
            logger.info(f"Fetching page: start={start}, page_size={page_size}")
            async with session.get(base_url, headers=headers, params=params, timeout=15) as response:
                api_time = time.time() - api_start
                logger.info(f"API response status code: {response.status}, took {api_time:.2f} seconds")

                if response.status in [401, 403]:
                    logger.info("Session expired during sync, re-logging in...")
                    session, new_csrf_token = await login_and_get_csrf(session)
                    if not session or not new_csrf_token:
                        error_msg = "❌ Failed to re-login during sync!"
                        logger.error(error_msg)
                        await send_telegram_message(error_msg, chat_id)
                        return False
                    headers["x-csrf-token"] = new_csrf_token
                    csrf_token = new_csrf_token
                    async with session.get(base_url, headers=headers, params=params, timeout=15) as response:
                        if not response.ok:
                            error_msg = f"❌ API sync retry failed! Status code: {response.status}"
                            logger.error(error_msg)
                  
