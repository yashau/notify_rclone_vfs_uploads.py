#!/usr/bin/env python3

import os
import time
import asyncio
import threading
import re
import logging
from datetime import datetime, timedelta
import sys

try:
    import telegram
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    from dotenv import load_dotenv
except ModuleNotFoundError as e:
    logging.error(f"Required module is missing: {e.name}")
    sys.exit(1)

load_dotenv()

def check_env_vars(required_vars):
    for var in required_vars:
        if not os.getenv(var):
            logging.error(f"Environment variable {var} is not set.")
            sys.exit(1)

required_vars = ['TELEGRAM_TOKEN', 'CHAT_ID', 'RCLONE_CACHE_DIR', 'JOB_NAME']
check_env_vars(required_vars)

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
DIRECTORY_TO_WATCH = os.getenv('RCLONE_CACHE_DIR')
JOB_NAME = os.getenv('JOB_NAME')

logfile = os.path.join(os.path.dirname(__file__), 'upload.log')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M',
                    handlers=[logging.FileHandler(logfile), logging.StreamHandler()])

class TelegramBotHandler(FileSystemEventHandler):
    def __init__(self, bot, loop, message_buffer, lock):
        self.bot = bot
        self.loop = loop
        self.message_buffer = message_buffer
        self.lock = lock

    async def send_telegram_message(self, message):
        await self.bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='Markdown')

    def extract_canonical_name(self, path):
        patterns = []
        pattern_index = 1
        while True:
            pattern_env = f"PATTERN_{pattern_index}"
            pattern = os.getenv(pattern_env)
            if not pattern:
                break
            patterns.append(pattern)
            pattern_index += 1

        for pattern in patterns:
            filename = os.path.basename(path)
            match = re.match(pattern, filename)
            if match:
                return match.group(1)

        return None

    def on_created(self, event):
        if not event.is_directory:
            name = os.path.basename(event.src_path)
            message = f"{name} has started uploading."
            logging.info(message)

    def on_deleted(self, event):
        if not event.is_directory:
            name = os.path.basename(event.src_path)
            canonical_name = self.extract_canonical_name(event.src_path)
            if canonical_name:
                current_time = datetime.now().strftime("%H:%M")
                message = f"{current_time} - {canonical_name}"
                with self.lock:
                    self.message_buffer.append(message)
            message = f"{name} has finished uploading."
            logging.info(message)

def start_event_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

async def send_buffered_messages(bot, loop, message_buffer, lock):
    while True:
        now = datetime.now()
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        time_to_sleep = (next_hour - now).total_seconds()

        logging.info(f"Sleeping for {time_to_sleep} seconds until next Telegram notification.")

        await asyncio.sleep(time_to_sleep)

        with lock:
            if message_buffer:
                backup_list = ""
                for line in message_buffer:
                    backup_list += f"\n{line}"
                full_message = f"*{JOB_NAME}*\n\nThe following backups completed successfully:\n{backup_list}"
                await bot.send_message(chat_id=CHAT_ID, text=full_message, parse_mode='Markdown')
                message_buffer.clear()

def main():
    bot = telegram.Bot(token=TELEGRAM_TOKEN)
    loop = asyncio.new_event_loop()
    message_buffer = []
    lock = threading.Lock()
    event_handler = TelegramBotHandler(bot, loop, message_buffer, lock)

    observer = Observer()
    observer.schedule(event_handler, DIRECTORY_TO_WATCH, recursive=True)
    observer.start()

    event_loop_thread = threading.Thread(target=start_event_loop, args=(loop,))
    event_loop_thread.start()

    asyncio.run_coroutine_threadsafe(send_buffered_messages(bot, loop, message_buffer, lock), loop)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        loop.call_soon_threadsafe(loop.stop)
    observer.join()
    event_loop_thread.join()

if __name__ == "__main__":
    try:
        check_env_vars(['TELEGRAM_TOKEN', 'CHAT_ID', 'RCLONE_CACHE_DIR', 'JOB_NAME'])
        main()
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        sys.exit(1)