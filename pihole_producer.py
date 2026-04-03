import os
import re
import time
import pandas as pd
from datetime import datetime

LOG_FILE = "/var/log/pihole/pihole.log"
BUFFER_SIZE = 500
OUTPUT_DIR = "./data/bronze"

# EXAMPLE: Apr  3 12:00:01 dnsmasq[123]: query[A] google.com from 192.168.1.10
LOG_PATTERN = re.compile(
    r"(?P<month>\w{3}\s+\d+)\s+(?P<time>\d{2}:\d{2}:\d{2})\s+dnsmasq\[\d+\]:\s+"
    r"query\[(?P<type>\w+)\]\s+(?P<domain>[^\s]+)\s+from\s+(?P<client_ip>[^\s]+)"
)

def follow(file_path):
    with open(file_path, "r") as f:
        f.seek(0, os.SEEK_END)
        while  True:
            line = f.readline()
            if not line:
                time.sleep(0.1)
                continue
            yield line

def save_to_parquet(data):
    if not data:
        return

    df = pd.DataFrame(data)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"pihole_logs_{timedtamp}.parquet"

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    path = os.path.join(OUTPUT_DIR, filename)
    df.to_parquet(path, index=False, engine='pyarrow')
    print(f"[*] Saved {len(data)} rows to {path}")

def run_ingestion():
    print(f"[!] Starting Paranoid Ingestor on {LOG_FILE}")
    buffer = []

    try:
        for line in follow(LOG_FILE):
            match = LOG_PATTERN.search(line)
            if match:
                entry = match.groupdict()
                entry['year'] = datetime.now().year
                buffer.append(entry)

        if len(buffer) >= BUFFER_SIZE:
            save_to_parquet(buffer)
            buffer = []

    except KeyboardInterrupt:
        print("\n[!] Stopping... Saving remaining data.")
        save_to_parquet(buffer)

if __name__ == "__main__":
    run_ingestion()
                       