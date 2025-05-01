import os, time, requests
import sys
from pathlib import Path

CSV_URL = "https://database.lichess.org/lichess_db_puzzle.csv.bz2"
LOCAL_PATH = Path(__file__).parent / "lichess_db_puzzle.csv.bz2"

def ensure_latest_csv():
    # 1) If we don’t have a local copy, download it unconditionally.
    if not LOCAL_PATH.exists():
        download_csv()
        return

    # 2) Otherwise, send a HEAD request to get the remote Last-Modified header.
    head = requests.head(CSV_URL, allow_redirects=True)
    remote_mod = head.headers.get("Last-Modified")
    if not remote_mod:
        # server doesn’t provide a Last-Modified → can’t compare, skip redownload
        return

    # parse the remote timestamp into a POSIX timestamp
    remote_ts = time.mktime(
        time.strptime(remote_mod, "%a, %d %b %Y %H:%M:%S %Z")
    )

    # get the local file’s modification time
    local_ts = LOCAL_PATH.stat().st_mtime

    # if remote is newer, redownload
    if remote_ts > local_ts:
        download_csv()

def download_csv():
    resp = requests.get(CSV_URL, stream=True)
    resp.raise_for_status()
    # get total size for progress bar
    total = resp.headers.get("Content-Length")
    total = int(total) if total is not None else 0
    chunk_size = 32768
    downloaded = 0
    with open(LOCAL_PATH, "wb") as f:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            f.write(chunk)
            if total:
                downloaded += len(chunk)
                done = int(50 * downloaded / total)
                percent = downloaded / total * 100
                bar = "=" * done + " " * (50 - done)
                print(f"\rDownloading: [{bar}] {percent:5.1f}%", end="", flush=True)
    if total:
        print()  # newline after completion
    # set the local file’s mtime to match the server’s Last-Modified
    remote_mod = resp.headers.get("Last-Modified")
    if remote_mod:
        remote_ts = time.mktime(
            time.strptime(remote_mod, "%a, %d %b %Y %H:%M:%S %Z")
        )
        os.utime(LOCAL_PATH, (remote_ts, remote_ts))

# Call this at startup:
ensure_latest_csv()