import os, time, requests, bz2, shutil
import pandas as pd
from pathlib import Path

CSV_URL = "https://database.lichess.org/lichess_db_puzzle.csv.bz2"
LOCAL_PATH = Path(__file__).parent / "lichess_db_puzzle.csv.bz2"
VERSION = Path(__file__).parent / "version.txt"
def ensure_latest_csv():
    # Check for internet connection
    try:
        requests.head("https://www.google.com", timeout=3)
    except requests.RequestException:
        print("No connection, continuing")
        return

    # 1) If we don't have a local copy, download it unconditionally.
    if not VERSION.exists():
        download_csv()
        decompress_csv()
        convert_to_parquet()
        return

    # 2) Otherwise, send a HEAD request to get the remote Last-Modified header.
    head = requests.head(CSV_URL, allow_redirects=True)
    remote_mod = head.headers.get("Last-Modified")

    # parse the remote timestamp into a POSIX timestamp.
    global remote_ts
    remote_ts = time.mktime(
        time.strptime(remote_mod, "%a, %d %b %Y %H:%M:%S %Z")
    )

    # get the local file's modification time
    try:
        with open(VERSION, "r") as f:
            local_ts = (f.read())
    except:
        local_ts = 0
    # if remote is newer, redownload
    if float(remote_ts) > float(local_ts):
        download_csv()
        decompress_csv()
        convert_to_parquet()
    else:
        print("Up to date")

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
    # set the local file's mtime to match the server's Last-Modified
    remote_mod = resp.headers.get("Last-Modified")
    if remote_mod:
        remote_ts = time.mktime(
            (time.strptime(remote_mod, "%a, %d %b %Y %H:%M:%S %Z"))
        )
        os.utime(LOCAL_PATH, (remote_ts, remote_ts))

def decompress_csv():
    print("\nDecompressing CSV...")
    compressed_csv_path = LOCAL_PATH
    decompressed_csv_path = Path(__file__).parent / "lichess_db_puzzle.csv"
    with bz2.open(compressed_csv_path, 'rb') as f_in, open(decompressed_csv_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

def convert_to_parquet():
    print("\nConverting to parquet...")
    parquet_path = "lichess_db_puzzle.parquet"
    decompressed_csv_path = Path(__file__).parent / "lichess_db_puzzle.csv"
    fieldnames = [
        "id","fen","moves","rating","ratingDeviation","popularity","nbPlays","themes","gameUrl","openingTags"
    ]
    
    # Get total number of lines for progress bar
    total_lines = sum(1 for _ in open(decompressed_csv_path, "rt", encoding="utf-8")) - 1  # subtract header
    
    # Manually parse CSV to preserve commas in openingTags
    records = []
    with open(decompressed_csv_path, "rt", encoding="utf-8") as f:
        next(f)  # skip header row
        for i, line in enumerate(f, 1):
            # There are multiple commas in the openingTags field, so we need to split on the last for the parquet file
            parts = line.rstrip("\n").split(",", len(fieldnames) - 1) 
            # Check the openingTags field correctly parsed
            if len(parts) == len(fieldnames):
                records.append(parts)
            
            # Update progress bar
            done = int(50 * i / total_lines)
            percent = i / total_lines * 100
            bar = "=" * done + " " * (50 - done)
            print(f"\rConverting to parquet: [{bar}] {percent:5.1f}%", end="", flush=True)
    
    print()  # newline after completion
    
    df = pd.DataFrame(records, columns=fieldnames)
    df.to_parquet(parquet_path)
    # Update version file
    with open(VERSION, "w") as f:
        f.write(f"{remote_ts}")
    LOCAL_PATH.unlink()
    decompressed_csv_path.unlink()

if __name__ == "__main__":
    ensure_latest_csv()