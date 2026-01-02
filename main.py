import os
import mmap
import time
from concurrent.futures import ProcessPoolExecutor
from collections import Counter

# --- CONFIGURATION ---
DIRECTORY = "Oath_Keepers"
EMAILS_FILE = "OK Emails.txt"
COPS_FILE = "Local_Cops.txt"
KEYWORDS_FILE = "keywords.txt"
RESULTS_FILE = "Results_Final.txt"


def load_resources():
    def clean_read(filename):
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
                # Encode to bytes for fast mmap comparison
                return [line.strip().lower().encode('utf-8') for line in f if line.strip()]
        return []

    keywords = clean_read(KEYWORDS_FILE)
    emails = clean_read(EMAILS_FILE)

    cops = []
    if os.path.exists(COPS_FILE):
        with open(COPS_FILE, 'r', encoding='utf-8', errors='ignore') as f:
            current_ref = "Unknown_Source"
            for line in f:
                if "NEWFILE" in line:
                    current_ref = line.split(",", 1)[1].strip() if "," in line else line.strip()
                    continue
                parts = line.split(",", 1)
                if len(parts) > 0:
                    last_name = parts[0].strip().lower().encode('utf-8')
                    if last_name != b'': cops.append((last_name, line.strip(), current_ref))
    return keywords, emails, cops


def get_context(mm, pos):
    """Efficiently extracts surrounding context without loading the whole file."""
    # Find start of previous line (look back two newline characters)
    first_nl = mm.rfind(b'\n', 0, pos)
    second_nl = mm.rfind(b'\n', 0, first_nl) if first_nl != -1 else -1
    start = second_nl + 1

    # Find end of next line
    first_nl_fwd = mm.find(b'\n', pos)
    second_nl_fwd = mm.find(b'\n', first_nl_fwd + 1) if first_nl_fwd != -1 else -1
    end = second_nl_fwd if second_nl_fwd != -1 else mm.size()

    return mm[start:end].decode('utf-8', errors='ignore').strip()


def search_worker(file_name, keywords, emails, cops):
    file_path = os.path.join(DIRECTORY, file_name)
    print(f"Processing {file_name}...")
    results = []
    match_counts = Counter()

    try:
        file_size = os.path.getsize(file_path)
        if file_size == 0: return results, match_counts, 0

        with open(file_path, 'rb') as f:
            # 0 length maps the whole file
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                # 1. Check Emails
                for email in emails:
                    pos = mm.find(email)
                    while pos != -1:
                        context = get_context(mm, pos)
                        results.append(f"[EMAIL] {email.decode()} in {file_name}\nContext: {context}")
                        match_counts['Emails'] += 1
                        pos = mm.find(email, pos + 1)

                # 2. Check Cops + Keywords
                for last_name, full_cop_info, ref_source in cops:
                    pos = mm.find(last_name)
                    while pos != -1:
                        context = get_context(mm, pos)
                        context_lower = context.lower()
                        # Verify keywords in context
                        for kw in keywords:
                            if kw.decode() in context_lower:
                                results.append(
                                    f"[MATCH] {full_cop_info} ({ref_source}) in {file_name}\nReview: {context}")
                                match_counts[ref_source] += 1
                                break
                        pos = mm.find(last_name, pos + 1)
        return results, match_counts, file_size
    except Excep tion as e:
        return [f"Error in {file_name}: {e}"], Counter(), 0


def main():
    keywords, emails, cops = load_resources()
    file_list = [f for f in os.listdir(DIRECTORY) if os.path.isfile(os.path.join(DIRECTORY, f))]

    total_stats = Counter()
    total_bytes_processed = 0
    start_time = time.perf_counter()

    print(f"Analyzing {len(file_list)} file(s)...")

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(search_worker, f, keywords, emails, cops) for f in file_list]

        with open(RESULTS_FILE, 'w', encoding='utf-8') as out_f:
            for future in futures:
                batch_results, batch_stats, bytes_read = future.result()
                total_stats += batch_stats
                total_bytes_processed += bytes_read
                if batch_results:
                    for found in batch_results:
                        out_f.write(found + "\n" + "-" * 20 + "\n")

    end_time = time.perf_counter()
    duration = end_time - start_time
    total_mb = total_bytes_processed / (1024 * 1024)
    speed = total_mb / duration if duration > 0 else 0

    # --- FINAL REPORT ---
    summary = [
        "\n" + "=" * 40,
        "SEARCH PERFORMANCE & SUMMARY",
        "=" * 40,
        f"Total Files Scanned:   {len(file_list)}",
        f"Total Data Processed:  {total_mb:.2f} MB",
        f"Total Time Taken:      {duration:.2f} seconds",
        f"Average Speed:         {speed:.2f} MB/s",
        "-" * 40
    ]

    for source, count in total_stats.most_common():
        summary.append(f"{source}: {count} matches")

    summary_text = "\n".join(summary)
    with open(RESULTS_FILE, 'a', encoding='utf-8') as out_f:
        out_f.write(summary_text)

    print(summary_text)


if __name__ == '__main__':
    main()