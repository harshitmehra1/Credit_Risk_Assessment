# diagnose_schema.py
import csv
from pathlib import Path

FILES = [
    Path("data/raw/accepted_cleaned.csv"),
    Path("data/raw/rejected_cleaned.csv"),
]

SAMPLE_SHOW = 10  # how many bad-line samples to print

def diagnose(file_path: Path):
    print("\n" + "="*80)
    print(f"Diagnosing: {file_path}")
    if not file_path.exists():
        print(" ❌ File not found.")
        return

    bad_lines = []
    total = 0
    expected_fields = None

    # Open with universal newline support and csv.reader so quoting is respected
    with file_path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            print(" ❌ Empty file.")
            return
        expected_fields = len(header)
        total += 1  # count header
        # iterate lines
        for i, row in enumerate(reader, start=2):
            total += 1
            if len(row) != expected_fields:
                # store a compact sample: line number, field count, first/last 5 chars
                snippet = ",".join(row)[:300].replace("\n"," ").replace("\r"," ")
                bad_lines.append((i, len(row), snippet))
                if len(bad_lines) >= SAMPLE_SHOW:
                    # keep scanning counts but only collect a few examples
                    continue
        # no exception -> done

    print(f"Total lines scanned (incl header): {total:,}")
    print(f"Expected fields (header columns): {expected_fields}")
    print(f"Lines with unexpected field count: {len(bad_lines):,}")

    # show samples
    if bad_lines:
        print("\nSample bad-line entries (line_no, field_count, snippet):")
        for line_no, cnt, snippet in bad_lines[:SAMPLE_SHOW]:
            print(f" - line {line_no:,} -> {cnt} fields; snippet: {snippet!r}")
        print("\nRecommendation: inspect these lines in the CSV (open with a text editor),")
        print("or re-run the cleaning step with robust CSV options or drop these rows if they are few.")
    else:
        print("No structural field-count problems detected by csv.reader (good).")

if __name__ == "__main__":
    for f in FILES:
        diagnose(f)
