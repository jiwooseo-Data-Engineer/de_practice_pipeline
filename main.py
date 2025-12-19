import argparse
from datetime import datetime, date, timedelta

def parse_ymd(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid --date. Use YYYY-MM-DD (e.g. 2025-12-19).")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=parse_ymd,
        required=True,
        help="Target date in YYYY-MM-DD",
    )
    args = parser.parse_args()

    from metrics import save_daily_metric
    save_daily_metric(args.date)

if __name__ == "__main__":
    main()