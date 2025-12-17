import argparse
from metrics import save_daily_metric

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    save_daily_metric(args.date)

# 테스트용: 바로 실행
if __name__ == "__main__":
    main()

