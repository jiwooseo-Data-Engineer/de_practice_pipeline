import schedule
import time

def job():
    # SQLAlchemy 쿼리 실행 + CSV 저장
    print("CSV 업데이트 완료!")

schedule.every().day.at("17:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(60)