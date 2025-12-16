import schedule
import time
from main import update_all_csv

def job():
    update_all_csv()
    print("CSV 업데이트 완료")

schedule.every().day.at("17:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(60)