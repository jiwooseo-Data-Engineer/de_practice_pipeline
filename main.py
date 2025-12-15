import pandas as pd
from sqlalchemy import text
from db import engine
from queries import DAU_QUERY, CTR_QUERY

def save_query_to_csv(query, filename):
    df = pd.read_sql(text(query), engine)
    df.to_csv(filename, index=False)
    print(f"{filename} 저장 완료!")

def update_all_csv():
    save_query_to_csv(DAU_QUERY, "dau.csv")
    save_query_to_csv(CTR_QUERY, "ctr.csv")

# 테스트용: 바로 실행
if __name__ == "__main__":
    update_all_csv()