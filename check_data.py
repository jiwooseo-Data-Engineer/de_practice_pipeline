import pandas as pd

# CSV 파일 읽기
df = pd.read_csv("dau.csv")  # 저장된 CSV 파일 이름

# 상위 5행 확인
print(df.head())

# 데이터 요약 통계
print(df.describe())

# 컬럼 확인
print(df.columns)