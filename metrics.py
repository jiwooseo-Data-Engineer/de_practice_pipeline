import pandas as pd
from db import engine
from queries import DAU_QUERY, CTR_QUERY

def get_daily_metrics():
    df_dau = pd.read_sql(DAU_QUERY, engine)
    df_ctr = pd.read_sql(CTR_QUERY, engine)
    return pd.merge(df_dau, df_ctr, on='event_date', how='left')