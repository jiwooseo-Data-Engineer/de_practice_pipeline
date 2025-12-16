import pandas as pd
from db import engine
from queries import DAU_QUERY, CTR_QUERY, CVR_QUERY, ARPU_QUERY


def get_daily_metrics():
    df_dau = pd.read_sql(DAU_QUERY, engine)
    df_ctr = pd.read_sql(CTR_QUERY, engine)
    df_cvr = pd.read_sql(CVR_QUERY, engine)
    df_arpu = pd.read_sql(ARPU_QUERY, engine)

    df = (
        df_dau
        .merge(df_ctr, on="event_date", how="left")
        .merge(df_cvr, on="event_date", how="left")
        .merge(df_arpu, on="event_date", how="left")
    )

    df = df.rename(columns={"event_date": "metric_date"})
    return df


def save_daily_metric():
    df = get_daily_metrics()
    print("row 수:", len(df))
    print(df)
    print(df.dtypes)

    df.to_sql(
        "daily_metric",
        con=engine,
        if_exists="append",
        index=False
    )



if __name__ == "__main__":
    save_daily_metric()