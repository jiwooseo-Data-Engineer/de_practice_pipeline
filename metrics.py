import pandas as pd
from sqlalchemy import text
from db import engine
from queries import DAU_QUERY, CTR_QUERY, CVR_QUERY, ARPU_QUERY


def get_daily_metrics(target_date):
    params = {"target_date": target_date}

    df_dau = pd.read_sql(text(DAU_QUERY), engine, params=params)
    df_ctr = pd.read_sql(text(CTR_QUERY), engine, params=params)
    df_cvr = pd.read_sql(text(CVR_QUERY), engine, params=params)
    df_arpu = pd.read_sql(text(ARPU_QUERY), engine, params=params)

    df = (
        df_dau
        .merge(df_ctr, on="event_date", how="left")
        .merge(df_cvr, on="event_date", how="left")
        .merge(df_arpu, on="event_date", how="left")
        .rename(columns={"event_date": "metric_date"})
    )

    return df

def save_daily_metric(target_date):
    with engine.begin() as conn:
        conn.execute(
            text("""
                DELETE FROM daily_metric
                WHERE metric_date = :target_date
            """),
            {"target_date": target_date}
        )

        df = get_daily_metrics(target_date)
        df.to_sql(
            "daily_metric",
            con=conn,
            if_exists="append",
            index=False
        )


if __name__ == "__main__":
    save_daily_metric()