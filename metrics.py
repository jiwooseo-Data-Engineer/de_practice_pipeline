import pandas as pd
from datetime import date
from sqlalchemy import text

from daily_metrics.db import engine
from daily_metrics.queries import DAU_QUERY, CTR_QUERY, CVR_QUERY, ARPU_QUERY


def _date_str(d):
    return d.isoformat() if hasattr(d, "isoformat") else str(d)


def get_daily_metrics(target_date: date) -> pd.DataFrame:
    target_date = _date_str(target_date)
    params = {"target_date": target_date}

    # pandas read_sql_query를 DBAPI 커넥션으로 실행 (SQLAlchemy 인식 문제 회피)
    raw_conn = engine.raw_connection()
    try:
        df_dau = pd.read_sql_query(DAU_QUERY, con=raw_conn, params=params)
        df_ctr = pd.read_sql_query(CTR_QUERY, con=raw_conn, params=params)
        df_cvr = pd.read_sql_query(CVR_QUERY, con=raw_conn, params=params)
        df_arpu = pd.read_sql_query(ARPU_QUERY, con=raw_conn, params=params)
    finally:
        raw_conn.close()

    df = (
        df_dau
        .merge(df_ctr, on="event_date", how="left")
        .merge(df_cvr, on="event_date", how="left")
        .merge(df_arpu, on="event_date", how="left")
        .rename(columns={"event_date": "metric_date"})
    )
    return df


def save_daily_metric(target_date: date) -> None:
    df = get_daily_metrics(target_date)

    # DELETE는 트랜잭션으로 처리
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM daily_metric WHERE metric_date = :target_date"),
            {"target_date": target_date},
        )

    # INSERT는 pandas가 엔진을 확실히 인식하도록 engine을 직접 넘김
    df.to_sql(
        "daily_metric",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )


if __name__ == "__main__":
    save_daily_metric(date.today())
