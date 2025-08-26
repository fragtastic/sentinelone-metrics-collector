import duckdb
from datetime import datetime, timedelta

def get_daily_max_per_query(db_path: str, days: int):
    with duckdb.connect(db_path) as con:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        query = f"""
        SELECT
            CAST(Timestamp AS DATE) AS day,
            Query,
            MAX(Result) AS max_result
        FROM s1_metrics
        WHERE Timestamp >= '{cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}'
        GROUP BY day, Query
        ORDER BY day DESC, Query
        """
        
        df = con.execute(query).fetchdf()
        return df

# Usage example
if __name__ == "__main__":
    df = get_daily_max_per_query('metrics.duckdb', 30)
    print(df)
