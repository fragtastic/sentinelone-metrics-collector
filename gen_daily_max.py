import duckdb

def export_daily_max_to_excel(db_path: str, output_file: str):
    con = duckdb.connect(db_path)
    
    # Ensure the Excel extension is available (autoloaded on first use, but safe to do manually)
    con.execute("INSTALL excel;")
    con.execute("LOAD excel;")
    
    # Create the aggregated table (in memory)
    con.execute("""
    CREATE OR REPLACE TABLE daily_max AS
    SELECT
        CAST(Timestamp AS DATE) AS day,
        Query,
        MAX(Result) AS max_result
    FROM s1_metrics
    GROUP BY day, Query
    ORDER BY day, Query;
    """)
    
    # Export to Excel directly
    con.execute(f"""
    COPY daily_max
    TO '{output_file}'
    WITH (FORMAT xlsx, HEADER true, SHEET 'DailyMax');
    """)
    
    print(f"Successfully exported daily max per query to '{output_file}'.")

if __name__ == "__main__":
    export_daily_max_to_excel('metrics.duckdb', 'daily_max.xlsx')
