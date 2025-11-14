from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager

# Define your database connection details
DB_HOST = "localhost"
DB_NAME = "mydatabase"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"
DB_PORT = 5434  # adjust if needed

# Example query
SQL_QUERY = "SELECT * FROM public.visits LIMIT 5;"


def main():
    try:
        with PostgresConnectorContextManager(
                db_host=DB_HOST,
                db_name=DB_NAME,
                db_user=DB_USER,
                db_password=DB_PASSWORD,
                db_port=DB_PORT
        ) as db:
            # Test query to confirm table contents
            test_df = db.get_data_sql("SELECT COUNT(*) FROM public.visits;")
            print(f"Total rows in table: {test_df.iloc[0, 0]}")

            df = db.get_data_sql(SQL_QUERY)
            print(df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
