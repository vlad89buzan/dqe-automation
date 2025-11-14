import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor


class PostgresConnectorContextManager:
    def __init__(self, db_host: str, db_user: str, db_password: str, db_port: int, db_name='mydatabase', ):
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_port = db_port
        self.conn = None

    def __enter__(self):
        try:
            self.conn = psycopg2.connect(
                host=self.db_host,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                port=self.db_port,
                cursor_factory=RealDictCursor
            )
            return self
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.conn:
            self.conn.close()

    def get_data_sql(self, sql: str) -> pd.DataFrame:
        with self.conn.cursor() as cur:
            cur.execute(sql)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            df = pd.DataFrame(data, columns=columns)
            return df
