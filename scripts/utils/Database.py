import logging

import psycopg2


class Database:
    def __init__(self):
        self.dbname = 'air_quality'
        self.user = 'postgres'
        self.password = '0000'
        self.host = 'localhost'
        self.port = 5432
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except psycopg2.Error as e:
            print("Error connecting to database:", e)

    def disconnect(self):
        if self.connection is not None:
            self.connection.close()

    def table_exists(self, table_name):
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = %s
                )
            """, (table_name,))
            exists = cursor.fetchone()[0]
            return exists
        except psycopg2.Error as e:
            print("Error checking if table exists:", e)
            return False

    def create_table(self, table_name, columns, primary_keys=None, foreign_keys=None):
        # Define the columns for the CREATE TABLE query
        columns_query = []
        for column_name, column_type in columns.items():
            columns_query.append(f"{column_name} {column_type}")

        # Define the primary key constraint
        if primary_keys:
            primary_key = ", ".join(primary_keys)
            columns_query.append(f"PRIMARY KEY ({primary_key})")

        # Define the foreign key constraints
        if foreign_keys:
            for fk_name, ref_table, ref_columns, columns in foreign_keys:
                fk_columns = ", ".join(columns)
                ref_columns = ", ".join(ref_columns)
                columns_query.append(
                    f"CONSTRAINT {fk_name} FOREIGN KEY ({fk_columns}) REFERENCES {ref_table} ({ref_columns})")

        try:
            cursor = self.connection.cursor()

            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_query)})"
            cursor.execute(query)
            self.connection.commit()
            logging.info(f"Table {table_name} created successfully")
        except psycopg2.Error as e:
            print("Error creating table:", e)
            self.connection.rollback()

    def bulk_insert(self, table, columns, values, conflict_keys, update_columns=None):
        try:
            cursor = self.connection.cursor()
            if update_columns:
                update_clause = ", ".join([f'"{column}" = excluded."{column}"' for column in update_columns])
                query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in columns])}) ON CONFLICT ({', '.join(conflict_keys)}) DO UPDATE SET {update_clause}"
            else:
                query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in columns])}) ON CONFLICT ({', '.join(conflict_keys)}) DO NOTHING"
            cursor.executemany(query, values)
            self.connection.commit()
        except psycopg2.Error as e:
            logging.error("Error bulk inserting data:", e)
            self.connection.rollback()

    def get_max_query(self, table_name, date_column, where_condition=None):
        try:
            cursor = self.connection.cursor()
            query = f"SELECT MAX({date_column}) FROM {table_name}"
            if where_condition:
                query += f" WHERE {where_condition}"
            cursor.execute(query)
            max_date = cursor.fetchone()[0]
            return max_date
        except psycopg2.Error as e:
            logging.error("Error getting max date:", e)
            return None

    def create_month_year_index(self, table_name):
        try:
            cursor = self.connection.cursor()
            index_name = f"{table_name}_month_year_idx"
            # Check if the index already exists
            cursor.execute("SELECT COUNT(*) FROM pg_indexes WHERE indexname = %s", (index_name,))
            if cursor.fetchone()[0] == 0:
                query = f"CREATE INDEX {index_name} ON {table_name} (EXTRACT(MONTH FROM date_local), EXTRACT(YEAR FROM date_local))"
                cursor.execute(query)
                self.connection.commit()
                logging.info(f"Index {index_name} created successfully")
            else:
                logging.info(f"Index {index_name} already exists")
        except psycopg2.Error as e:
            logging.error("Error creating index:", e)
            self.connection.rollback()
