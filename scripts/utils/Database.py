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

    def create_table(self, table_name, columns, primary_keys=None):
        # Define the columns for the CREATE TABLE query
        columns_query = []
        for column_name, column_type in columns.items():
            columns_query.append(f"{column_name} {column_type}")

        # Define the primary key constraint
        if primary_keys:
            primary_key = ", ".join(primary_keys)
            columns_query.append(f"PRIMARY KEY ({primary_key})")

        try:
            cursor = self.connection.cursor()

            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_query)})"
            cursor.execute(query)
            self.connection.commit()
            logging.info(f"Table {table_name} created successfully")
        except psycopg2.Error as e:
            print("Error creating table:", e)
            self.connection.rollback()

    def insert_data(self, table, columns, values):
        try:
            cursor = self.connection.cursor()
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in values])})"
            cursor.execute(query, values)
            self.connection.commit()
        except psycopg2.Error as e:
            logging.error("Error inserting data:", e)
            self.connection.rollback()

    def get_max_date(self, table_name, date_column):
        try:
            cursor = self.connection.cursor()
            query = f"SELECT MAX({date_column}) FROM {table_name}"
            cursor.execute(query)
            max_date = cursor.fetchone()[0]
            return max_date
        except psycopg2.Error as e:
            logging.error("Error getting max date:", e)
            return None

