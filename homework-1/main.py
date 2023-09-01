"""Скрипт для заполнения данными таблиц в БД Postgres."""

import psycopg2
import csv


def write_data(csv_file, table):
    with psycopg2.connect(host="localhost", database="north", user="postgres", password="nastasia1") as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT * from {table}')
            rows = cur.fetchall()

            with open(f'north_data/{csv_file}', 'r') as file:
                reader = csv.reader(file)
                next(reader)

                for row in reader:
                    cur.execute(f"INSERT INTO {table} VALUES ({', '.join(['%s'] * len(row))})", row)
                    conn.commit()

    conn.close()


write_data('customers_data.csv', 'customers')
write_data('employees_data.csv', 'employees')
write_data('orders_data.csv', 'orders')
