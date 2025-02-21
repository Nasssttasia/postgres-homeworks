import json

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', **params)
    cur = conn.cursor()

    cur.execute(f'DROP DATABASE {db_name}')
    cur.execute(f'CREATE DATABASE {db_name}')
    conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    cur.execute(script_file)


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    cur.execute("""
           CREATE TABLE suppliers (
               company_name VARCHAR(60),
               contact VARCHAR(200),
               address VARCHAR(200),
               phone VARCHAR,
               fax VARCHAR,
               homepage VARCHAR,
               products VARCHAR
           )
       """)


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, 'r') as file:
        json_data = json.loads(file)
        return json_data


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""

    cur.execute(f"INSERT INTO suppliers VALUES (%s, %s, %s, %s, %s,  %s, %s)", (suppliers['company_name'], suppliers['contact'],
                                                                                suppliers['address'], suppliers['phone'],
                                                                                suppliers['fax'], suppliers['homepage'],
                                                                                suppliers['products']))


def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    pass


if __name__ == '__main__':
    main()
