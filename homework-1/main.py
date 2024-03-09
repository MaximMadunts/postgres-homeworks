"""Скрипт для заполнения данными таблиц в БД Postgres."""

import csv
import psycopg2
from psycopg2 import sql

# Параметры подключения к базе данных
db_params = {
    'host':'localhost',
    'database':'north',
    'user':'postgres',
    'password':'i2MxSewr'
}
# Путь к CSV файлам
customers_csv = '../homework-1/north_data/customers_data.csv'
employees_csv = '../homework-1/north_data/employees_data.csv'
orders_csv = '../homework-1/north_data/orders_data.csv'

# SQL-запросы для вставки данных
insert_customer_query = """
INSERT INTO customers (customer_id, company_name, contact_name)
VALUES (%s, %s, %s);
"""

insert_employee_query = """
INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes)
VALUES (%s, %s, %s, %s, %s, %s);
"""

insert_order_query = """
INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city)
VALUES (%s, (SELECT customer_id FROM customers WHERE customer_id = %s), 
              (SELECT employee_id FROM employees WHERE employee_id = %s),
              %s, %s);
"""


def read_csv(file_path):
    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]


def execute_query(cursor, query, data):
    cursor.execute(query, data)


def main():
    try:
        # Подключение к базе данных
        connection = psycopg2.connect(
            database=db_params['database'],
            user=db_params['user'],
            password=db_params['password'],
            host=db_params['host'],
        )
        cursor = connection.cursor()

        # Чтение данных из CSV и вставка в таблицы
        customers_data = read_csv(customers_csv)
        employees_data = read_csv(employees_csv)
        orders_data = read_csv(orders_csv)

        for customer in customers_data:
            execute_query(cursor, insert_customer_query, (customer['customer_id'], customer['company_name'], customer['contact_name']))

        for employee in employees_data:
            execute_query(cursor, insert_employee_query, (employee['employee_id'], employee['first_name'], employee['last_name'], employee['title'], employee['birth_date'], employee['notes']))

        for order in orders_data:
            execute_query(cursor, insert_order_query, (order['order_id'], order['customer_id'], order['employee_id'], order['order_date'], order['ship_city']))

        # Применение изменений и закрытие соединения
        connection.commit()
        cursor.close()
        connection.close()

        print("Данные успешно добавлены в таблицы.")
    except Exception as e:
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    main()
