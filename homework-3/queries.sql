-- Напишите запросы, которые выводят следующую информацию:
-- 1. Название компании заказчика (company_name из табл. customers) и ФИО сотрудника, работающего над заказом этой компании (см таблицу employees),
SELECT company_name, CONCAT(first_name,' ', last_name) AS concatenated_values
FROM orders
JOIN customers as c USING(customer_id)
JOIN employees as e USING(employee_id)

-- когда и заказчик и сотрудник зарегистрированы в городе London, а доставку заказа ведет компания United Package (company_name в табл shippers)
SELECT company_name, CONCAT(first_name,' ', last_name) AS concatenated_values
FROM orders
JOIN customers as c USING(customer_id)
JOIN employees as e USING(employee_id)
WHERE customers.city = employees.city AND shippers.company_name = 'United Package';

-- 2. Наименование продукта, количество товара (product_name и units_in_stock в табл products),


-- имя поставщика и его телефон (contact_name и phone в табл suppliers) для таких продуктов,
-- которые не сняты с продажи (поле discontinued) и которых меньше 25 и которые в категориях Dairy Products и Condiments.
-- Отсортировать результат по возрастанию количества оставшегося товара.

SELECT product_name, units_in_stock, suppliers.contact_name, suppliers.phone
FROM products
INNER JOIN suppliers USING ( supplier_id )
INNER JOIN categories USING ( category_id )
WHERE discontinued = 0 AND units_in_stock < 25 AND categories.category_name IN ( 'Dairy Products', 'Condiments' )
ORDER BY units_in_stock;

-- 3. Список компаний заказчиков (company_name из табл customers), не сделавших ни одного заказа
SELECT company_name
FROM customers
LEFT JOIN orders USING ( customer_id )
WHERE orders.order_id IS NULL;

-- 4. уникальные названия продуктов, которых заказано ровно 10 единиц (количество заказанных единиц см в колонке quantity табл order_details)
-- Этот запрос написать именно с использованием подзапроса.
SELECT DISTINCT product_name
FROM products
WHERE product_id = ANY ( SELECT product_id FROM order_details WHERE quantity = 10 );