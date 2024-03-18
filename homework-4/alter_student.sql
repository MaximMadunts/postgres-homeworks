-- 1. Создать таблицу student с полями student_id serial, first_name varchar, last_name varchar, birthday date, phone varchar

CREATE TABLE student
(
student_id serial,
first_name varchar( 30 ) NOT NULL,
last_name varchar( 50 ) NOT NULL,
birthdate date NOT NULL,
phone varchar,
PRIMARY KEY ( student_id )
);

-- 2. Добавить в таблицу student колонку middle_name varchar
ALTER TABLE student ADD COLUMN middle_name varchar( 30 );


-- 3. Удалить колонку middle_name
ALTER TABLE student DROP COLUMN middle_name;

-- 4. Переименовать колонку birthday в birth_date
ALTER TABLE student RENAME COLUMN birthdate TO birth_date;


-- 5. Изменить тип данных колонки phone на varchar(32)
ALTER TABLE student ALTER COLUMN phone SET DATA TYPE varchar( 32 );


-- 6. Вставить три любых записи с автогенерацией идентификатора
INSERT INTO student ( first_name, last_name, birth_date, phone )
VALUES ( 'Alex', 'Pavlov', '21/01/1998', '+8 432 432 423 554' ),
( 'Egor', 'Antonov', '30/04/2000', '+7 904 412 432 432'),
( 'Vasily', 'Storov', '11/09/1991', '+7 943 412 432 432');

-- 7. Удалить все данные из таблицы со сбросом идентификатор в исходное состояние
TRUNCATE TABLE student RESTART IDENTITY;