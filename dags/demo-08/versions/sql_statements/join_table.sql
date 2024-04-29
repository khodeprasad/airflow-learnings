CREATE TABLE IF NOT EXISTS {{params.employees_departments_table}} AS 
        SELECT employees.name, employees.title, departments.name AS department_name 
        FROM employees JOIN departments 
        ON employees.department_id = departments.id
        