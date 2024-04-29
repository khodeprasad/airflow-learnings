CREATE TABLE IF NOT EXISTS {{params.employees_table}} (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            title VARCHAR(50) NOT NULL,
            department_id INTEGER NOT NULL
        )