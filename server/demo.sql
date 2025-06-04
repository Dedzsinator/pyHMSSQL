CREATE DATABASE company_db;

USE company_db;

SHOW DATABASES;

CREATE TABLE departments
(
    dept_id INT PRIMARY KEY,
    dept_name VARCHAR(100) NOT NULL,
    location VARCHAR(50),
    budget DECIMAL(12,2) DEFAULT 0
);

CREATE TABLE employees
(
    emp_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    dept_id INT,
    salary DECIMAL(10,2),
    hire_date DATETIME,
    status VARCHAR(20) DEFAULT 'Active',
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);

SHOW TABLES;

CREATE INDEX idx_emp_dept ON employees(dept_id);
CREATE INDEX idx_emp_salary ON employees(salary);
CREATE UNIQUE INDEX idx_emp_email ON employees(email);
CREATE INDEX idx_dept_name ON departments(dept_name);

SHOW INDEXES;

INSERT INTO departments
    (dept_id, dept_name, location, budget)
VALUES
    (1, 'Engineering', 'San Francisco', 2500000.00),
    (2, 'Marketing', 'New York', 800000.00),
    (3, 'Finance', 'Chicago', 1200000.00),
    (4, 'Human Resources', 'Austin', 600000.00),
    (5, 'Sales', 'Boston', 1800000.00);

INSERT INTO employees
    (emp_id, first_name, last_name, email, dept_id, salary, hire_date, status)
VALUES
    (101, 'John', 'Smith', 'john.smith@company.com', 1, 95000.00, '2020-01-15', 'Active'),
    (102, 'Sarah', 'Johnson', 'sarah.johnson@company.com', 1, 105000.00, '2019-03-22', 'Active'),
    (103, 'Mike', 'Brown', 'mike.brown@company.com', 2, 65000.00, '2021-06-10', 'Active'),
    (104, 'Emily', 'Davis', 'emily.davis@company.com', 2, 70000.00, '2020-11-05', 'Active'),
    (105, 'Robert', 'Wilson', 'robert.wilson@company.com', 3, 85000.00, '2018-09-12', 'Active'),
    (106, 'Lisa', 'Garcia', 'lisa.garcia@company.com', 3, 90000.00, '2019-07-30', 'Active'),
    (107, 'David', 'Martinez', 'david.martinez@company.com', 4, 60000.00, '2021-02-18', 'Active'),
    (108, 'Jennifer', 'Anderson', 'jennifer.anderson@company.com', 5, 75000.00, '2020-08-25', 'Active'),
    (109, 'Christopher', 'Taylor', 'chris.taylor@company.com', 5, 80000.00, '2019-12-03', 'Active'),
    (110, 'Amanda', 'Thomas', 'amanda.thomas@company.com', 1, 110000.00, '2018-05-14', 'Inactive');

SELECT *
FROM departments;

SELECT first_name, last_name, salary
FROM employees
WHERE salary >= 80000;

SELECT *
FROM employees
WHERE dept_id = 1 AND salary > 100000;

SELECT emp_id, first_name, last_name, salary
FROM employees
WHERE salary >= 70000 AND status = 'Active';

SELECT *
FROM employees
ORDER BY salary DESC LIMIT 5;

UPDATE employees
SET salary
= 98000.00 WHERE emp_id = 101;

UPDATE employees SET status = 'Inactive' WHERE dept_id = 4;

DELETE FROM employees WHERE emp_id = 110;

SELECT *
FROM employees
WHERE emp_id IN (101, 107);

SELECT e.first_name, e.last_name, e.salary, d.dept_name, d.location
FROM employees e
    INNER JOIN departments d ON e.dept_id = d.dept_id
WHERE e.salary >= 75000 AND d.location IN ('San Francisco', 'New York');

SELECT d.dept_name, d.location, e.first_name, e.last_name, e.salary
FROM departments d
    LEFT JOIN employees e ON d.dept_id = e.dept_id
WHERE d.budget > 1000000;

SELECT e.first_name + ' ' + e.last_name AS full_name,
    e.salary,
    d.dept_name,
    d.budget
FROM employees e
    INNER JOIN departments d ON e.dept_id = d.dept_id
WHERE e.salary > 70000 AND d.budget > 800000
ORDER BY e.salary DESC;

SELECT COUNT(*) as total_employees
FROM employees;
SELECT AVG(salary) as average_salary
FROM employees;
SELECT SUM(salary) as total_payroll
FROM employees;
SELECT MIN(salary) as lowest_salary, MAX(salary) as highest_salary
FROM employees;

SELECT COUNT(*) as active_employees
FROM employees
WHERE status = 'Active';
SELECT AVG(salary) as avg_eng_salary
FROM employees
WHERE dept_id = 1;

SELECT dept_id, COUNT(*) as employee_count, AVG(salary) as avg_salary
FROM employees
GROUP BY dept_id;

SELECT d.dept_name,
    COUNT(e.emp_id) as employee_count,
    AVG(e.salary) as avg_salary,
    SUM(e.salary) as total_salary
FROM departments d
    LEFT JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_id, d.dept_name;

SELECT d.dept_name,
    COUNT(e.emp_id) as employee_count,
    AVG(e.salary) as avg_salary
FROM departments d
    INNER JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_id, d.dept_name
HAVING COUNT(e.emp_id) >= 2 AND AVG(e.salary) > 75000;

SELECT d.dept_name,
    COUNT(e.emp_id) as employee_count,
    AVG(e.salary) as avg_salary,
    SUM(e.salary) as department_payroll
FROM departments d
    INNER JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_id, d.dept_name
ORDER BY avg_salary DESC;

SELECT d.dept_name,
    e.first_name + ' ' + e.last_name as employee_name,
    e.salary,
    e.hire_date
FROM departments d
    INNER JOIN employees e ON d.dept_id = e.dept_id
WHERE e.status = 'Active'
ORDER BY d.dept_name ASC, e.salary DESC;

SELECT first_name, last_name, salary
FROM employees
WHERE salary > (SELECT AVG(salary)
FROM employees);

SELECT d.dept_name, e.first_name, e.last_name, e.salary
FROM employees e
    INNER JOIN departments d ON e.dept_id = d.dept_id
WHERE e.salary = (
    SELECT MAX(salary)
FROM employees e2
WHERE e2.dept_id = e.dept_id
);

SELECT d.dept_name,
    d.budget,
    COALESCE(SUM(e.salary), 0) as actual_payroll,
    d.budget - COALESCE(SUM(e.salary), 0) as budget_remaining
FROM departments d
    LEFT JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_id, d.dept_name, d.budget
ORDER BY budget_remaining DESC;

SHOW INDEXES;

DROP INDEX idx_emp_salary ON employees;

CREATE INDEX idx_emp_salary ON employees(salary);

SHOW INDEXES FOR employees;

DROP INDEX idx_emp_dept ON employees;
DROP INDEX idx_emp_email ON employees;
DROP INDEX idx_dept_name ON departments;

DROP TABLE employees;
DROP TABLE departments;

SHOW TABLES;

DROP DATABASE company_db;

SHOW DATABASES;