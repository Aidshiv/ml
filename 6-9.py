*************prog 06********
#1 Open Terminal and run:

start-dfs.sh
start-yarn.sh

#2. Create Input Data Files
Create employees.txt and departments.txt in your local directory.

employees.txt

101,John,30,1,50000
102,Sam,28,2,60000
103,Anna,32,1,75000
104,David,29,3,62000
105,Lily,27,2,58000

departments.txt

1,HR
2,Finance
3,IT

#3. Upload Files to HDF

hdfs dfs -mkdir -p /user/cloudera
hdfs dfs -put -f employees.txt /user/cloudera/
hdfs dfs -put -f departments.txt /user/cloudera/

#4. Create and Save the Pig Script

gedit employee_analysis.pig

"
-- Load the employees dataset
employees = LOAD 'hdfs://localhost:9000/user/cloudera/employees.txt'
USING PigStorage(',')
AS (emp_id:int, name:chararray, age:int, dept_id:int, salary:int);

-- Load the departments dataset
departments = LOAD 'hdfs://localhost:9000/user/cloudera/departments.txt'
USING PigStorage(',')
AS (dept_id:int, dept_name:chararray);

-- 1. FILTER: Select employees with age greater than 28
filtered_employees = FILTER employees BY age > 28;

-- 2. PROJECT: Select only emp_id, name, and salary
projected_employees = FOREACH filtered_employees GENERATE emp_id, name, salary;

-- 3. SORT: Order employees by salary in descending order
sorted_employees = ORDER projected_employees BY salary DESC;

-- 4. GROUP: Group employees by department ID
grouped_by_department = GROUP employees BY dept_id;

-- 5. JOIN: Join employees with department names
joined_data = JOIN employees BY dept_id, departments BY dept_id;

-- STORE results into HDFS
STORE sorted_employees INTO 'hdfs://localhost:9000/user/cloudera/output/sorted_employees' USING PigStorage(',');
STORE grouped_by_department INTO 'hdfs://localhost:9000/user/cloudera/output/grouped_by_department' USING PigStorage(',');
STORE joined_data INTO 'hdfs://localhost:9000/user/cloudera/output/joined_data' USING PigStorage(',');

-- DISPLAY results on screen (optional during testing)
DUMP sorted_employees;
DUMP grouped_by_department;
DUMP joined_data;


# 5. Run the Pig Script in MapReduce Mode

pig -x mapreduce employee_analysis.pig


#6. View Output from HDFS

hdfs dfs -cat /user/cloudera/output/sorted_employees/part-r-00000
hdfs dfs -cat /user/cloudera/output/grouped_by_department/part-r-00000
hdfs dfs -cat /user/cloudera/output/joined_data/part-r-00000





**************prog7*************************

#1 Start Hadoop, Hive, and HDFS:
start-dfs.sh
start-yarn.sh
hive


# 2. Upload Data to HDFS

Create the data file employees.txt

101,John,30,1,50000
102,Sam,28,2,60000
103,Anna,32,1,75000
104,David,29,3,62000
105,Lily,27,2,58000


Upload it to HDFS:

hdfs dfs -mkdir -p /user/cloudera
hdfs dfs -put -f employees.txt /user/cloudera/

# 3. Open Hive Shell

hive

# 4. Database Operations

-- Create Database
CREATE DATABASE employee_db;

-- Use Database
USE employee_db;

-- Alter Database (add property)
ALTER DATABASE employee_db SET DBPROPERTIES ('Owner'='Admin');

-- Drop Database
DROP DATABASE employee_db CASCADE;

#5. Table Operations
-- Create Table
CREATE TABLE employees (
 emp_id INT,
 name STRING,
 age INT,
 dept_id INT,
 salary FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Load Data into Table
LOAD DATA INPATH '/user/cloudera/employees.txt' INTO TABLE employees;

-- Add Column to Table
ALTER TABLE employees ADD COLUMNS (email STRING);

-- Rename Table
ALTER TABLE employees RENAME TO employees_new;

-- Drop Table
DROP TABLE employees_new;

#6. View Operations

-- Recreate employees table if dropped
CREATE TABLE employees (
 emp_id INT,
 name STRING,
 age INT,
 dept_id INT,
 salary FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/cloudera/employees.txt' INTO TABLE employees;

-- Create View
CREATE VIEW high_salary_employees AS
SELECT emp_id, name, salary
FROM employees
WHERE salary > 50000;

-- Alter View
ALTER VIEW high_salary_employees AS
SELECT emp_id, name, age, salary
FROM employees
WHERE salary > 60000;

-- Drop View
DROP VIEW high_salary_employees;


# 7. Function (UDF) Operations

ssume you have a UDF JAR file /user/cloudera/custom_udf.jar with a class com.example.hiveudf.ToUpperUDF.


-- Add JAR
ADD JAR /user/cloudera/custom_udf.jar;

-- Create Function
CREATE FUNCTION to_upper AS 'com.example.hiveudf.ToUpperUDF';

-- Use the Function
SELECT to_upper(name) FROM employees;

-- Drop the Function
DROP FUNCTION to_upper;


#8. Index Operations

-- Create Index
CREATE INDEX emp_dept_idx
ON TABLE employees (dept_id)
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
WITH DEFERRED REBUILD;

-- Rebuild Index
ALTER INDEX emp_dept_idx ON employees REBUILD;

-- Drop Index
DROP INDEX emp_dept_idx ON employees;

#9. Useful Hive Commands

-- Show All Tables
SHOW TABLES;

-- Describe Table Structure
DESCRIBE employees;

-- Display Table Data
SELECT * FROM employees LIMIT 5;

***********prog8***********
# Sample input text (can be multiline)
text = """
hello world
hello hadoop
big data world
"""

# Mapper function: splits lines into (word, 1) pairs
def mapper(line):
    words = line.strip().split()
    return [(word, 1) for word in words]

# Shuffle & Sort using defaultdict
from collections import defaultdict
mapped = []
for line in text.strip().split('\n'):
    mapped.extend(mapper(line))

shuffle_sort = defaultdict(list)
for word, count in mapped:
    shuffle_sort[word].append(count)

# Reducer function: sums the counts for each word
def reducer(shuffled_data):
    reduced = {}
    for word, counts in shuffled_data.items():
        reduced[word] = sum(counts)
    return reduced

# Execute reducer and print the output
word_counts = reducer(shuffle_sort)
for word, count in word_counts.items():
    print(f"{word}\t{count}")





*************prog9*******************

#1. Start Cloudera Services

sudo service cloudera-scm-server start
sudo service cloudera-scm-agent start
# Check status
sudo service --status-all | grep cloudera

#2. Access HUE Web Interface
Open browser → go to http://localhost:8888

Login with:
Username: cloudera
Password: cloudera


#3. Upload Dataset to HDFS

Sample employees.csv:

id,name,department,salary
1,John,IT,70000
2,Alice,HR,60000
3,Bob,IT,75000
4,Charlie,Finance,80000
5,David,HR,62000
6,Eva,IT,72000
7,Frank,Finance,81000
8,Grace,HR,65000
Upload via HUE:



Navigate: File Browser → HDFS → Create /user/cloudera/data

Upload employees.csv to /user/cloudera/data


Or via Terminal:

hdfs dfs -mkdir -p /user/cloudera/data
hdfs dfs -put employees.csv /user/cloudera/data/


#4. Create Hive Table in HUE
Open Query Editor → Select Hive, then run:


CREATE DATABASE IF NOT EXISTS company;
USE company;

CREATE TABLE employees (
  id INT,
  name STRING,
  department STRING,
  salary INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/cloudera/data/employees.csv' INTO TABLE employees;


#5. Verify Data

SELECT * FROM employees;

#6. Run Data Analysis Queries




>>Highest salary per department:

SELECT department, MAX(salary) AS highest_salary
FROM employees
GROUP BY department;


>>Employees with salary > 65000:

SELECT * FROM employees WHERE salary > 65000;

#7. Generate Reports in HUE
Run query in Query Editor
Click Export → select format (CSV, Excel, JSON)
Download report file



#8. Create Dashboard for Visualization
In HUE → Click Dashboard
Click Create New Dashboard
Add Widget → Choose chart type (Bar, Pie, etc.)

Enter query, e.g.:

SELECT department, COUNT(*) AS employee_count FROM employees GROUP BY department;


Click Run Query → View chart visualization

