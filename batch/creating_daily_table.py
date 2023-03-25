import mysql.connector
import json

# Get configuration data
with open('/tmp/pycharm_project_301/config.json') as f:
    config = json.load(f)

# Connect to MySQL database
db = mysql.connector.connect(
    host="localhost",
    user=config['mysql']['user'],
    password=config['mysql']['password'],
    database="stocks"
)

# Create a cursor object
cursor = db.cursor()

# Execute the SQL statement to create the table
cursor.execute("""
  CREATE TABLE IF NOT EXISTS daily_stocks (
    id INT AUTO_INCREMENT,
    ticker VARCHAR(10), 
    volume FLOAT,
    volume_weighted FLOAT,
    open_price FLOAT,
    close_price FLOAT,
    highest_price FLOAT,
    lowest_price FLOAT,
    business_day DATE,
    transactions_number INT,
    PRIMARY KEY (id, business_day)
  )
PARTITION BY RANGE(YEAR(business_day)) (
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024)
);
""")

# Close the database connection
db.close()