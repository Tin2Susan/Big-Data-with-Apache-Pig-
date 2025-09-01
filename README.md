# Bookstore Data Processing with Apache Pig

This project processes a sample bookstore database using Apache Pig to replicate SQL queries and 
extend functionality with a Python UDF. The data comes from two CSV files:

cust_order.csv (order details)

order_line.csv (books and prices)

# Tasks
Task 1

Translate the given SQL query into a Pig script to compute:

order_date

number of distinct orders

number of books

total price

Output format:
(2021,3,28) 36 15 366.9899970293045

# Task 2

Develop a Python UDF to add a note column:

>= 300 → high value

>= 100 and < 300 → medium

< 100 → low value

Output format:

(2021,3,28) 36 15 366.9899970293045 high value

# Tools

Apache Pig

Hadoop (for large datasets)

Python (UDFs)
