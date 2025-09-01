REGISTER 'categorize_price.py' USING org.apache.pig.scripting.streaming.python.PythonScriptEngine AS myfuncs

-- Load customer order data from 'cust_order.csv'
customer_order_data = LOAD 'hdfs:///input/cust_order.csv' USING PigStorage(',') AS (
    order_id: int,
    order_date: chararray,
    customer_id: int,
    shipping_method_id: int,
    dest_address_id: int);

-- Format the order_date field into a tuple (year, month, day)
formatted_date_orders = FOREACH customer_order_data GENERATE
    order_id,
    CONCAT(CONCAT('(', SUBSTRING(order_date, 1, 5), ','),
    CONCAT(REPLACE(SUBSTRING(order_date, 6, 8), '^0+', ''), ','),
    REPLACE(SUBSTRING(order_date, 9, 11), '^0+', ''), ')') AS order_date,
    customer_id,
    shipping_method_id,
    dest_address_id;

-- Load order line data from 'order_line.csv'
order_line_data = LOAD 'hdfs:///input/order_line.csv' USING PigStorage(',') AS (
    line_id: int,
    order_id: int,
    book_id: int,
    price: float);

-- Join formatted order data with order line data by order_id
joined_data = JOIN formatted_date_orders BY order_id, order_line_data BY order_id;

-- Group the joined data by order_date
grouped_data = GROUP joined_data BY formatted_date_orders::order_date;

-- Calculate the results for each group
results = FOREACH grouped_data {
    -- Count the distinct order IDs as num_orders
    num_orders = DISTINCT joined_data.formatted_date_orders::order_id;
    
    -- Generate the final results
    GENERATE
        group AS order_day,
        COUNT(joined_data.order_line_data::book_id) AS num_books,
        COUNT(num_orders) AS num_orders,
        SUM(joined_data.price) AS total_price;
}

-- Order the results by total_price in descending order
ordered_data = ORDER results BY total_price DESC;

transformed_data = FOREACH ordered_data GENERATE order_day, num_books, num_orders, total_price, myfuncs.categorize_price(total_price) AS note:chararray;

store transformed_data into 'hdfs:///output/task2'; 


