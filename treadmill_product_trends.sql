-- Connect to the treadmill_db database
\c treadmill_db;

-- Drop the treadmill_products if already exists
DROP TABLE IF EXISTS treadmill_products;

-- Create the treadmill_products table
CREATE TABLE treadmill_products (
    Id SERIAL PRIMARY KEY,
    Product TEXT,
    Age NUMERIC,
    Gender VARCHAR(10),
    Education VARCHAR(20),
    MaritalStatus VARCHAR(10),
    Usage NUMERIC,
    Fitness NUMERIC,
    Income NUMERIC,
    Miles NUMERIC
);

-- Import the input CSV file
COPY treadmill_products(Product, Age, Gender, Education, MaritalStatus, Usage, Fitness, Income, Miles)
FROM '/treadmill_sales_and_customer_trends/treadmill_data.csv'
DELIMITER ','
CSV HEADER;

-- Average age of customer per Treadmill Type
SELECT Product, AVG(Age) AS average_age FROM treadmill_products
GROUP BY Product;

-- Count of customers per Treadmill Type
SELECT Product, COUNT(*) AS total_customers FROM treadmill_products
GROUP BY Product
ORDER BY total_customers DESC;

-- Average miles run per Treadmill Type
SELECT Product, AVG(Miles) AS average_miles FROM treadmill_products
GROUP BY Product
ORDER BY average_miles DESC;

--  Range of fitness levels associated with each Treadmill Type
SELECT Product, MIN(Fitness) AS min_level_fitness, MAX(Fitness) AS max_level_fitness FROM treadmill_products
GROUP BY Product;

-- Fitness level distribution per Treadmill Type
SELECT Product, Fitness, COUNT(*) FROM treadmill_products
GROUP BY Product, Fitness
ORDER BY Product, Fitness;

-- The correlation between income and fitness level for each treadmill type
SELECT Product, CORR(Fitness, Income) FROM treadmill_products
GROUP BY Product;

-- Gender preference distribution across treadmill types (using CTE)
-- WITH ProductCounts AS (
--     SELECT Product, COUNT(*) AS total_count
--     FROM treadmill_products
--     GROUP BY Product
-- )
-- SELECT sales.Product, 
--        sales.Gender,
--        ROUND(COUNT(sales.Gender) * 100.0 / counts.total_count, 2) AS gender_percentage
-- FROM treadmill_products AS sales
-- JOIN ProductCounts AS counts 
--     ON counts.Product = sales.Product
-- GROUP BY sales.Product, sales.Gender, counts.total_count
-- ORDER BY sales.Product;

-- Gender preference distribution across treadmill types (using window function - Over clause)
SELECT sales.Product,
       sales.Gender,
       ROUND(COUNT(sales.Gender) * 100.0 / SUM(COUNT(sales.Gender)) OVER (PARTITION BY sales.Product), 2) AS gender_percentage
    FROM treadmill_products AS sales
    GROUP BY sales.Product, sales.Gender
ORDER BY sales.Product;