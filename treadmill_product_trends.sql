-- Connect to the treadmill_db database
\c treadmill_db;

-- Drop the treadmill_products if already exists
DROP TABLE IF EXISTS treadmill_products CASCADE;

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
FROM '/Users/hesam/Desktop/codes/treadmill_sales_and_customer_trends/input/treadmill_data.csv'
DELIMITER ','
CSV HEADER;

-- Average age of customer per Treadmill Type
-- Count of customers per Treadmill Type
-- Average miles run per Treadmill Type
--  Range of fitness levels associated with each Treadmill Type
-- The correlation between income and fitness level for each treadmill type
-- Treadmill type associated with the highest income customers
-- Top 3 treadmill types used by the most active customers (highest miles run)

COPY(
    WITH 
        avg_age AS (
            SELECT Product, AVG(Age) AS average_age 
            FROM treadmill_products 
            GROUP BY Product
        ),
        customer_count AS (
            SELECT Product, COUNT(*) AS total_customers 
            FROM treadmill_products 
            GROUP BY Product
        ),
        avg_miles AS (
            SELECT Product, AVG(Miles) AS average_miles 
            FROM treadmill_products 
            GROUP BY Product
        ),
        fitness_range AS (
            SELECT Product, MIN(Fitness) AS min_level_fitness, MAX(Fitness) AS max_level_fitness 
            FROM treadmill_products 
            GROUP BY Product
        ),
        income_correlation AS (
            SELECT Product, CORR(Fitness, Income) AS income_fitness_correlation 
            FROM treadmill_products 
            GROUP BY Product
        ),
        high_income AS (
            SELECT Product, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY Income) AS high_income_percentile 
            FROM treadmill_products 
            GROUP BY Product
        ),
        max_income AS (
            SELECT Product, MAX(Income) AS max_income 
            FROM treadmill_products 
            GROUP BY Product
        ),
        top_miles AS (
            SELECT Product, SUM(Miles) AS total_miles 
            FROM treadmill_products 
            GROUP BY Product
        )

    SELECT
            a.Product, 
            a.average_age, 
            c.total_customers, 
            m.average_miles, 
            f.min_level_fitness, 
            f.max_level_fitness, 
            ic.income_fitness_correlation, 
            hi.high_income_percentile, 
            mi.max_income, 
            tm.total_miles
        FROM avg_age a
        LEFT JOIN customer_count c ON a.Product = c.Product
        LEFT JOIN avg_miles m ON a.Product = m.Product
        LEFT JOIN fitness_range f ON a.Product = f.Product
        LEFT JOIN income_correlation ic ON a.Product = ic.Product
        LEFT JOIN high_income hi ON a.Product = hi.Product
        LEFT JOIN max_income mi ON a.Product = mi.Product
        LEFT JOIN top_miles tm ON a.Product = tm.Product
    ORDER BY c.total_customers DESC
)
TO '/Users/hesam/Desktop/codes/treadmill_sales_and_customer_trends/results/treadmill_summary.csv' WITH (FORMAT CSV, HEADER);

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
COPY(
    SELECT sales.Product,
        sales.Gender,
        ROUND(COUNT(sales.Gender) * 100.0 / SUM(COUNT(sales.Gender)) OVER (PARTITION BY sales.Product), 2) AS gender_percentage
        FROM treadmill_products AS sales
        GROUP BY sales.Product, sales.Gender
    ORDER BY sales.Product
)
TO '/Users/hesam/Desktop/codes/treadmill_sales_and_customer_trends/results/treadmill_gender_percentage.csv' WITH (FORMAT CSV, HEADER);

-- Fitness level distribution per Treadmill Type
COPY(
    SELECT Product, Fitness, COUNT(*)
        FROM treadmill_products
        GROUP BY Product, Fitness
    ORDER BY Product, Fitness
)
TO '/Users/hesam/Desktop/codes/treadmill_sales_and_customer_trends/results/treadmill_fitness_level.csv' WITH (FORMAT CSV, HEADER);
