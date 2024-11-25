use datawarehouse;

-- query number1: Find the top 5 products that generated the highest revenue, separated by weekday and weekend 
-- sales, with results grouped by month for a specified year. 

SELECT 
    t.MONTH,
    (CASE 
        WHEN DAYOFWEEK(t.ORDER_DATE) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END) AS day_type,
    p.product_name,
    SUM(s.total_sales) AS total_revenue
FROM 
    sales s
JOIN
    product p ON s.PRODUCT_ID = p.PRODUCT_ID
JOIN 
	timeE t ON s.TIME_ID=t.TIME_ID
WHERE 
    t.YEAR= '2019'
GROUP BY 
    t.MONTH,
    day_type,
    p.product_name
ORDER BY 
    total_revenue DESC
LIMIT 5;


-- query number2: Calculate the revenue growth rate for each store on a quarterly basis for 2017. -- 
SELECT STORE_ID, t.QUARTER AS Quarters, t.YEAR AS Year, SUM(TOTAL_SALES) AS TotalRevenue
FROM Sales s
JOIN
	timee t ON t.TIME_ID=s.TIME_ID
WHERE YEAR(ORDER_DATE) = 2019
GROUP BY STORE_ID, Year, Quarters;

-- query number3: For each store, show the total sales contribution of each supplier broken down by product 
-- category. The output should group results by store, then supplier, and then product category 
-- under each supplier.
SELECT st.STORE_NAME, su.SUPPLIER_NAME, pr.PRODUCT_NAME, SUM(s.TOTAL_SALES) AS TotalSalesContribution
FROM Sales s
JOIN Product pr
    ON s.PRODUCT_ID = pr.PRODUCT_ID
JOIN Supplier su
    ON s.SUPPLIER_ID = su.SUPPLIER_ID
JOIN Store st
    ON s.STORE_ID = st.STORE_ID
GROUP BY  st.STORE_NAME, su.SUPPLIER_NAME,pr.PRODUCT_NAME;

-- query number4: Present total sales for each product, drilled down by seasonal periods (Spring, Summer, Fall, 
-- Winter) and further by region. This can help understand product performance in different regions 
-- across seasonal periods.
SELECT p.product_name,
(CASE 
	WHEN t.MONTH IN (3, 4, 5) THEN 'Spring'
	WHEN t.MONTH IN (6, 7, 8) THEN 'Summer'
	WHEN t.MONTH IN (9, 10, 11) THEN 'Fall'
	ELSE 'Winter'
    END) as season,
    SUM(s.TOTAL_SALES) as total_sales
FROM 
    sales s
JOIN
	timee t ON t.TIME_ID=s.TIME_ID
JOIN 
    product p ON s.PRODUCT_ID = p.PRODUCT_ID
GROUP BY 
    p.product_name,season;


-- query number5: Calculate the month-to-month revenue volatility for each store and supplier pair. Volatility can be 
-- defined as the percentage change in revenue from one month to the next, helping identify stores 
-- or suppliers with highly fluctuating sales. 

 
-- query number6: Identify the top 5 products frequently bought together within a set of orders (i.e., multiple 
-- products purchased in the same transaction). This product affinity analysis could inform potential 
-- product bundling strategies.
SELECT
    s1.PRODUCT_ID AS Product1,
    s2.PRODUCT_ID AS Product2,
    COUNT(*) AS Frequency
FROM
    sales s1
JOIN
    sales s2 ON s1.ORDER_ID = s2.ORDER_ID AND s1.PRODUCT_ID < s2.PRODUCT_ID
GROUP BY
    s1.PRODUCT_ID, s2.PRODUCT_ID
ORDER BY
    Frequency DESC
LIMIT 5;

-- query number7: Use the ROLLUP operation to aggregate yearly revenue data by store, supplier, and product, 
-- enabling a comprehensive overview from individual product-level details up to total revenue per 
-- store. This query should provide an overview of cumulative and hierarchical sales figures. 

SELECT STORE_ID, SUPPLIER_ID, PRODUCT_ID, t.YEAR as Year,SUM(TOTAL_SALES) AS Total_Revenue
FROM
    sales s
JOIN
	timee t ON s.TIME_ID=t.TIME_ID
GROUP BY
    ROLLUP(STORE_ID, SUPPLIER_ID, PRODUCT_ID, YEAR)
ORDER BY
    STORE_ID, SUPPLIER_ID, PRODUCT_ID, Year;

-- query number8: For each product, calculate the total revenue and quantity sold in the first and second halves of 
-- the year, along with yearly totals. This split-by-time-period analysis can reveal changes in product 
-- popularity or demand over the year.
SELECT
    p.PRODUCT_ID, t.YEAR,
    SUM(CASE WHEN t.MONTH BETWEEN 1 AND 6 THEN s.TOTAL_SALES ELSE 0 END) AS First_Half_Revenue,
    SUM(CASE WHEN t.MONTH BETWEEN 7 AND 12 THEN s.TOTAL_SALES ELSE 0 END) AS Second_Half_Revenue,
    SUM(s.TOTAL_SALES) AS Total_Revenue,
    SUM(CASE WHEN t.MONTH BETWEEN 1 AND 6 THEN s.QUANTITY ELSE 0 END) AS First_Half_Quantity,
    SUM(CASE WHEN t.MONTH BETWEEN 7 AND 12 THEN s.QUANTITY ELSE 0 END) AS Second_Half_Quantity,
    SUM(s.QUANTITY) AS Total_Quantity
FROM
    sales s
JOIN
    timee t ON s.TIME_ID = t.TIME_ID
JOIN
    product p ON s.PRODUCT_ID = p.PRODUCT_ID
GROUP BY 
	p.PRODUCT_ID, t.YEAR
ORDER BY
    p.PRODUCT_ID, t.YEAR;
 
 -- query number9: Calculate daily average sales for each product and flag days where the sales exceed twice the daily 
-- average by product as potential outliers or spikes.

-- query number10:
    
