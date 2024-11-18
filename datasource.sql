-- drop database if exists `datasource` ;
DROP TABLE if exists `TRANSACTIONS`;
-- DROP TABLE if exists `CUSTOMERS`;
-- DROP TABLE if exists `PRODUCTS`;
-- CREATE database `datasource` ;
use `datasource` ;


create table transactions (
  ORDER_ID INT,
  ORDER_DATE DATE,
  PRODUCT_ID INT NOT NULL,
  QUANTITY INT,
  CUSTOMER_ID INT NOT NULL,
  PRIMARY KEY (ORDER_ID, CUSTOMER_ID, PRODUCT_ID)
);

 create table products(
 	PRODUCT_ID INT PRIMARY KEY,
     PRODUCT_NAME VARCHAR(255),
     PRODUCT_PRICE DECIMAL(10,2),
     SUPPLIER_ID INT,
     SUPPLIER_NAME VARCHAR(255),
     STORE_ID INT,
     STORE_NAME VARCHAR(255)
);

create table customers(
    CUSTOMER_ID INT PRIMARY KEY,
    CUSTOMER_NAME VARCHAR(255),
    GENDER VARCHAR(10)
);
	




LOAD DATA LOCAL INFILE "E:/OneDrive - FAST National University/Semester5/Data Warehousing/Project/transactions_data.csv"
INTO TABLE transactions
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(ORDER_ID, ORDER_DATE, PRODUCT_ID, QUANTITY, CUSTOMER_ID);



LOAD DATA LOCAL INFILE 'E:/OneDrive - FAST National University/Semester5/Data Warehousing/Project/customers_data.csv'
INTO TABLE customers
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE "E:/OneDrive - FAST National University/Semester5/Data Warehousing/Project/products_data.csv"
INTO TABLE products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME);


SET GLOBAL local_infile = 1;
SHOW GLOBAL VARIABLES LIKE 'local_infile';


select COUNT(*) as `Total records in Transactions` from transactions;
select COUNT(*) as `Total records in Products` from products;
select COUNT(*) as `Total records in Customers` from customers;