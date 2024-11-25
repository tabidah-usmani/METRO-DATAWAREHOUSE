drop schema if exists `datawarehouse` ;
DROP TABLE if exists `datawarehouse`.`product`;
DROP TABLE if exists `datawarehouse`.`supplier`;
DROP TABLE if exists `datawarehouse`.`customer`;
DROP TABLE if exists `datawarehouse`.`store`;
DROP TABLE if exists `datawarehouse`.`sales`;
CREATE SCHEMA `datawarehouse` ;

-- drop schema if exists `datasource` ;
-- DROP TABLE if exists `TRANSACTIONS`;
-- DROP TABLE if exists `CUSTOMERS`;
-- DROP TABLE if exists `PRODUCTS`;
-- CREATE SCHEMA `datasource` ;
-- use `datasource` ;

-- these are for creating datawarehouse which will be populated by meshjoin in java 
use datawarehouse;

create table datawarehouse.customer(
	CUSTOMER_ID INT PRIMARY KEY,
    CUSTOMER_NAME varchar(255),
    GENDER varchar(10)
    );
    
    
create table datawarehouse.product(
	PRODUCT_ID INT PRIMARY KEY,
    PRODUCT_NAME varchar(255),
    PRODUCT_PRICE varchar(10)
    );
    
    
create table datawarehouse.store(
	STORE_ID INT PRIMARY KEY,
    STORE_NAME varchar(255)
    );
    
create table datawarehouse.supplier(
	SUPPLIER_ID INT PRIMARY KEY,
    SUPPLIER_NAME varchar(255)
    );
    
create table datawarehouse.timee(
	TIME_ID INT,
    ORDER_DATE DATE,
    DAY INT,
    WEEK INT,
    MONTH INT,
    YEAR INT,
    QUARTER INT,
    PRIMARY KEY(TIME_ID, ORDER_DATE));
    
create table datawarehouse.sales(
	ORDER_ID INT PRIMARY KEY,
--     ORDER_DATE DATE,
    QUANTITY INT,
	CUSTOMER_ID INT,
    PRODUCT_ID INT,
    STORE_ID INT,
    SUPPLIER_ID INT,
    TIME_ID INT,
    TOTAL_SALES DECIMAL(10,2),
	FOREIGN KEY (PRODUCT_ID) REFERENCES datawarehouse.product(PRODUCT_ID),
	FOREIGN KEY (CUSTOMER_ID) REFERENCES datawarehouse.customer(CUSTOMER_ID),
	FOREIGN KEY (STORE_ID) REFERENCES datawarehouse.store(STORE_ID),
	FOREIGN KEY (SUPPLIER_ID) REFERENCES datawarehouse.supplier(SUPPLIER_ID),
	FOREIGN KEY (TIME_ID) REFERENCES datawarehouse.timee(TIME_ID)
	
    );




