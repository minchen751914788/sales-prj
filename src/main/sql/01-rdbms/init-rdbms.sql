/*==============================================================*/
/* DBMS name:      MySQL 5.0                                    */
/* Created on:     2018/11/23 1:09:10                           */
/*==============================================================*/

CREATE DATABASE IF NOT EXISTS sales_source DEFAULT CHARSET utf8 COLLATE utf8_general_ci;

USE sales_source;

DROP TABLE IF EXISTS customer;

DROP TABLE IF EXISTS product;

DROP TABLE IF EXISTS sales_order;

/*==============================================================*/
/* Table: customer                                              */
/*==============================================================*/
CREATE TABLE customer
(
   customer_number      INT(11) PRIMARY KEY AUTO_INCREMENT,
   customer_name        VARCHAR(128) NOT NULL,
   customer_street_address VARCHAR(256) NOT NULL,
   customer_zip_code    INT(11) NOT NULL,
   customer_city        VARCHAR(32) NOT NULL,
   customer_state       VARCHAR(32) NOT NULL
);

/*==============================================================*/
/* Table: product                                               */
/*==============================================================*/
CREATE TABLE product
(
   product_code         INT(11) NOT NULL AUTO_INCREMENT,
   product_name         VARCHAR(128) NOT NULL,
   product_category     VARCHAR(256) NOT NULL,
   PRIMARY KEY (product_code)
);

/*==============================================================*/
/* Table: sales_order                                           */
/*==============================================================*/
CREATE TABLE sales_order
(
   order_number         INT(11) NOT NULL AUTO_INCREMENT,
   customer_number      INT(11) NOT NULL,
   product_code         INT(11) NOT NULL,
   order_date           DATETIME NOT NULL,
   entry_date           DATETIME NOT NULL,
   order_amount         DECIMAL(18,2) NOT NULL,
   PRIMARY KEY (order_number)
);

/*==============================================================*/
/* insert data                                        */
/*==============================================================*/

INSERT INTO customer
( customer_name
, customer_street_address
, customer_zip_code
, customer_city
, customer_state
 )
VALUES
  ('Big Customers', '7500 Louise Dr.', '17050',
       'Mechanicsburg', 'PA')
, ( 'Small Stores', '2500 Woodland St.', '17055',
       'Pittsburgh', 'PA')
, ('Medium Retailers', '1111 Ritter Rd.', '17055',
       'Pittsburgh', 'PA'
)
,  ('Good Companies', '9500 Scott St.', '17050',
       'Mechanicsburg', 'PA')
, ('Wonderful Shops', '3333 Rossmoyne Rd.', '17050',
      'Mechanicsburg', 'PA')
, ('Loyal Clients', '7070 Ritter Rd.', '17055',
       'Pittsburgh', 'PA');


INSERT INTO product(product_name,product_category) VALUES
('Hard Disk','Storage'),
('Floppy Drive','Storage'),
('lcd panel','monitor');


#创建dim_date表
USE sales_source;
SHOW TABLES;

DELIMITER //
CREATE PROCEDURE USP_Load_Dim_Date(dt_start DATE,dt_end DATE)
   BEGIN

      WHILE dt_start<=dt_end DO
         INSERT INTO dim_date (`date`,`month`,`month_name`,`quarter`,`year`)
         VALUES (dt_start,MONTH(dt_start),MONTHNAME(dt_start),QUARTER(dt_start),YEAR(dt_start));
         SET dt_start =ADDDATE(dt_start,1);
      END WHILE;

      COMMIT;
   END //


CREATE TABLE dim_date(
   `date`                 DATE,
   `MONTH`                TINYINT,
   month_name            VARCHAR(16),
   `QUARTER`              TINYINT,
   `YEAR`                 INT
);
SHOW TABLES;
CALL USP_Load_Dim_Date('2010-1-1','2050-1-1');

#在hive的rds也创建个dim_date表
#通过sqoop导入,如果报错,可能是因为split未指定,修改后的sh如下



