--建库

create database if not exists sales_dm;

use sales_dm;
create table if not exists sales_result(
`date` date,
product_name string,
order_amount decimal(18,2)
);

select
`date`,
product_name

from  sales_dw.fact_sales_order()

