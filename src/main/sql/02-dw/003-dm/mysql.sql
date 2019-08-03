
insert overwrite table sales_result
select
`date`,
product_name,
order_amount
from
(select
`date`,
product_name,
order_amount,
row_number()over(distribute by `date` sort by order_amount desc) level
from
(
select `date`,
product_name,
sum(order_amount) order_amount
from
(select date_format(o.effective_date,'yyyy-MM-dd') `date`,
p.product_name,
f.order_amount
from
sales_dw.fact_sales_order f,
sales_dw.dim_order o,
sales_dw.dim_product p
where
f.order_sk=o.order_sk and
f.product_sk=p.product_sk
) t
group by t.`date`,t.product_name
) t2
)t3
where level<=3;


/*
sqoop export --connect "jdbc:mysql://192.168.170.45:3306/sales_dm?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password 88888888 \
--table sales_result \
--export-dir "/user/hive/warehouse/sales_dm.db/sales_result"    \
--input-fields-terminated-by '\001'   \
--input-null-string '\\N'   \
--input-null-non-string '\\N'

*/

