-- 建库
create database if not exists sales_dm;

use sales_dm;

--  建表
create table if not exists sales_result(
   `date` date,
   product_name string,
   order_amount decimal(18,2)
);

--  统计计算 (日期                 产品名                 订单数量    )
-- 需求1：将每天各种类型的产品的销售量进行汇总

select
    `date`,
    product_name,
    sum(order_amount) order_amount
from
(
    select
       date_format(o.effective_date,'yyyy-MM-dd') `date`,
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
group by t.`date`,t.product_name;


-- 需求2：将每天销量最高的top3的产品的进行汇总 (topN)

select
        `date`,
        product_name,
        order_amount
from
(
    select
            `date`,
            product_name,
            order_amount,
            row_number() over( distribute by `date` sort by  order_amount desc ) level
    from
    (
        select
            `date`,
            product_name,
            sum(order_amount) order_amount
        from
        (
            select
               date_format(o.effective_date,'yyyy-MM-dd') `date`,
               p.product_name,
               f.order_amount
            from
              sales_dw.fact_sales_order f,
              sales_dw.dim_order o,
              sales_dw.dim_product p
            where
              f.order_sk=o.order_sk and
              f.product_sk=p.product_sk
        ) t1
        group by t1.`date`,t1.product_name
    ) t2
) t3
where level<=3;

-- 需求3：将最近五天销量最高的top3的产品的进行汇总 (topN)
-- 思路：
-- 定位出最近五天
select
  `date`
from(
    select
        distinct date_format(effective_date,'yyyy-MM-dd') `date`
    from
        sales_dw.dim_order
) t order by  `date` limit 5;


-- 求解
select
        `date`,
        product_name,
        order_amount
from
(
    select
            `date`,
            product_name,
            order_amount,
            row_number() over( distribute by `date` sort by  order_amount desc ) level
    from
    (
        select
            `date`,
            product_name,
            sum(order_amount) order_amount
        from
        (
            select
               date_format(o.effective_date,'yyyy-MM-dd') `date`,
               p.product_name,
               f.order_amount
            from
              sales_dw.fact_sales_order f,
              sales_dw.dim_order o,
              sales_dw.dim_product p
            where
              f.order_sk=o.order_sk and
              f.product_sk=p.product_sk
        ) t1
        group by t1.`date`,t1.product_name
    ) t2
) t3
where level<=3 and
           `date` in(
                select
                  `date`
                from(
                    select
                        distinct date_format(effective_date,'yyyy-MM-dd') `date`
                    from
                        sales_dw.dim_order
                ) t order by  `date` limit 5
           );

-- 需求4：将最近五天销量最高的产品的进行汇总 (top1)
select
        `date`,
        product_name,
        order_amount
from
(
    select
            `date`,
            product_name,
            order_amount,
            row_number() over( distribute by `date` sort by  order_amount desc ) level
    from
    (
        select
            `date`,
            product_name,
            sum(order_amount) order_amount
        from
        (
            select
               date_format(o.effective_date,'yyyy-MM-dd') `date`,
               p.product_name,
               f.order_amount
            from
              sales_dw.fact_sales_order f,
              sales_dw.dim_order o,
              sales_dw.dim_product p
            where
              f.order_sk=o.order_sk and
              f.product_sk=p.product_sk
        ) t1
        group by t1.`date`,t1.product_name
    ) t2
) t3
where level=1 and
           `date` in(
                select
                  `date`
                from(
                    select
                        distinct date_format(effective_date,'yyyy-MM-dd') `date`
                    from
                        sales_dw.dim_order
                ) t order by  `date` limit 5
           );

-- 将计算的结果落地到表中
insert overwrite table sales_result
select
        `date`,
        product_name,
        order_amount
from
(
    select
            `date`,
            product_name,
            order_amount,
            row_number() over( distribute by `date` sort by  order_amount desc ) level
    from
    (
        select
            `date`,
            product_name,
            sum(order_amount) order_amount
        from
        (
            select
               date_format(o.effective_date,'yyyy-MM-dd') `date`,
               p.product_name,
               f.order_amount
            from
              sales_dw.fact_sales_order f,
              sales_dw.dim_order o,
              sales_dw.dim_product p
            where
              f.order_sk=o.order_sk and
              f.product_sk=p.product_sk
        ) t1
        group by t1.`date`,t1.product_name
    ) t2
) t3
where level=1 and
           `date` in(
                select
                  `date`
                from(
                    select
                        distinct date_format(effective_date,'yyyy-MM-dd') `date`
                    from
                        sales_dw.dim_order
                ) t order by  `date` limit 5
           );





