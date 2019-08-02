sqoop import \
--connect jdbc:mysql://NODE03:3306/sales_source \
--username root \
--password 88888888 \
--table dim_date \
--hive-import \
--hive-table sales_rds.dim_date \
--hive-overwrite \
--target-dir /user/hive/warehouse/sales_rds.db/dim_date \
--delete-target-dir
