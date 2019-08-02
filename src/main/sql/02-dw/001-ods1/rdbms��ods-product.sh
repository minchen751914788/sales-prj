sqoop import \
--connect jdbc:mysql://NODE03:3306/sales_source \
--username root \
--password 88888888 \
--table product \
--hive-import \
--hive-table sales_rds.product \
--hive-overwrite \
--target-dir /user/hive/warehouse/sales_rds.db/product \
--delete-target-dir
