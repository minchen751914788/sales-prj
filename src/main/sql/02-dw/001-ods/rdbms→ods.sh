sqoop import \
--connect jdbc:mysql://NODE03:3306/sales_source \
--username root \
--password 88888888 \
--table customer \
--hive-import \
--hive-table sales_rds.customer \
--hive-overwrite \
--target-dir /user/hive/warehouse/sales_rds.db/customer \
--delete-target-dir