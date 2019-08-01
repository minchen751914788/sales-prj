# 创建job
sqoop job --create myjob -- import --connect jdbc:mysql://node03:3306/sales_source \
--username root \
--password-file /sqoop/pwd/sqoopPWD.pwd \
--query "select * from sales_order where \$CONDITIONS" \
--hive-import \
--hive-database sales_rds \
--hive-table sales_order \
--split-by order_number \
--fields-terminated-by '\t' \
--lines-terminated-by '\n' \
--target-dir /user/hive/warehouse/sales_rds.db/sales_order \
--hive-delims-replacement ' ' \
--incremental append \
--check-column entry_date \
--last-value '1900-1-1' \


# 查看job
sqoop job --list
sqoop job --show sales_job

# 执行job
sqoop job --exec sales_job

