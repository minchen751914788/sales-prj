# 创建job
 1，导入的shell脚本：
  方式1：incremental之append
	sqoop job --create sales_job -- import --connect jdbc:mysql://NODE03:3306/sales_source \
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
	--last-value '1900-1-1'

	瑕疵：增量导入失败，原因：/opt/hive/conf/hive-site.xml文件个别参数配置有问题。


 方式2：incremental之lastmodified
	sqoop job --create sales_job -- import --connect jdbc:mysql://node03:3306/sales_source \
	--username root \
	--password-file /sqoop/pwd/sqoopPWD.pwd \
	--query "select * from sales_order where \$CONDITIONS" \
	--split-by order_number \
	--fields-terminated-by '\t' \
	--lines-terminated-by '\n' \
	--target-dir /user/hive/warehouse/sales_rds.db/sales_order \
	--incremental lastmodified \
	--check-column entry_date \
	--last-value '1900-1-1' \
	--append


# 查看job
sqoop job --list
sqoop job --show sales_job

# 执行job
sqoop job --exec sales_job

