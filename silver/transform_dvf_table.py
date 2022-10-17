# Databricks notebook source
DVF_table = spark.sql("SELECT DISTINCT md5( CONCAT_WS('|', *)) as DVF_ID, NOW() as CreatedDate, * FROM bronze.dvf_file where Valeur_fonciere is not  null and Surface_reelle_bati is not null and Type_local = 'Appartement' and Valeur_fonciere < 1000000 and  Commune IN ('DIJON')")
DVF_table.distinct().createOrReplaceTempView('dvf_file_updates')

# COMMAND ----------

#Create dvf table
#DVF_table.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/silver/dvf_table") 
#spark.sql("CREATE TABLE IF NOT EXISTS silver.dvf_table USING DELTA LOCATION '/FileStore/silver/dvf_table'")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.dvf_table
# MAGIC USING dvf_file_updates
# MAGIC ON silver.dvf_table.DVF_ID = dvf_file_updates.DVF_ID
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver.dvf_table

# COMMAND ----------


