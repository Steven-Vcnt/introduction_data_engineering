# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import FloatType

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.introductiondatastorage.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.introductiondatastorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.introductiondatastorage.dfs.core.windows.net", "sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2022-10-15T01:39:36Z&st=2022-10-14T17:39:36Z&spr=https&sig=m0Q9064gcgiv%2BQUKSUa8Y16Vlc9IIX8EiEU%2FlGSPKMM%3D")

# COMMAND ----------

sp_dvf_file=spark.read.csv('abfs://containerintro@introductiondatastorage.dfs.core.windows.net/immodata/valeursfoncieres-2021.txt',sep="|", header=True)

# COMMAND ----------

display(sp_dvf_file)

# COMMAND ----------


renamed_sp_dvf = sp_dvf_file.select([F.col(col).alias(col.replace(' ', '_')) for col in sp_dvf_file.columns])
dot_sp_dvf = renamed_sp_dvf.withColumn('Valeur_fonciere', regexp_replace('Valeur_fonciere', ',', '.').cast("int"))
dot_sp_dvf = dot_sp_dvf.withColumn('Surface_reelle_bati', dot_sp_dvf['Surface_reelle_bati'].cast("int"))

# COMMAND ----------

dot_sp_dvf.write.format("delta").save("/FileStore/tables/dvf_2021")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.dvf USING DELTA LOCATION '/FileStore/tables/dvf_2021'
