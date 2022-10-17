# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT 
# MAGIC  'DVF_ID' ,'CreatedDate' ,'Reference_document' ,'No_disposition' ,'Date_mutation' ,'Nature_mutation' ,'Valeur_fonciere' ,'No_voie' ,'B/T/Q' ,'Type_de_voie' ,'Code_voie' ,'Voie' ,'Code_postal' ,'Commune' ,'Code_departement' ,'Code_commune' ,'Prefixe_de_section' ,'Section' ,'No_plan' ,'Code_type_local' ,'Type_local' ,'Surface_reelle_bati' ,'Nombre_pieces_principales' ,'Nature_culture' ,'Nature_culture_speciale' ,'Surface_terrain'
# MAGIC FROM silver.dvf_table

# COMMAND ----------

DVF_table = spark.sql('''
SELECT  DVF_ID ,CreatedDate ,Reference_document ,No_disposition ,Date_mutation ,Nature_mutation ,Valeur_fonciere ,No_voie ,`B/T/Q` ,Type_de_voie ,Code_voie ,Voie ,Code_postal ,Commune ,Code_departement ,Code_commune ,Prefixe_de_section ,Section ,No_plan ,Code_type_local ,Type_local ,Surface_reelle_bati ,Nombre_pieces_principales ,Nature_culture ,Nature_culture_speciale ,Surface_terrain
FROM silver.dvf_table
''')
DVF_table.distinct().createOrReplaceTempView('dvf_file_updates')

# COMMAND ----------

#Create dvf table
#DVF_table.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/gold/dvf_table") 
#spark.sql("CREATE TABLE IF NOT EXISTS gold.dvf_table USING DELTA LOCATION '/FileStore/gold/dvf_table'")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.dvf_table
# MAGIC USING dvf_file_updates
# MAGIC ON gold.dvf_table.DVF_ID = dvf_file_updates.DVF_ID
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
