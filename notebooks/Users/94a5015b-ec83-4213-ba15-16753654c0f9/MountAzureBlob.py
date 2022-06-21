# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

containername = "customcontainer"
storagename = "azurebst09"
mountpoint = "/mnt/custommount"


try:
    dbutils.fs.mount(source = "wasbs://" + containername + "@" + storagename + ".blob.core.windows.net",
                     mount_point = mountpoint,
                     extra_configs = {"fs.azure.account.key." + storagename + ".blob.core.windows.net": dbutils.secrets.get(scope = "dbstoragescope", key = "secret")}
    )
except Exception as e:
    print(f'already mounted, please unmount using dbutils.fs.unmount("{mountpoint}")')

# COMMAND ----------

# MAGIC %fs head /mnt/custommount/SmallDatasetTravel.csv

# COMMAND ----------



# COMMAND ----------

# MAGIC %scala
# MAGIC val mydataframe = spark.read.option("header", true).option("inferSchema", true).csv("/mnt/custommount/")
# MAGIC display(mydataframe)

# COMMAND ----------

# MAGIC %scala
# MAGIC val selexp = mydataframe.select("DEST", "ORIG", "count")
# MAGIC display(selexp)

# COMMAND ----------

# MAGIC %scala
# MAGIC val renamedata = selexp.withColumnRenamed("DEST", "Destination_Country")
# MAGIC display(renamedata)

# COMMAND ----------

# MAGIC %scala
# MAGIC // display(renamedata.describe())
# MAGIC // val filter = renamedata.where("count > 49")
# MAGIC // display(filter)
# MAGIC display(renamedata)
# MAGIC renamedata.createOrReplaceTempView("usertraveldata")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from usertraveldata
# MAGIC select ORIG, sum(count)
# MAGIC from usertraveldata
# MAGIC group by ORIG
# MAGIC order by sum(count) desc

# COMMAND ----------

# MAGIC %scala
# MAGIC val agg_data = spark.sql(""" select ORIG, sum(count)
# MAGIC from usertraveldata
# MAGIC group by ORIG
# MAGIC order by sum(count) desc """)
# MAGIC 
# MAGIC agg_data.write.option("header", "true").format("com.databricks.spark.csv").save("/mnt/custommount/traveloutput.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists databrickstables

# COMMAND ----------

# MAGIC %scala
# MAGIC val mydataframe2 = spark.read.option("header", true).option("inferSchema", true).csv("dbfs:/FileStore/tables/createdviadatabricksdb/*csv")
# MAGIC val selexp2 = mydataframe2.select("DEST", "ORIG", "count")
# MAGIC val renamedata2 = selexp2.withColumnRenamed("DEST", "Destination_Country")
# MAGIC display(renamedata2)
# MAGIC renamedata2.createOrReplaceTempView("usertraveldata2")
# MAGIC val agg_data2 = spark.sql(""" select ORIG, sum(count)
# MAGIC from usertraveldata2
# MAGIC group by ORIG
# MAGIC order by sum(count) desc """)
# MAGIC 
# MAGIC agg_data2.write.option("header", "true").format("com.databricks.spark.csv").saveAsTable("databrickstables.travel_1and2_orig_aggregate")

# COMMAND ----------

