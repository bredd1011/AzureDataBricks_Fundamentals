// Databricks notebook source
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType}

var fireschema = StructType(Array(StructField("IncidentNumber", StringType, true),
                                  StructField("ExposureNumber", IntegerType, true),
                                  StructField("ID", StringType, true),
                                  StructField("Address", StringType, true),
                                  StructField("IncidentDate", StringType, true),
                                  StructField("CallNumber", StringType, true),
                                  StructField("AlarmDtTm", StringType, true),
                                  StructField("ArrivalDtTm", StringType, true),
                                  StructField("CloseDtTm", StringType, true),
                                  StructField("City", StringType, true),
                                  StructField("zipcode", StringType, true),
                                  StructField("Battalion", StringType, true),
                                  StructField("StationArea", StringType, true),
                                  StructField("Box", StringType, true),
                                  StructField("SuppressionUnits", IntegerType, true),
                                  StructField("SuppressionPersonnel", IntegerType, true),
                                  StructField("EMSUnits", IntegerType, true),
                                  StructField("EMSPersonnel", IntegerType, true),
                                  StructField("OtherUnits", IntegerType, true),
                                  StructField("OtherPersonnel", IntegerType, true),
                                  StructField("FirstUnitOnScene", StringType, true),
                                  StructField("EstPropertyLoss", IntegerType, true),
                                  StructField("EstContentsLoss", IntegerType, true),
                                  StructField("FireFatalities", IntegerType, true),
                                  StructField("FireInjuries", IntegerType, true),
                                  StructField("CivilianFatalities", IntegerType, true),
                                  StructField("CivilianInjuries", IntegerType, true),
                                  StructField("NumberOfAlarms", IntegerType, true),
                                  StructField("PrimarySituation", StringType, true),
                                  StructField("MutualAid", StringType, true),
                                  StructField("ActionTakenPrimary", StringType, true)))

val FireFile = "/FileStore/tables/Fire_Incidents.csv"
val fireDF = spark.read.schema(fireschema).option("header", "true").csv(FireFile)
display(fireDF)

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/tables/

// COMMAND ----------

val CivilianFatal = fireDF.select("IncidentNumber", "Address", "City", "zipcode", "AlarmDtTm", "CivilianFatalities", "PrimarySituation", "ActionTakenPrimary").where($"CivilianFatalities" > 0 )
CivilianFatal.show()

// COMMAND ----------

import org.apache.spark.sql.functions._

fireDF.select("IncidentNumber").where(col("CivilianFatalities")>0).agg(countDistinct("IncidentNumber") as "DistinctFatalCalls").show()

// COMMAND ----------

fireDF.select("CivilianFatalities").where(col("CivilianFatalities")>0).groupBy("CivilianFatalities").count().orderBy(desc("count")).show()

// COMMAND ----------

val newFireDF = fireDF.withColumnRenamed("ActionTakenPrimary", "PrimaryAction")
newFireDF.select("PrimaryAction").where(col("CivilianFatalities")>0).groupBy("PrimaryAction").count().orderBy(desc("count")).show()

// COMMAND ----------

import org.apache.spark.sql.{functions => F}
newFireDF.select(F.sum("NumberOfAlarms"), F.avg("CivilianFatalities"), F.min("CivilianFatalities"), F.max("CivilianFatalities")).show()

// COMMAND ----------

// MAGIC %python
// MAGIC print('git_commit_add_on')

// COMMAND ----------

