// Databricks notebook source
// MAGIC %scala 
// MAGIC 
// MAGIC dbutils.widgets.text("database", "")
// MAGIC dbutils.widgets.text("table", "")

// COMMAND ----------

var database = dbutils.widgets.get("database")
var table = dbutils.widgets.get("table")

def isEmpty(x: String) = Option(x).forall(_.isEmpty)

if (isEmpty(database)) {
  throw new IllegalArgumentException("missing required option: database")
}

if (isEmpty(table)) {
  throw new IllegalArgumentException("missing required option: table")
}

println(s"database=${database}")
println(s"table=${table}")

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC package org.apache.spark.sql.hive {
// MAGIC   
// MAGIC import org.apache.spark.sql.hive.HiveUtils
// MAGIC import org.apache.spark.SparkContext
// MAGIC 
// MAGIC object utils {
// MAGIC     def dropTable(sc: SparkContext, dbName: String, tableName: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = {
// MAGIC       HiveUtils
// MAGIC           .newClientForMetadata(sc.getConf, sc.hadoopConfiguration)
// MAGIC           .dropTable(dbName, tableName, ignoreIfNotExists, false)
// MAGIC     }
// MAGIC   }
// MAGIC }

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.hive.utils
// MAGIC utils.dropTable(spark.sparkContext, database, table, true, true)
