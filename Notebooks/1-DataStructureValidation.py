# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
from datetime import datetime

# COMMAND ----------

subscription =  dbutils.secrets.get("KeyVault","SubscriptionId")
appId = dbutils.secrets.get("KeyVault","SpPurviewAppId")
directoryId = dbutils.secrets.get("KeyVault","TenantId")
appKey = dbutils.secrets.get("KeyVault","SpPurviewKey")
resourceGroup = dbutils.secrets.get("KeyVault","ResourceGroupName")
queueSAS = dbutils.secrets.get("KeyVault", "QueueSAS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Landing to Raw Structure Validation

# COMMAND ----------

# MAGIC %run "/DataQuality/Config/2-PurviewAPI"

# COMMAND ----------

productEntityPath = "/mnt/lake/raw/AdventureWorks/Product"
productDataPath = "/mnt/landing/SalesLT/Product"

# COMMAND ----------

df = (spark.read.format("json").load(productDataPath))

display(df)

# COMMAND ----------

print(df.schema)

# COMMAND ----------

productSchema = getValidationSchema(entityQualifiedName = "mssql://sqlsrvr-dqframework-uks-dev.database.windows.net/AdventureWorks/SalesLT/Product")

print(productSchema)

# COMMAND ----------

# Read data
productDf = (spark.read.format("json")
  .schema(productSchema)
  .load(productDataPath)
)

display(productDf)

# COMMAND ----------

productDf = (productDf.withColumn("InputFile",input_file_name())
        .withColumn("IngestedDate", lit(datetime.now())))

(productDf.write
  .format("delta")
  .mode("overwrite")
  .save(productEntityPath)
)
