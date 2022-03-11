# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
from datetime import datetime
from delta.tables import *
import spark_df_profiling

# COMMAND ----------

# MAGIC %run "/DataQuality/Config/2-PurviewAPI"

# COMMAND ----------

# MAGIC %run "/DataQuality/Config/3-Transformation"

# COMMAND ----------

dataframe = spark.read.format("delta").load("/mnt/lake/raw/AdventureWorks/Product")

# COMMAND ----------

# Display Profile
display(dataframe)

# COMMAND ----------

# Dbutils Profile
dbutils.data.summarize(dataframe)

# COMMAND ----------

# Describe Profiling
display(dataframe.describe())

# COMMAND ----------

def profileData(inputDF, entityName):
    describeDF = inputDF.describe()
    
    columnList = []
    separator = ', '
    inputColumns = describeDF.columns
    splitColumns = separator.join(inputColumns).split(",")
    numberOfColumns = len(inputColumns)
    for columnIterator in range(numberOfColumns):
        columnList.append("'{}'".format(inputColumns[columnIterator]) + "," + splitColumns[columnIterator])

    outputColumns = separator.join(columnList)

    describeUnpivot = describeDF.selectExpr("summary", f"stack({numberOfColumns},{outputColumns}) as (ColumnName, Value)")
    
    describeUnpivot = (describeUnpivot.filter("ColumnName != 'summary'")
                       .withColumn("EntityName", lit(entityName))
                       .withColumn("ProfileDate",lit(datetime.now()))
                       .withColumn("ProfileDateKey", date_format(lit(datetime.now()),"yyyyMMdd").cast(IntegerType()))
                       .filter("summary = 'count'")
                      )
    return describeUnpivot

# COMMAND ----------

def addRejectColumns(inputDF):
    return(inputDF.withColumn("RejectRow",lit('0')) 
               .withColumn("RejectReason",lit(None).cast(StringType())) 
               .withColumn("RejectColumn",lit(None).cast(StringType())) 
               .withColumn("AssessmentDate",lit(datetime.now()))
            )

# COMMAND ----------

def dataQualityAssessment(inputDF, entityName, entityQualifiedName):
    
    rawDF = addRejectColumns(inputDF)
    
    purviewSchema = getPurviewSchema(entityQualifiedName)

    # Check validity of Purview Schema. If valid then get Transformation Rules from Purview else return None
    if purviewSchema != None:
          transformDF = getTransformationRule(purviewSchema)
    else:
        return None
    
    display(transformDF)
    
    profileDF = profileData(inputDF, entityName)
    
    cleanDF = applyDataQualityTransformations(transformDF, rawDF)
        
    badDQDF = (cleanDF
                     .filter("RejectRow != 0")
                     .withColumn("rejectReason", split(col("RejectReason"), ","))
                     .withColumn("rejectColumn", split(col("RejectColumn"), ","))
                    )

    goodDQDF = (cleanDF
                 .filter("RejectRow = 0")
                 .withColumnRenamed("RejectReason", "RejectReasons")
                 .withColumnRenamed("RejectColumn", "RejectColumns")
                )

    badDQDF = (badDQDF
               .withColumn("RejectReasons", explode("rejectReason"))
               .withColumn("RejectColumns", explode("rejectColumn"))
               .drop("rejectReason", "rejectColumn")
              )

    dataQualityDF = badDQDF.union(goodDQDF)

    dataQualityDF = (dataQualityDF
                     .withColumn("EntityName", lit(entityName).cast(StringType()))
                     .withColumn("AssessmentDateKey", date_format(lit(datetime.now()),"yyyyMMdd").cast(IntegerType()))
                    )

    dataQualityDF = dataQualityDF.select("EntityName", "AssessmentDateKey", "AssessmentDate", "RejectReasons", "RejectColumns", "RejectRow")

    dataQualityDF.write.format("delta").partitionBy("AssessmentDate").mode("append").save(f"/mnt/lake/curated/DataQuality/Fact/DataQuality/")
    
    profileDF.write.format("delta").partitionBy("ProfileDate").mode("append").save(f"/mnt/lake/curated/DataQuality/Fact/DataProfile/")
    
    return cleanDF
  

# COMMAND ----------

def writeData(inputDF, saveLocation, rejectLocation, entityName, mergeColumns):
    
    rejectDF = inputDF.filter("RejectRow = 2")
    
    validDF = inputDF.filter("RejectRow != 2")
    
    if len(validDF.take(1)) == 0:
        print("Valid Dataframe is Empty")
    else:
        # Create delta table for merge
        struct = validDF.schema
        cols = ""
        for c in struct:
            cols = (cols + f"""{c.jsonValue()["name"]} {c.jsonValue()["type"]},""" )     
        createStatement = f"CREATE TABLE IF NOT EXISTS {entityName} ({cols[:-1]}) USING DELTA LOCATION '{saveLocation}'"
        dropStatement = f"DROP TABLE IF EXISTS {entityName};"
        spark.sql(dropStatement)
        spark.sql(createStatement)
        
        detlaDf = DeltaTable.forName(spark, f"{entityName}")
        
        # Merge data into Base layer
        (detlaDf.alias("t")
          .merge(
            validDF.alias("s"), 
            f"{mergeColumns}")
          .whenMatchedUpdateAll()
          .whenNotMatchedInsertAll()
          .execute()
          )
        print(f"Valid Dataframe saved to {saveLocation}")
    
    if len(rejectDF.take(1)) == 0:
        print("Reject Dataframe is Empty")
    else:
        rejectDF.write.format("delta").partitionBy("AssessmentDate").mode("append").save(rejectLocation)
        print(f"Reject Dataframe saved to {rejectLocation}")
    
    

# COMMAND ----------

addressEntityQualifiedName = "mssql://sqlsrvr-dqframework-uks-dev.database.windows.net/AdventureWorks/SalesLT/Address"
addressEntityPath = "/mnt/lake/base/AdventureWorks/Address"
addressEntityRejectPath = "/mnt/lake/reject/AdventureWorks/Address"
addressDataPath = "/mnt/lake/raw/AdventureWorks/Address"
addressEntityName = "Address"
addressMergeColumn = "s.AddressID = t.AddressID"

# COMMAND ----------

addressRawDF = (spark
      .read
      .format("delta")
      .load(addressDataPath)
     )

# COMMAND ----------

addressRawDF = (addressRawDF
        .withColumn("LastUpdatedDate",current_timestamp())
        )

# COMMAND ----------

addressCleanDF = dataQualityAssessment(addressRawDF, addressEntityName, addressEntityQualifiedName)

display(addressCleanDF)

# COMMAND ----------

writeData(addressCleanDF, addressEntityPath, addressEntityRejectPath, addressEntityName, addressMergeColumn)

# COMMAND ----------

salesOrderDetailEntityQualifiedName = "mssql://sqlsrvr-dqframework-uks-dev.database.windows.net/AdventureWorks/SalesLT/SalesOrderDetail"
salesOrderDetailEntityPath = "/mnt/lake/base/AdventureWorks/SalesOrderDetail"
salesOrderDetailEntityRejectPath = "/mnt/lake/reject/AdventureWorks/SalesOrderDetail"
salesOrderDetailDataPath = "/mnt/lake/raw/AdventureWorks/SalesOrderDetail"
salesOrderDetailEntityName = "SalesOrderDetail"
salesOrderDetailMergeColumns = "s.SalesOrderID = t.SalesOrderID AND s.SalesOrderDetailID = t.SalesOrderDetailID"

# COMMAND ----------

salesOrderDetailRawDF = (spark
      .read
      .format("delta")
      .load(salesOrderDetailDataPath)
     )

# COMMAND ----------

salesOrderDetailRawDF = (salesOrderDetailRawDF
        .withColumn("LastUpdatedDate",current_timestamp())
        )

# COMMAND ----------

salesOrderDetailCleanDF = dataQualityAssessment(salesOrderDetailRawDF, salesOrderDetailEntityName, salesOrderDetailEntityQualifiedName)

display(salesOrderDetailCleanDF)

# COMMAND ----------

writeData(salesOrderDetailCleanDF, salesOrderDetailEntityPath, salesOrderDetailEntityRejectPath, salesOrderDetailEntityName, salesOrderDetailMergeColumns)
