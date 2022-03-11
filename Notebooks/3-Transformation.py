# Databricks notebook source
import json
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession

# COMMAND ----------

def applyReplaceSQL(df, attribute, transformation):
    exprN = (transformation)
    df = df.withColumn(attribute, expr(exprN))
    return df

# COMMAND ----------

def applyDataQualityTransformations(tdf, inputdf):
    # One by one, apply these transformations to the dataframe
    inputdf = inputdf.alias("parentdf")

    for row in tdf.rdd.collect():
        rejectCode = 0
        if row.RuleType == "Advise":
            rejectCode = 1
        elif row.RuleType == "Quarantine":
            rejectCode = 2

        if rejectCode > 0:
            inputdf = applyReplaceSQL(
                inputdf,
                "RejectRow",
                f"CASE WHEN {row.TransformationRule} THEN {rejectCode} ELSE RejectRow END",
            )  # noqa 511
            inputdf = applyReplaceSQL(
                inputdf,
                "RejectReason",
                f"CASE WHEN {row.TransformationRule} THEN CONCAT(coalesce(RejectReason,''), CASE WHEN RejectReason IS NULL THEN '' ELSE ',' END, "
                + f"'{row.RejectionReason}') ELSE RejectReason END",
            )  # noqa 511
            inputdf = applyReplaceSQL(
                inputdf,
                "RejectColumn",
                f"CASE WHEN {row.TransformationRule} THEN CONCAT(coalesce(RejectColumn,''), CASE WHEN RejectColumn IS NULL THEN '' ELSE ',' END, "
                + f"'{row.ColumnName}') ELSE RejectColumn END",
            )  # noqa 511
    return inputdf

# COMMAND ----------

def applyDataValidationChecks(tdf, inputdf):
    # One by one, apply these transformations to the dataframe
    inputdf = inputdf.alias("parentdf")

    for row in tdf.rdd.collect():
        if "email" in row.Classification.lower():
            validationCode = 1
        if "phone" in row.Classification.lower():
            validationCode = 2

        if validationCode > 0:
            inputdf = applyReplaceSQL(
                inputdf,
                "ValidationType",
                f"CASE WHEN {row.TransformationRule} THEN {validationCode} ELSE ValidationType END"
            ) 
    return inputdf

# COMMAND ----------

# customerSchema = "CustomerID integer, NameStyle string, Title string, FirstName string, MiddleName string, LastName string, Suffix string, CompanyName string, SalesPerson string, EmailAddress string, Phone string, rowguid string, ModifiedDate timestamp"
# customerEntityQualifiedName = "mssql://ust-datalake-sql-uks.database.windows.net/AdventureWorks/SalesLT/Customer"
# customerEntityPath = "/mnt/lake/base/AdventureWorks/Customer"
# customerEntityRejectPath = "/mnt/lake/base/AdventureWorks/Customer/_reject"
# customerDataPath = "/mnt/lake/raw/AdventureWorks/Customer"
# customerEntityName = "Customer"
# customerMergeColumns = "s.CustomerID = t.CustomerID"

# customerRawDF = (spark
#       .read
#       .format("delta")
#       .load(customerDataPath)
#      )

# COMMAND ----------

# columns = ["ColumnName","Classification"]
# data = [("EmailAddress", "MICROSOFT.PERSONAL.EMAIL"), 
#         ("FirstName", "MICROSOFT.PERSONAL.NAME"), 
#         ("LastName", "MICROSOFT.PERSONAL.NAME"),
#         ("Phone", "MICROSOFT.PERSONAL.US.PHONE_NUMBER")
#        ]

# customerAttributeClassification = spark.createDataFrame(data).toDF(*columns)


# COMMAND ----------

# MAGIC %run "/DataQuality/Config/4-Validator"

# COMMAND ----------

# for row in customerAttributeClassification.rdd.collect():
#     if "email" in row.Classification.lower():
#         emailValidationDf = applyReplaceSQL(
#                 customerRawDF,
#                 "EmailValidation",
#                 f"{row.ColumnName}",
#             ) 
#         for row in emailValidationDf.rdd.collect():
            
#         display(emailValidationDf)
#     if "phone" in row.Classification.lower():
#         print("Phone Validation")

# COMMAND ----------


