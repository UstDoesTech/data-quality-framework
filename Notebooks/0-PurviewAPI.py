# Databricks notebook source
import os, json, jmespath
from pyapacheatlas.core import AtlasEntity
from pyapacheatlas.core.util import GuidTracker
from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess, TypeCategory
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core.typedef import AtlasAttributeDef, EntityTypeDef, RelationshipTypeDef
from pyspark.sql.types import StructType,StructField, StringType, ArrayType
from pyspark.sql.functions import explode, expr

# COMMAND ----------

tenant_id = dbutils.secrets.get("KeyVault","TenantId")
client_id = dbutils.secrets.get("KeyVault","SpPurviewAppID")
client_secret = dbutils.secrets.get("KeyVault","SpPurviewKey")
purview_account_name = dbutils.secrets.get("KeyVault","PurviewAccountName")

# COMMAND ----------

oauth = ServicePrincipalAuthentication(
        tenant_id=os.environ.get("TENANT_ID", tenant_id),
        client_id=os.environ.get("CLIENT_ID", client_id),
        client_secret=os.environ.get("CLIENT_SECRET", client_secret)
    )
client = PurviewClient(
    account_name = os.environ.get("PURVIEW_NAME", purview_account_name),
    authentication=oauth
)
guid = GuidTracker()

# COMMAND ----------

def getValidationSchema (entityQualifiedName: str):
  
    entity = client.get_entity(qualifiedName=entityQualifiedName, typeName="azure_sql_table")
    schema = ""

    for column in entity["entities"][0]['relationshipAttributes']['columns']:
        columnAttributes = client.get_entity(column['guid'])
        columnName = columnAttributes['entities'][0]['attributes']['name']
        columnType = columnAttributes['entities'][0]['attributes']['data_type']
        if columnType == "datetime":
            columnType = "timestamp"
        if columnType == "int":
            columnType = "integer"
        if columnType == "money":
            columnType = "double"
        if columnType == "decimal":
            columnType = "double"
        if columnType == "uniqueidentifier":
            columnType = "string"
        if columnType == "nvarchar":
            columnType = "string"
        if columnType == "varchar":
            columnType = "string"
        if columnType == "varbinary":
            columnType = "string"
        schemaColumn = columnName + ' ' + columnType + ', '
        schema += schemaColumn
    
    schema = schema[:-2]
    
    return schema

# COMMAND ----------

validationSchema = getValidationSchema("mssql://sqlsrvr-dqframework-uks-dev.database.windows.net/AdventureWorks/SalesLT/Product")
print(validationSchema)

# COMMAND ----------

def getPurviewSchema (entityQualifiedName: str):
  
    entity = client.get_entity(qualifiedName=entityQualifiedName, typeName="azure_sql_table")
    transformList = []
    
    for column in entity["entities"][0]['relationshipAttributes']['columns']:
        columnAttributes = client.get_entity(column["guid"])
        columnName = columnAttributes['entities'][0]['attributes']['name']
        for meaning in columnAttributes["entities"][0]["relationshipAttributes"]["meanings"]:
            transformList.append([columnName, meaning["guid"]])
  
    schema = "ColumnName string, TransformationGuid string"
    purviewSchema = spark.createDataFrame(transformList, schema)
    
    return purviewSchema

# COMMAND ----------

purviewSchema = getPurviewSchema("mssql://sqlsrvr-dqframework-uks-dev.database.windows.net/AdventureWorks/SalesLT/Product")
display(purviewSchema)

# COMMAND ----------

def getTransformationRule (purviewSchema):
  schema = "ColumnName string, TransformationRule string, RuleType string, RejectionReason string"
  emptyRDD = spark.sparkContext.emptyRDD()
  transformdf = spark.createDataFrame(emptyRDD,schema)
  
  for row in purviewSchema.rdd.collect():
    term = client.glossary.get_term(guid = row[1])
    transformationRule = term["attributes"]["Data Quality"]["Data Quality Rule"]
    ruleType = term["attributes"]["Data Quality"]["Data Quality Rule Type"]
    rejectionReason = term["attributes"]["Data Quality"]["Data Quality Failure Reason"]
    
    # Inject Column Name if there is a placeholder in the transformation rule if it exists
    transformExpn = f'{(transformationRule).format(row[0])}'
    
    newRow = spark.createDataFrame([(row[0], transformExpn, ruleType, rejectionReason)], schema)
    transformdf = transformdf.union(newRow)
  
  return transformdf

# COMMAND ----------

transformdf = getTransformationRule(purviewSchema)
display(transformdf)

# COMMAND ----------

def getAttributeClassification(entityQualifiedName):
    entity = client.get_entity(qualifiedName=entityQualifiedName, typeName="azure_sql_table")
    assetSchema = jmespath.search("[referredEntities.*.[attributes.name, classifications[0].[typeName][0]]]", entity)[0]
    
    schema = "ColumnName string, Classification string"
    classificationSchema = spark.createDataFrame(assetSchema, schema).filter("Classification is not null")
    
    return classificationSchema

# COMMAND ----------

attributeClassification = getAttributeClassification("mssql://sqlsrvr-dqframework-uks-dev.database.windows.net/AdventureWorks/SalesLT/Customer")
display(attributeClassification)
