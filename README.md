# Data Quality Framework

This repo contains the code as demonstrated in the session: Implementing a Data Quality Framework in Purview. 

## Prerequisities

- A Purview Account
- An instance of Spark running Python, such as Databricks or Synapse Spark Pools. Alternatively, some of this can be executed solely within Python - allowing for adaptation for non-Spark environments
- An Azure Key Vault, or secret store, containing all the secrets and credentials used to connect to the Purview Account

## Navigation

The example notebooks are contained within the folder `Notebooks`. They should be executed in order from 0 through to 3, in order to emulate the demo.

In due course, there will be a full python library which will extend upon the pyapacheatlas library - to manipulate and abstract the API calls for the purposes of Data Quality Extraction.
