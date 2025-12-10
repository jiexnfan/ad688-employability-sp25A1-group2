---
title: "Data Analysis"
subtitle: "Comprehensive Data Cleaning & Exploratory Analysis of Job Market Trends"
author:
  - name: Jiexin (Avery) Fan
    affiliations:
      - id: bu
        name: Boston University
        city: Boston
        state: MA
  - name: Ivan Villasmil
    affiliations:
      - id: bu
        name: Boston University
        city: Boston
        state: MA
  - name: Jiayin Liu
    affiliations:
      - id: bu
        name: Boston University
        city: Boston
        state: MA

number-sections: true
date: '2025-11-29'
date-modified: today
date-format: long

format: 
  pdf: default
  docx: default
  html:
    theme: cerulean
    toc: true
    toc-depth: 2
    number-sections: true
    df-print: paged

execute:
  echo: true
  eval: true
  freeze: auto

jupyter: python3
---

# Business Running Case; Evaluating Personal Job Market Prospects in 2025

::: {#da605a9e .cell}
``` {.python .cell-code}
# Load required libraries
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, regexp_replace, transform, when
from pyspark.sql import functions as F
from pyspark.sql.functions import col, monotonically_increasing_id, to_date, pow
import re

# Set random seed and default renderer for Plotly
np.random.seed(950)
pio.renderers.default = "notebook"

# Initialize Spark Session
spark = SparkSession.builder.appName("LightcastData").getOrCreate()

# Upload CSV file into a Spark DataFrame
df = spark.read.option("header", "true").option("inferSchema", "true").option("multiLine","true").option("escape", "\"").csv("./data/lightcast_job_postings.csv")

# Register DataFrame as a Temporary SQL table
df.createOrReplaceTempView("job_postings")

# Verify Data: Display Schema (column names & data types)
# print("---This is Diagnostic check, No need to print it in the final doc---") # Comment line when rendering the submission
# df.printSchema() # Comment line when rendering the submission

# Typecast columns to double
df = df.withColumn("SALARY", col("SALARY").cast("double"))
df = df.withColumn("SALARY_TO", col("SALARY_TO").cast("double"))
df = df.withColumn("SALARY_FROM", col("SALARY_FROM").cast("double"))
df = df.withColumn("MIN_YEARS_EXPERIENCE", col("MIN_YEARS_EXPERIENCE").cast("double"))
df = df.withColumn("MAX_YEARS_EXPERIENCE", col("MAX_YEARS_EXPERIENCE").cast("double"))
df = df.withColumn("DURATION", col("DURATION").cast("double"))
df = df.withColumn("MODELED_DURATION", col("MODELED_DURATION").cast("double"))
df = df.withColumn("IS_INTERNSHIP", col("IS_INTERNSHIP").cast("double"))
df = df.withColumn("COMPANY_IS_STAFFING", col("COMPANY_IS_STAFFING").cast("double"))

# Typecast dates to date type
df = df.withColumn("POSTED", to_date(col("POSTED"), "M/d/yyyy"))
df = df.withColumn("EXPIRED", to_date(col("EXPIRED"), "M/d/yyyy"))
df = df.withColumn("LAST_UPDATED_DATE", to_date(col("LAST_UPDATED_DATE"), "M/d/yyyy"))
df = df.withColumn("MODELED_EXPIRED", to_date(col("MODELED_EXPIRED"), "M/d/yyyy"))

# Verify Data: Display first five rows
# print("---This is Diagnostic check, No need to print it in the final doc---") # Comment line when rendering the submission
# df.show(5)  # Comment line when rendering the submission
```
:::


::: {#b19fcc49 .cell}
``` {.python .cell-code}
# Load required libraries
from pyspark.sql.functions import col, trim, when, pow
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler 
from pyspark.ml import Pipeline

# Take subset of relevant columns
relevant_columns = ["SALARY", "MIN_YEARS_EXPERIENCE", "EDUCATION_LEVELS_NAME", "EMPLOYMENT_TYPE_NAME", "REMOTE_TYPE_NAME", "DURATION", "IS_INTERNSHIP", "COMPANY_IS_STAFFING"]

df_analysis = df.select(*relevant_columns)

# Drop rows with NAs in relevant columns  *** AVOID DROPPING TO ACCESS ALL DATA ***
# df_analysis = df_analysis.dropna(subset=[
#   "SALARY", "MIN_YEARS_EXPERIENCE", 
#   "EDUCATION_LEVELS_NAME", "EMPLOYMENT_TYPE_NAME", "REMOTE_TYPE_NAME",
#   "DURATION", "IS_INTERNSHIP", "COMPANY_IS_STAFFING"
# ])

# Preview cleaned dataframe
# df_analysis.show(5, truncate=False)

# Identify unique values in categorical columns
categorical_columns = [
  "EDUCATION_LEVELS_NAME", 
  "EMPLOYMENT_TYPE_NAME", 
  "REMOTE_TYPE_NAME",
  "IS_INTERNSHIP", 
  "COMPANY_IS_STAFFING"
]

# Preview unique values in categorical columns
# for col_name in categorical_columns:
#     print(f"Unique values in {col_name}:")
#     df_analysis.select(col_name).distinct().show(truncate=False)

# Redefine unique values in REMOTE_TYPE_NAME to be: 'Remote': Remote; 'Hybrid Remote': Hybrid; 'None': Onsite; 'Not Remote': Onsite
df_analysis = df_analysis.withColumn("REMOTE_GROUP",
    when(trim(col("REMOTE_TYPE_NAME")) == "Remote", "Remote")
    .when(trim(col("REMOTE_TYPE_NAME")) == "Hybrid Remote", "Hybrid")
    .when(trim(col("REMOTE_TYPE_NAME")) == "Not Remote", "Onsite")
    .when(col("REMOTE_TYPE_NAME").isNull(), "Onsite")
    .otherwise("Onsite") 
)

# Redefine unique values in EMPLOYMENT_TYPE_NAME to be: Full-time, Part-time, Flexible
df_analysis = df_analysis.withColumn("EMPLOYMENT_GROUP",
    when(trim(col("EMPLOYMENT_TYPE_NAME")) == "Full-time (> 32 hours)", "Full-time")
    .when(trim(col("EMPLOYMENT_TYPE_NAME")) == "Part-time (â‰¤ 32 hours)", "Part-time")
    .when(trim(col("EMPLOYMENT_TYPE_NAME")) == "Part-time / full-time", "Flexible")
    .when(col("EMPLOYMENT_TYPE_NAME").isNull(), "Full-time")
    .otherwise("Flexible")
)

# Preview unique values in EMPLOYMENT_TYPE_NAME
# df_analysis.select("EMPLOYMENT_TYPE_NAME").distinct().show(truncate=False)
# df_analysis.show(5, truncate=False)

# Typecast MIN_YEARS_EXPERIENCE to categories: 0-1: Internship/Entry Level; 1-3: Junior; 3-5: Mid-Level; 5-10: Senior; 10+: Expert
df_analysis = df_analysis.withColumn("MIN_YEARS_EXPERIENCE_GROUP",
  when(col("MIN_YEARS_EXPERIENCE").between(0, 1), "Internship/Entry Level")
    .when(col("MIN_YEARS_EXPERIENCE").between (1, 3), "Junior")
    .when(col("MIN_YEARS_EXPERIENCE").between(3, 5), "Mid-Level")
    .when(col("MIN_YEARS_EXPERIENCE").between(5, 10), "Senior")
    .otherwise("Expert")
)

# Replace NULL in MIN_YEARS_EXPERIENCE to "0"
df_analysis= df_analysis.withColumn("MIN_YEARS_EXPERIENCE",
    when(col("MIN_YEARS_EXPERIENCE").isNull(), 0)
    .otherwise(col("MIN_YEARS_EXPERIENCE")))

# Preview typacasted values in MIN_YEARS_EXPERIENCE_GROUP
# df_analysis.select("MIN_YEARS_EXPERIENCE_GROUP").distinct().show(truncate=False)
# df_analysis.show(5, truncate=False)

# 3. Replace NULL & zeros in Duration to "1"
df_analysis = df_analysis.withColumn("DURATION",
    when(col("DURATION").isNull(), 1)
    .when(col("DURATION") == 0, 1)
    .otherwise(col("DURATION")))

# Preview cleaned MIN_YEARS_EXPERIENCE_GROUP
# df_analysis.select("MIN_YEARS_EXPERIENCE_GROUP").distinct().show(truncate=False)
# df_analysis.show(5, truncate=False)

# 4. Remove [\n \n] characters from EDUCATION_LEVELS_NAME
df_analysis = df_analysis.withColumn(
    "EDUCATION_LEVELS_CLEAN",
    F.trim(F.regexp_replace(F.col("EDUCATION_LEVELS_NAME"), r'[\[\]\n"]', ''))
).drop("EDUCATION_LEVELS_NAME")

# Preview cleaned EDUCATION_LEVELS_NAME
# df_analysis.select("EDUCATION_LEVELS_CLEAN").distinct().show(truncate=False)
# df_analysis.show(5, truncate=False)

# df_analysis.select("EMPLOYMENT_GROUP").distinct().show(truncate=False)
# df_analysis.show(5, truncate=False)

# Drop reformated/redundant columns
df_analysis = df_analysis.drop("EMPLOYMENT_TYPE_NAME", "REMOTE_TYPE_NAME", "EDUCATION_LEVELS_NAME")
df_analysis.show(5, truncate=False)

```
:::


