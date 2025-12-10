---
title: '**Data Cleaning & Preprocessing**'
---


::: {#e5139580 .cell execution_count=1}
``` {.python .cell-code}
import os, findspark

#  Spark 4.0.1
os.environ["SPARK_HOME"] = "/opt/spark-4.0.1-bin-hadoop3"
os.environ["PATH"] = os.path.join(os.environ["SPARK_HOME"], "bin") + ":" + os.environ["PATH"]


findspark.init("/opt/spark-4.0.1-bin-hadoop3")

# create Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("JobPostingsAnalysis").getOrCreate()
```

::: {.cell-output .cell-output-stderr}
```
WARNING: Using incubator modules: jdk.incubator.vector
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
25/12/10 17:33:28 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
25/12/10 17:33:30 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
```
:::
:::


::: {#1ab9a80b .cell execution_count=2}
``` {.python .cell-code}
# Load CSV file into Spark DataFrame

df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", "\"")
        .csv("../data/lightcast_job_postings.csv")
)
```

::: {.cell-output .cell-output-stderr}
```
                                                                                
```
:::
:::


::: {#1c11abbb .cell execution_count=3}
``` {.python .cell-code}
df.count()
```

::: {.cell-output .cell-output-stderr}
```
                                                                                
```
:::

::: {.cell-output .cell-output-display execution_count=3}
```
72498
```
:::
:::



### **Which columns are irrelevant or redundant?**

The original dataset contains 131 variables, including job information, industry codes, geographic identifiers, tracking fields, and Lightcast-specific coded values. 
These fields either contain redundant information or lack interpretability; therefore, they are removed from the cleaned dataset.

1. **Multiple levels and historical versions of NAICS and SOC codes**

e.g., NAICS_2022_2, NAICS_2022_3, SOC_2021_2, which represent the same classification system with redundant layers.

2. **Location fields that contain encoded strings or latitude–longitude formats**

e.g., CITY, LOCATION, MSA_NAME_OUTGOING, rather than readable city or county names.

3. **Lightcast proprietary coded fields**

e.g., ONET, CIP, LOT_V6_ series, which only include numerical codes whose meaning cannot be interpreted directly. Since readable name fields exist, these numerical versions are unnecessary.

4. **Tracking and metadata fields**
 
such as ACTIVE_URLS, LAST_UPDATED_TIMESTAMP, and SOURCE_TYPE, which are internal system variables that do not contribute to labor market analysis.

5. **Non-analytical text fields**

e.g., MODELED_EXPIRED, DURATION unrelated to our wage, industry, or geographic comparisons.


***


## **Why are we removing multiple versions of NAICS/SOC codes?**

The dataset includes numerous versions and hierarchical levels of industry (NAICS) and occupation (SOC) codes. These repeated layers introduce several issues:

- They duplicate the same industry/occupation information across different levels.

- They may cause the same job posting to be categorized multiple times.

- They introduce unnecessary dimensions for modeling and visualization.

- They make results more difficult to interpret due to inconsistent classification levels.

To ensure clarity and standardization, we keep only:

1. NAICS_2022_6_NAME (latest and most granular industry classification) 
2. SOC_2021_4_NAME (consistent and interpretable occupation classification)


::: {#35d6dbd9 .cell execution_count=4}
``` {.python .cell-code}
columns_to_drop = [
    "ID", "URL", "ACTIVE_URLS", "DUPLICATES", "LAST_UPDATED_TIMESTAMP",
    "NAICS2", "NAICS3", "NAICS4", "NAICS5", "NAICS6",
    "SOC_2", "SOC_3", "SOC_4", "SOC_5",
    "NAICS_2022_2", "NAICS_2022_3","NAICS_2022_4","NAICS_2022_5",
    "NAICS_2022_2_NAME","NAICS_2022_3_NAME","NAICS_2022_4_NAME","NAICS_2022_5_NAME",
    "SOC_5_NAME","SOC_4_NAME","SOC_3_NAME","SOC_2_NAME",
    "NAICS2_NAME","NAICS3_NAME","NAICS4_NAME","NAICS5_NAME",
    "LAST_UPDATED_DATE","EXPIRED","DURATION",    # date variables are not related to our topic 
    "MODELED_EXPIRED","MODELED_DURATION","MODELED_EXPIRED",
    "COMPANY", "TITLE", "SKILLS",                  # remove numerical COMPANY
    "MAX_EDULEVELS","MAX_EDULEVELS_NAME",
    "EMPLOYMENT_TYPE","REMOTE_TYPE", "EMPLOYMENT_TYPE",     # remove numerical EMPLOYMENT_TYPE, REMOTE_TYPE, keeping the text version
    "LOCATION", "CITY","COUNTY","MSA","STATE", 
    "MSA_NAME", "COUNTY_NAME_OUTGOING", "COUNTY_NAME_INCOMING", "MSA_NAME_OUTGOING", "MSA_NAME_INCOMING",   
    "COUNTY_OUTGOING","COUNTY_INCOMING", "MSA_OUTGOING", "MSA_INCOMING",         # LOCATION contains latitude and longitude coordinates, CITY ontains encoded strings instead of actual city names 
    "EDUCATION_LEVELS", "MIN_EDULEVELS",  # encoded strings instead of actual EDUCATION_LEVELS, we keep EDUCATION_LEVELS_NAME. MIN_EDULEVELS contains number 
    "SPECIALIZED_SKILLS", "CERTIFICATIONS", "COMMON_SKILLS", "SOFTWARE_SKILLS",    # encoded strings
    "SOC_2021_2", "SOC_2021_2_NAME", "SOC_2021_3", "SOC_2021_3_NAME", "SOC_2021_5", "SOC_2021_5_NAME",
    "LOT_CAREER_AREA", "LOT_OCCUPATION", "LOT_SPECIALIZED_OCCUPATION", "LOT_OCCUPATION_GROUP","LOT_V6_SPECIALIZED_OCCUPATION", "LOT_V6_CAREER_AREA", "LOT_V6_OCCUPATION_GROUP",  
    "ACTIVE_SOURCES_INFO", "MAX_YEARS_EXPERIENCE", "LIGHTCAST_SECTORS", "LIGHTCAST_SECTORS_NAME", "ORIGINAL_PAY_PERIOD",   # missing values >50%
    "SOURCE_TYPES", "SOURCES", "BODY", 
    "COMPANY_NAME", "TITLE_NAME",  #keep  COMPANY_RAW and TITLE_CLEAN (simplified version)
    "ONET", "ONET_2019","CIP6", "CIP4", "CIP2",       # numerical 
    "CIP6_NAME","CIP4_NAME","CIP2_NAME",     # remove zipcode-related variables, we keep location name from state to county
    
] 

df = df.drop(*columns_to_drop)
```
:::


## **3. How will this improve analysis?**

After cleaning, the dataset is reduced from 131 variables to 38 variables, which brings several advantages:

1. **Improved interpretability**

- Removing encoded fields (e.g., CITY with codes, LOCATION with coordinates, Lightcast numeric IDs) ensures that all remaining variables contain readable, meaningful text such as state names, county names, industry names, and occupation names.

- These fields can be directly used for mapping, comparison, and reporting.

2. **Less noise, more focused analysis**

- Eliminating irrelevant or repetitive fields ensures that our wage analysis, industry structure comparisons, and geographic insights remain clean and consistent.

- Keeping only the most relevant variables avoids duplicate counting across different NAICS/SOC layers.

3. **Higher modeling efficiency**

- A smaller and cleaner feature set reduces the need for extensive dummy encoding.

- It minimizes risks of multicollinearity and improves model stability.

- Feature engineering becomes faster and more controlled.

4. **Cleaner structure for visualization and reporting**

- With only meaningful fields preserved, charts and tables become easier to interpret.

- All retained variables directly support our analytical goals.

::: {#f9caa902 .cell execution_count=5}
``` {.python .cell-code}
from pyspark.sql import functions as F

total_rows = df.count()

exprs = [
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df.columns
]

missing_row = df.agg(*exprs).collect()[0].asDict()

missing_stats = [(col, missing_row[col], missing_row[col] / total_rows)
                 for col in df.columns]

missing_df = spark.createDataFrame(
    missing_stats,
    ["column", "missing_count", "missing_ratio"]
)

missing_df.orderBy("missing_ratio", ascending=False).show(60)
```

::: {.cell-output .cell-output-stderr}
```
25/12/10 17:33:56 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
[Stage 11:>                                                         (0 + 2) / 2]
```
:::

::: {.cell-output .cell-output-stdout}
```
+--------------------+-------------+--------------------+
|              column|missing_count|       missing_ratio|
+--------------------+-------------+--------------------+
|              SALARY|        41690|  0.5750503462164474|
|           SALARY_TO|        40100|   0.553118706722944|
|         SALARY_FROM|        40100|   0.553118706722944|
|MIN_YEARS_EXPERIENCE|        23146| 0.31926397969599163|
|         COMPANY_RAW|          541|0.007462274821374383|
|         TITLE_CLEAN|          140|0.001931087754144942|
|           TITLE_RAW|          104|0.001434522331650...|
|SPECIALIZED_SKILL...|           44|6.069132941598389E-4|
| COMPANY_IS_STAFFING|           44|6.069132941598389E-4|
| CERTIFICATIONS_NAME|           44|6.069132941598389E-4|
|EDUCATION_LEVELS_...|           44|6.069132941598389E-4|
|  COMMON_SKILLS_NAME|           44|6.069132941598389E-4|
|  MIN_EDULEVELS_NAME|           44|6.069132941598389E-4|
|SOFTWARE_SKILLS_NAME|           44|6.069132941598389E-4|
|EMPLOYMENT_TYPE_NAME|           44|6.069132941598389E-4|
|           ONET_NAME|           44|6.069132941598389E-4|
|       IS_INTERNSHIP|           44|6.069132941598389E-4|
|      ONET_2019_NAME|           44|6.069132941598389E-4|
|    REMOTE_TYPE_NAME|           44|6.069132941598389E-4|
|          SOC_2021_4|           44|6.069132941598389E-4|
|           CITY_NAME|           44|6.069132941598389E-4|
|     SOC_2021_4_NAME|           44|6.069132941598389E-4|
|         COUNTY_NAME|           44|6.069132941598389E-4|
|LOT_CAREER_AREA_NAME|           44|6.069132941598389E-4|
|          STATE_NAME|           44|6.069132941598389E-4|
| LOT_OCCUPATION_NAME|           44|6.069132941598389E-4|
|         NAICS6_NAME|           44|6.069132941598389E-4|
|LOT_SPECIALIZED_O...|           44|6.069132941598389E-4|
|         SKILLS_NAME|           44|6.069132941598389E-4|
|LOT_OCCUPATION_GR...|           44|6.069132941598389E-4|
|LOT_V6_SPECIALIZE...|           44|6.069132941598389E-4|
|   LOT_V6_OCCUPATION|           44|6.069132941598389E-4|
|LOT_V6_OCCUPATION...|           44|6.069132941598389E-4|
|LOT_V6_OCCUPATION...|           44|6.069132941598389E-4|
|LOT_V6_CAREER_ARE...|           44|6.069132941598389E-4|
|        NAICS_2022_6|           44|6.069132941598389E-4|
|   NAICS_2022_6_NAME|           44|6.069132941598389E-4|
|              POSTED|           22|3.034566470799194...|
+--------------------+-------------+--------------------+

```
:::

::: {.cell-output .cell-output-stderr}
```
                                                                                
```
:::
:::


# Handling Missing Values

- Numerical fields (e.g., Salary) are filled with the median.

- Categorical fields (e.g., Industry) are replaced with "Unknown".

- Columns with >50% missing values are dropped. (We already finished that by dropping un-relevant conlumns', but salary varaibles are super important so we still keep it and we will see whatever we can do later.)

::: {#83fdee1e .cell execution_count=6}
``` {.python .cell-code}
# Fill numerical missing values
median_experience = df.approxQuantile("MIN_YEARS_EXPERIENCE", [0.5], 0.01)[0]
df = df.fillna({"MIN_YEARS_EXPERIENCE": median_experience})
```

::: {.cell-output .cell-output-stderr}
```
                                                                                
```
:::
:::


::: {#ba534de6 .cell execution_count=7}
``` {.python .cell-code}
# Fill categorical missing values
df = df.fillna({"NAICS_2022_6_NAME": "Unknown"})
```
:::


::: {#bfe4e8de .cell execution_count=8}
``` {.python .cell-code}
from pyspark.sql import functions as F

total_rows = df.count()

# Compute missing count for each column
exprs = [
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df.columns
]

missing_row = df.agg(*exprs).collect()[0].asDict()

# Convert into list of (column, missing_count)
missing_stats = [(col, missing_row[col]) for col in df.columns]


missing_df = spark.createDataFrame(
    missing_stats,
    ["column", "missing_count"]
)


missing_df.orderBy("missing_count", ascending=False).show(60)
```

::: {.cell-output .cell-output-stderr}
```
                                                                                
```
:::

::: {.cell-output .cell-output-stdout}
```
+--------------------+-------------+
|              column|missing_count|
+--------------------+-------------+
|              SALARY|        41690|
|           SALARY_TO|        40100|
|         SALARY_FROM|        40100|
|         COMPANY_RAW|          541|
|         TITLE_CLEAN|          140|
|           TITLE_RAW|          104|
|SPECIALIZED_SKILL...|           44|
| COMPANY_IS_STAFFING|           44|
| CERTIFICATIONS_NAME|           44|
|EDUCATION_LEVELS_...|           44|
|  COMMON_SKILLS_NAME|           44|
|  MIN_EDULEVELS_NAME|           44|
|SOFTWARE_SKILLS_NAME|           44|
|EMPLOYMENT_TYPE_NAME|           44|
|           ONET_NAME|           44|
|       IS_INTERNSHIP|           44|
|      ONET_2019_NAME|           44|
|    REMOTE_TYPE_NAME|           44|
|          SOC_2021_4|           44|
|           CITY_NAME|           44|
|     SOC_2021_4_NAME|           44|
|         COUNTY_NAME|           44|
|LOT_CAREER_AREA_NAME|           44|
|          STATE_NAME|           44|
| LOT_OCCUPATION_NAME|           44|
|         NAICS6_NAME|           44|
|LOT_SPECIALIZED_O...|           44|
|         SKILLS_NAME|           44|
|LOT_OCCUPATION_GR...|           44|
|LOT_V6_SPECIALIZE...|           44|
|   LOT_V6_OCCUPATION|           44|
|LOT_V6_OCCUPATION...|           44|
|LOT_V6_OCCUPATION...|           44|
|LOT_V6_CAREER_ARE...|           44|
|        NAICS_2022_6|           44|
|              POSTED|           22|
|   NAICS_2022_6_NAME|            0|
|MIN_YEARS_EXPERIENCE|            0|
+--------------------+-------------+

```
:::
:::


---
jupyter:
  kernelspec:
    display_name: .venv
    language: python
    name: python3
  language_info:
    codemirror_mode:
      name: ipython
      version: 3
    file_extension: .py
    mimetype: text/x-python
    name: python
    nbconvert_exporter: python
    pygments_lexer: ipython3
    version: 3.12.3
---
