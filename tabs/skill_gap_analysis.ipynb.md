---
title: '**1. team-based skill dataframe**'
---


::: {#e13dc00b .cell execution_count=78}
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
:::


::: {#2d06956b .cell execution_count=79}
``` {.python .cell-code}
# Load CSV file into Spark DataFrame

df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("escape", "\"")
        .csv("../data/lightcast_job_postings (for skills analysis).csv")
)
```

::: {.cell-output .cell-output-stderr}
```
                                                                                
```
:::
:::


::: {#67ea3ec0 .cell execution_count=80}
``` {.python .cell-code}
df.count()
```

::: {.cell-output .cell-output-stderr}
```
                                                                                
```
:::

::: {.cell-output .cell-output-display execution_count=80}
```
72498
```
:::
:::




::: {#eb29fe8d .cell execution_count=88}
``` {.python .cell-code}
import pandas as pd

skills_data = {
    "Name": ["Ivan", "Avery", "Jiayin"],
    "Python": [3, 3, 4],
    "SQL": [2, 2, 3],
    "R": [2, 2, 3],
    "Tableau": [3, 2, 3],
    "Power BI": [3, 3, 3],
    "Excel": [5, 4, 4],
    "Statistics": [3, 3, 3],
    "Data Cleaning": [2, 3, 3],
    "Communication": [4, 3, 3]
}

df_skills = pd.DataFrame(skills_data)
df_skills.set_index("Name", inplace=True)


mean_row = df_skills.mean().round(2).to_frame().T
mean_row.index = ["Team Average"]


df_with_avg = pd.concat([df_skills, mean_row])



df_with_avg

```

::: {.cell-output .cell-output-display execution_count=88}
```{=html}
<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Python</th>
      <th>SQL</th>
      <th>R</th>
      <th>Tableau</th>
      <th>Power BI</th>
      <th>Excel</th>
      <th>Statistics</th>
      <th>Data Cleaning</th>
      <th>Communication</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>Ivan</th>
      <td>3.00</td>
      <td>2.00</td>
      <td>2.00</td>
      <td>3.00</td>
      <td>3.0</td>
      <td>5.00</td>
      <td>3.0</td>
      <td>2.00</td>
      <td>4.00</td>
    </tr>
    <tr>
      <th>Avery</th>
      <td>3.00</td>
      <td>2.00</td>
      <td>2.00</td>
      <td>2.00</td>
      <td>3.0</td>
      <td>4.00</td>
      <td>3.0</td>
      <td>3.00</td>
      <td>3.00</td>
    </tr>
    <tr>
      <th>Jiayin</th>
      <td>4.00</td>
      <td>3.00</td>
      <td>3.00</td>
      <td>3.00</td>
      <td>3.0</td>
      <td>4.00</td>
      <td>3.0</td>
      <td>3.00</td>
      <td>3.00</td>
    </tr>
    <tr>
      <th>Team Average</th>
      <td>3.33</td>
      <td>2.33</td>
      <td>2.33</td>
      <td>2.67</td>
      <td>3.0</td>
      <td>4.33</td>
      <td>3.0</td>
      <td>2.67</td>
      <td>3.33</td>
    </tr>
  </tbody>
</table>
</div>
```
:::
:::


::: {#51c48c4f .cell execution_count=83}
``` {.python .cell-code}
import seaborn as sns
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
sns.heatmap(
    df_skills, 
    annot=True, 
    cmap="coolwarm", 
    linewidths=0.5,
    linecolor='white'
)

plt.title("Team Skill Levels Heatmap", fontsize=20, fontweight='bold')
plt.xticks(rotation=30)
plt.yticks(rotation=0)
plt.tight_layout()
plt.show()

```

::: {.cell-output .cell-output-display}
![](skill_gap_analysis_files/figure-ipynb/cell-6-output-1.png){}
:::
:::


### **Ivan**

* Technical skills range from basic to intermediate, with Python, SQL, and R at an introductory level.
* Strong proficiency in Excel, with solid capability in data organization and presentation.
* Communication stands out as the key strength, suitable for tasks involving cross-team collaboration and result delivery.

---

### **Avery**

* Balanced skill profile with steady performance in core data-processing abilities such as Python, SQL, R, and Data Cleaning.
* Confident in Excel for routine analysis and reporting tasks.
* A well-rounded analyst with stable foundational skills across all areas.

---

### **Jiayin**

* Stronger technical orientation, especially with higher proficiency in Python and Excel.
* Maintains consistent intermediate-level ability across SQL, R, visualization tools, and data cleaning.
* A comprehensive technical profile, suitable for more automation-, modeling-, or technically focused analytical tasks.

# **2. Compare team skills to industry requirements**

## **2.1 Extract most in-demand skills from IT job postings.**

We selected **SOFTWARE_SKILLS_NAME** and **SPECIALIZED_SKILLS_NAME** as the sources for extracting skills because:

* These two fields focus on the core technical skills required in IT and data-related roles.
* **SOFTWARE_SKILLS_NAME** includes essential tools and software skills such as Python, SQL, Tableau, and AWS.
* **SPECIALIZED_SKILLS_NAME** provides additional domain-specific technical skills, including Machine Learning, Data Modeling, and Forecasting.
* Compared with other skill fields (e.g., *COMMON_SKILLS_NAME*), these two are more technical and therefore more suitable for identifying the most in-demand skills in the job market.


::: {#e7466288 .cell execution_count=84}
``` {.python .cell-code}
from pyspark.sql.functions import col, concat_ws

df_preview = df.select(
    "SOFTWARE_SKILLS_NAME",
    "SPECIALIZED_SKILLS_NAME"
).withColumn(
    "merged_skills",
    concat_ws(", ", col("SOFTWARE_SKILLS_NAME"), col("SPECIALIZED_SKILLS_NAME"))
)

df_preview = df_preview.select("merged_skills")
df_preview.show(10, truncate=False)
```

::: {.cell-output .cell-output-stdout}
```
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|merged_skills                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|[\n  "SQL (Programming Language)",\n  "Power BI"\n], [\n  "Merchandising",\n  "Predictive Modeling",\n  "Data Modeling",\n  "Advanced Analytics",\n  "Data Extraction",\n  "Statistical Analysis",\n  "Data Mining",\n  "Business Analysis",\n  "Finance",\n  "Algorithms",\n  "Statistics",\n  "SQL (Programming Language)",\n  "Ad Hoc Reporting",\n  "Power BI",\n  "Economics"\n]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|[\n  "Oracle Business Intelligence (BI) / OBIA",\n  "Oracle E-Business Suite",\n  "PL/SQL",\n  "Oracle Fusion Middleware"\n], [\n  "Procurement",\n  "Financial Statements",\n  "Oracle Business Intelligence (BI) / OBIA",\n  "Oracle E-Business Suite",\n  "PL/SQL",\n  "Supply Chain",\n  "Business Intelligence",\n  "Oracle Fusion Middleware",\n  "Project Accounting"\n]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|[\n  "Microsoft Office"\n], [\n  "Exception Reporting",\n  "Data Analysis",\n  "Data Integrity"\n]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|[\n  "SAS (Software)",\n  "Google Cloud Platform (GCP)"\n], [\n  "Exit Strategies",\n  "User Story",\n  "Hardware Configuration Management",\n  "On Prem",\n  "Agile Methodology",\n  "Solution Design",\n  "Advanced Analytics",\n  "Reengineering",\n  "Cross-Functional Collaboration",\n  "Requirements Elicitation",\n  "Business Analysis",\n  "Data Management",\n  "Data Architecture",\n  "Market Trend",\n  "Business Valuation",\n  "Systems Development Life Cycle",\n  "Test Planning",\n  "Multi-Tenant Cloud Environments",\n  "Scrum (Software Development)",\n  "Project Management",\n  "Data Migration",\n  "Regulatory Compliance",\n  "Product Roadmaps",\n  "SAS (Software)",\n  "Software As A Service (SaaS)",\n  "Data Domain",\n  "Product Requirements",\n  "Data Governance",\n  "Competitive Intelligence",\n  "Operations Architecture",\n  "Risk Appetite",\n  "Google Cloud Platform (GCP)",\n  "User Feedback"\n]                                                                                                                                                                                                                                                                                                                                                              |
|[], []                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|[\n  "Power BI",\n  "Qlik Sense (Data Analytics Software)",\n  "PHP Development",\n  "Python (Programming Language)",\n  "Dashboard",\n  "Looker Analytics",\n  "Microsoft SQL Servers",\n  "PL/SQL",\n  "Database Management Systems",\n  "Visual Basic For Applications",\n  "Bash (Scripting Language)",\n  "Tableau (Business Intelligence Software)"\n], [\n  "Power BI",\n  "Qlik Sense (Data Analytics Software)",\n  "Business Analysis",\n  "Business Process",\n  "PHP Development",\n  "Python (Programming Language)",\n  "Dashboard",\n  "Key Performance Indicators (KPIs)",\n  "Looker Analytics",\n  "Microsoft SQL Servers",\n  "PL/SQL",\n  "Database Management Systems",\n  "Visual Basic For Applications",\n  "Business Intelligence",\n  "Data Analysis",\n  "Business Operations",\n  "Ad Hoc Reporting",\n  "Extract Transform Load (ETL)",\n  "Bash (Scripting Language)",\n  "Tableau (Business Intelligence Software)"\n]                                                                                                                                                                                                                                                                                                                                                           |
|[\n  "Interactive Data Language (IDL)"\n], [\n  "Interactive Data Language (IDL)",\n  "Business Metrics",\n  "Data Analysis"\n]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|[\n  "Microsoft Office"\n], [\n  "Exception Reporting",\n  "Data Analysis",\n  "Data Integrity"\n]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|[\n  "JavaScript (Programming Language)",\n  "Order Management Systems",\n  "SAP Sales And Distribution",\n  "SAP Applications"\n], [\n  "JavaScript (Programming Language)",\n  "Order Management",\n  "Order Management Systems",\n  "Order To Cash Process",\n  "Sales Order",\n  "SAP Sales And Distribution",\n  "Purchasing Process",\n  "Credit Risk Management",\n  "Consignment",\n  "Billing",\n  "SAP Applications",\n  "Purchasing"\n]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|[\n  "Salesforce",\n  "Google Docs",\n  "Productivity Software",\n  "Google Slides",\n  "Google Sheets",\n  "Microsoft PowerPoint",\n  "Google Analytics",\n  "Python (Programming Language)",\n  "Dashboard",\n  "SQL (Programming Language)",\n  "Microsoft Office",\n  "Microsoft Excel",\n  "SAS (Software)",\n  "Tableau (Business Intelligence Software)",\n  "R (Programming Language)"\n], [\n  "Salesforce",\n  "Google Docs",\n  "Productivity Software",\n  "Google Slides",\n  "Business Analytics",\n  "Customer Engagement",\n  "Data Mining",\n  "Data Science",\n  "Statistical Modeling",\n  "Data Analysis",\n  "Google Analytics",\n  "Python (Programming Language)",\n  "Ad Hoc Analysis",\n  "Marketing Management",\n  "Dashboard",\n  "Market Research",\n  "Information Systems",\n  "Analytics",\n  "Statistics",\n  "Operations Research",\n  "Machine Learning",\n  "Business Requirements",\n  "Marketing",\n  "Key Performance Indicators (KPIs)",\n  "SQL (Programming Language)",\n  "Marketing Performance Measurement And Management",\n  "SAS (Software)",\n  "Customer Relationship Management",\n  "Economics",\n  "Analytical Techniques",\n  "Data Visualization",\n  "Tableau (Business Intelligence Software)",\n  "Computer Science",\n  "R (Programming Language)"\n]|
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
only showing top 10 rows
```
:::
:::


::: {#2a518bac .cell execution_count=85}
``` {.python .cell-code}
from pyspark.sql.functions import regexp_replace, trim, split, explode, col


df_clean = df_preview.withColumn(
    "merged_clean",
    regexp_replace("merged_skills", r"[\[\]\n\"]", "")
)


df_clean = df_clean.withColumn(
    "skill_array",
    split(col("merged_clean"), ",")
)


df_clean = df_clean.withColumn(
    "skill_array",
    split(trim(col("merged_clean")), ",\s*")
)


df_exploded = df_clean.select(
    explode("skill_array").alias("skill")
)


df_exploded = df_exploded.filter(trim(col("skill")) != "")

df_exploded.show(20, truncate=False)
```

::: {.cell-output .cell-output-stdout}
```
+----------------------------------------+
|skill                                   |
+----------------------------------------+
|SQL (Programming Language)              |
|Power BI                                |
|Merchandising                           |
|Predictive Modeling                     |
|Data Modeling                           |
|Advanced Analytics                      |
|Data Extraction                         |
|Statistical Analysis                    |
|Data Mining                             |
|Business Analysis                       |
|Finance                                 |
|Algorithms                              |
|Statistics                              |
|SQL (Programming Language)              |
|Ad Hoc Reporting                        |
|Power BI                                |
|Economics                               |
|Oracle Business Intelligence (BI) / OBIA|
|Oracle E-Business Suite                 |
|PL/SQL                                  |
+----------------------------------------+
only showing top 20 rows
```
:::

::: {.cell-output .cell-output-stderr}
```
<>:18: SyntaxWarning: invalid escape sequence '\s'
<>:18: SyntaxWarning: invalid escape sequence '\s'
/tmp/ipykernel_1774/3409365684.py:18: SyntaxWarning: invalid escape sequence '\s'
  split(trim(col("merged_clean")), ",\s*")
```
:::
:::


::: {#f16be32d .cell execution_count=86}
``` {.python .cell-code}
from pyspark.sql.functions import col

top_skills = (
    df_exploded
    .groupBy("skill")
    .count()
    .orderBy(col("count").desc())
)

top_skills.show(20, truncate=False)
```

::: {.cell-output .cell-output-stderr}
```
[Stage 89:>                                                         (0 + 1) / 1]
```
:::

::: {.cell-output .cell-output-stdout}
```
+----------------------------------------+-----+
|skill                                   |count|
+----------------------------------------+-----+
|SQL (Programming Language)              |43466|
|Data Analysis                           |28515|
|Python (Programming Language)           |24506|
|SAP Applications                        |24246|
|Dashboard                               |23712|
|Tableau (Business Intelligence Software)|23562|
|Power BI                                |21700|
|Computer Science                        |17287|
|Project Management                      |13711|
|Business Process                        |13278|
|Business Requirements                   |13035|
|Microsoft Excel                         |12826|
|Finance                                 |12438|
|R (Programming Language)                |12208|
|Business Intelligence                   |10423|
|Microsoft Azure                         |9518 |
|Amazon Web Services                     |9384 |
|Agile Methodology                       |9131 |
|Data Management                         |8691 |
|Statistics                              |8343 |
+----------------------------------------+-----+
only showing top 20 rows
```
:::

::: {.cell-output .cell-output-stderr}
```
                                                                                
```
:::
:::


::: {#58758c76 .cell execution_count=87}
``` {.python .cell-code}
import matplotlib.pyplot as plt
import numpy as np


top20_pdf = top_skills.limit(20).toPandas()


top20_pdf = top20_pdf.sort_values("count", ascending=True)


morandi_colors = [
    "#A3A8AA", "#C7A8A3", "#D6C6B9", "#B8C5C3", "#A7B1A7",
    "#D1B7A1", "#C5C1B9", "#B7A39A", "#A6A6A6", "#D4C7B5",
    "#C4B5A5", "#B1A29A", "#C9BFB5", "#B8B2A6", "#AFA39A",
    "#D0C8BE", "#BBB5A8", "#A6A39B", "#C6BEB3", "#B4ADA3"
]


plt.figure(figsize=(12, 8))
bars = plt.barh(top20_pdf['skill'], top20_pdf['count'], color=morandi_colors)


for bar in bars:
    width = bar.get_width()
    plt.text(width + 300,           
             bar.get_y() + bar.get_height()/2,
             f"{int(width)}",
             va='center', fontsize=10)


plt.xlabel("Frequency", fontsize=12)
plt.ylabel("Skill", fontsize=12)
plt.title("Top 20 Most In-Demand Skills", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.show()

```

::: {.cell-output .cell-output-stderr}
```
                                                                                
```
:::

::: {.cell-output .cell-output-display}
![](skill_gap_analysis_files/figure-ipynb/cell-10-output-2.png){}
:::
:::


## **2.2 Identify gaps between team skill levels and job expectations**


Based on the team’s average skill scores and the market’s Top 20 most in-demand skills, the following gaps between team capabilities and job expectations are identified:

---

### **1. Programming & Database Skills (Python / SQL)**

- SQL ranks **#1 (43,466 postings)** and Python ranks **#3 (24,506 postings)**, making them essential for nearly all data roles.  
- Team averages:  
  - **Python: 3.33 / 5** (moderate, slightly below market expectations)  
  - **SQL: 2.33 / 5** (low, below typical hiring standards)  

**Gap:** SQL requires significant improvement; Python also needs enhancement.

---

### **2. Statistics and Data Management Skills Are Insufficient**

- Statistics and Data Management both appear in the Top 20 most in-demand skills.  
- Team averages:  
  - **Statistics: 3.00 / 5**  
  - **Data Cleaning: 2.67 / 5**  

**Gap:** Both statistical foundations and data-cleaning capabilities fall short of market expectations.

---

### **3. BI Tools (Tableau / Power BI) Fall Short of Market Demand**

- Tableau (23,562 postings) and Power BI (21,700 postings) are within the top 10 in-demand skills.  
- Team averages:  
  - **Tableau: 2.67 / 5**  
  - **Power BI: 3.00 / 5**  

**Gap:** Visualization tool proficiency is below the level expected for analyst roles.

---

### **4. Missing Business Analysis & Process Skills**

Highly demanded skills include:

- Business Requirements (13,035 postings)  
- Business Process (13,278 postings)  
- Project Management (13,711 postings)

These skill categories **do not appear in the team’s assessment**, indicating potential blind spots.

**Gap:** Business analysis, requirement documentation, and process mapping skills are missing.

---

### **5. Excel Meets Expectations but Advanced Skills May Lag**

- Excel is a commonly required skill (12,826 postings).  
- Team average: **4.33 / 5**, a strong performance.

**Gap:** Minimal, though advanced Excel modeling may still require improvement.

---

### **6. Communication Is a Strength but Still Needs Alignment with Market Expectations**

- Team average: **3.33 / 5**, showing a stable competency.

**Gap:** Cross-functional communication and business storytelling can be further strengthened.

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
