from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window


def transform_employee_data(df_clean):
    df = df_clean.withColumn(
        "total_compensation",
        col("base_salary")+col("bonus"))

#   Filter only Active Employees    
    df = df.filter(col("employment_status")=="Active")

#   High Value Employee Flag
    df = df.withColumn(
        "compensation_category",
        when(col("total_compensation")>100000,"HIGH")
        .otherwise("NORMAL"))
    
#   Department-wise Aggregation    
    df_department_summary = df.groupBy("department").agg(
        avg(col("base_salary")).alias("Average_Salary"),
        sum(col("bonus")).alias("Total_Bonus"),
        count(col("employee_id")).alias("Employee_Count"))
    
#   Window Function: Rank employees within department
    window_spec = Window.partitionBy("department").orderBy(col("total_compensation").desc())

    df_final = df.withColumn("salary_rank_in_department",
                             row_number().over(window_spec))

    return df_final,df_department_summary



