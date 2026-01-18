from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from reader import read_employee_data
from cleaner import clean_employee_data
from transformer import transform_employee_data
from writer import write_employee_data
from logger import get_logger

logger = get_logger("main")

logger.info("Spark job started")

def create_spark_session():
    return SparkSession.builder\
    .appName("EmployeeDataPipeline")\
    .getOrCreate()


if __name__ =="__main__":
    spark = create_spark_session()
    print("Spark Version:",spark.version)
    # spark.stop()

    input_path = "/mnt/d/PySpark/Employee Data Processing Pipeline/data/raw/employees.csv"
    output_path = "/mnt/d/PySpark/Employee Data Processing Pipeline/data/output"

    df_raw = read_employee_data(spark, input_path)
    df_clean, df_corrupt = clean_employee_data(df_raw)
    df_final, df_department_summary = transform_employee_data(df_clean)

    write_employee_data(
        df_final,
        df_department_summary,
        df_corrupt,
        output_path
    )

    spark.stop()

    logger.info("Spark job completed successfully")

