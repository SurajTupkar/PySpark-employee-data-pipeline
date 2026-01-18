from pyspark.sql.types import *
from pyspark.sql.functions import *
from logger import get_logger


logger = get_logger("reader")



def read_employee_data(spark,file_path):

    logger.info(f"Reading employee data from path: {file_path}")


    schema=StructType([
    StructField("employee_id",StringType(),True),
    StructField("full_name",StringType(),True),
    StructField("department",StringType(),True),
    StructField("location",StringType(),True),
    StructField("status",StringType(),True),
    StructField("base_salary",StringType(),True),
    StructField("bonus",StringType(),True),
    StructField("joining_date",StringType(),True),
    StructField("_corrupt_record",StringType(),True)
    ])

    """
    Reads employee CSV data using predefined schema.
    Handles corrupted records explicitly.
    """

    df = spark.read.format("csv")\
        .option("header",True)\
        .option("mode","PERMISSIVE")\
        .schema(schema)\
        .load(file_path)
    
    logger.info("Employee data read successfully")

    return df
    
