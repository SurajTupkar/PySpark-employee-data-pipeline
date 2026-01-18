from pyspark.sql.types import *
from pyspark.sql.functions import *
from logger import get_logger

logger = get_logger("cleaner")


def clean_employee_data(df_raw):

#    Separate corrupt records

    logger.info("Starting data cleaning process")
    df_corrupt = df_raw.filter(col("_corrupt_record").isNotNull())
    df_valid   = df_raw.filter(col("_corrupt_record").isNull())

    logger.info(f"Valid records count: {df_valid.count()}")
    logger.info(f"Corrupt records count: {df_corrupt.count()}")

    
#    Rename Columns
    
    df = df_valid.withColumnRenamed("full_name","employee_name")\
                            .withColumnRenamed("location","work_location")\
                            .withColumnRenamed("status","employment_status")
    


#   Handle Null Values
    
    df = df.fillna({
        "base_salary":0,
        "bonus":0,
        "joining_date":"1900-07-01"
    })




#   Data Type Casting
    
    df = df.withColumn("employee_id",col("employee_id").cast(IntegerType()))\
                     .withColumn("base_salary",col("base_salary").cast(IntegerType()))\
                     .withColumn("bonus",col("bonus").cast(IntegerType()))\
                     .withColumn("joining_date",to_date(col("joining_date"),"yyyy-MM-dd"))
    




    return df,df_corrupt