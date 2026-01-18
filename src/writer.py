from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from logger import get_logger

logger = get_logger("writer")


def write_employee_data(
    df_final:DataFrame,
    df_department_summary:DataFrame,
    df_corrupt:DataFrame,
    output_base_path:str):


    employees_path = f"{output_base_path}/employees/"
    department_summary_path = f"{output_base_path}/department_summary/"
    corrupt_records_path = f"{output_base_path}/corrupt_records/"

    logger.info(f"Writing final employee data to path: {employees_path}")
    df_final.write.format("parquet")\
            .mode("overwrite")\
            .partitionBy("department","employment_status")\
            .parquet(employees_path)
    
    logger.info(f"Writing department summary data to path: {department_summary_path}")
    df_department_summary.write.format("parquet")\
                         .mode("overwrite")\
                         .parquet(department_summary_path)
    

    logger.info(f"Writing corrupt records to path: {corrupt_records_path}")
    df_corrupt.write.format("parquet")\
              .mode("overwrite")\
              .parquet(corrupt_records_path)
    
    logger.info("Data written successfully")
    
