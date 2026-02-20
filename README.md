Employee Data Processing Pipeline (PySpark)

📌 Project Overview

-> This project is an end-to-end PySpark data pipeline designed to process employee data in a structured, scalable, and production-ready manner.
-> The pipeline demonstrates core Data Engineering fundamentals, including:
   1) Schema enforcement
   2) Data cleaning & validation
   3) Business transformations
   4) Analytical aggregations
   5) Partitioned storage
   6) Centralized logging



🏗️ Architecture Flow

                                                Raw CSV Data
                                                    ↓
                                     Reader (Schema + Corrupt Handling)
                                                    ↓
                                     Cleaner (Nulls, Rename, Casting)
                                                    ↓
                                       Transformer (Business Logic)
                                                    ↓
                                    Writer (Partitioned Parquet Output)


📂 Project Folder Structure

```
PySpark/Employee Data Processing Pipeline/
│
├── data/
│   ├── raw/
│   │   └── employees.csv
│   │
│   └── output/
│       ├── employees/
│       ├── department_summary/
│       └── corrupt_records/
│
├── logs/
│   └── app.log
│
├── src/
│   ├── reader.py
│   ├── cleaner.py
│   ├── transformer.py
│   ├── writer.py
│   ├── logger.py
│   └── main.py
│
├── venv/                 # ignored via .gitignore
├── __pycache__/          # ignored via .gitignore
├── .gitignore
├── requirements.txt
└── README.md

```

⚙️ Technologies Used

-> Python 3.12
-> Apache Spark 3.5.1
-> PySpark
-> WSL (Ubuntu on Windows)
-> Git & GitHub
-> VS Code



📥 Input Data

employees.csv

Contains employee-level information such as:
employee_id
full_name
department
location
status
base_salary
bonus
joining_date
The pipeline handles:
Missing values
Incorrect schema
Corrupt records


🔄 Processing Logic

1. Reader (reader.py)
-> Enforces schema using StructType
-> Reads CSV in FAILFAST mode
-> Captures corrupt records using _corrupt_record

2. Cleaner (cleaner.py)
-> Separates valid and corrupt records
-> Renames columns for consistency
-> Handles null values
-> Performs data type casting
-> Converts joining_date to DateType

3. Transformer (transformer.py)
-> Calculates total_compensation
-> Filters Active employees only
-> Categorizes compensation (HIGH / NORMAL)
-> Department-level aggregations:
-> Average Salary
-> Total Bonus
-> Employee Count


4. Writer (writer.py)
-> Writes data in parquet format
-> Uses partitioning for optimized querying
-> Output paths:
                -> /employees
                -> /department_summary
                -> /corrupt_records


📝 Logging
-> Centralized logging using python logging.
-> Logs are written to "logs/app.log".
-> Logs are also visible on the terminal.
-> Each module logs its own execution steps


How to Run the Project

1. Activate Virtual Environment
source venv/bin/activate

2. Run the Pipeline
python src/main.py


🎯 Key Highlights

-> Modular, production-ready code structure
-> Schema enforcement & corrupt record handling
-> Partitioned Parquet output for performance
-> Clean separation of responsibilities
-> Interview-ready Data Engineering project


🚀 Future Enhancements

-> Unit testing with PyTest
-> Configuration management via YAML
-> Spark submit integration
-> Cloud storage support (S3/ADLS)
-> Workflow orchestration (Airflow)


👨‍💻 Author
Suraj Tupkar
Data Engineer
PYTHON | SQL | PySpark | AWS


