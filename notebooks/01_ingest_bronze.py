import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

SOURCE_PATH = "Files/onprem_source"
BRONZE_DB   = "bronze"

def ingest_excel_to_bronze(filename: str, table_name: str):
    local_path = f"/lakehouse/default/{SOURCE_PATH}/{filename}"
    pdf = pd.read_excel(local_path, dtype=str)
    pdf["_ingested_at"] = pd.Timestamp.now().isoformat()
    pdf["_source_file"] = filename
    sdf = spark.createDataFrame(pdf)
    (
        sdf.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(f"{BRONZE_DB}.raw_{table_name}")
    )
    print(f"✓  {filename}  →  {BRONZE_DB}.raw_{table_name}  ({len(pdf)} rows)")

ingest_excel_to_bronze("projects.xlsx",   "projects")
ingest_excel_to_bronze("employees.xlsx",  "employees")
ingest_excel_to_bronze("timesheets.xlsx", "timesheets")
ingest_excel_to_bronze("invoices.xlsx",   "invoices")

print("\nBronze ingestion complete.")