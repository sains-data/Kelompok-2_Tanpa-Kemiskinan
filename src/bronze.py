import os
os.environ["AWS_REGION"] = "us-east-1"
import urllib.request
from pyspark.sql import SparkSession

base_dir = os.getcwd().replace("\\", "/")
hadoop_home = f"{base_dir}/.hadoop_dummy"
bin_dir = f"{hadoop_home}/bin"
os.makedirs(bin_dir, exist_ok=True)
if not os.path.exists(f"{bin_dir}/winutils.exe"):
    urllib.request.urlretrieve("https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe", f"{bin_dir}/winutils.exe")
if not os.path.exists(f"{bin_dir}/hadoop.dll"):
    urllib.request.urlretrieve("https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/hadoop.dll", f"{bin_dir}/hadoop.dll")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")

def execute_bronze(spark: SparkSession):
    print("--- Memulai Eksekusi Lapisan Bronze (Target: MinIO) ---")
    loans_df = spark.read.csv("data/raw/kiva_loans.csv", header=True, inferSchema=True, quote='"', escape='"', multiLine=True)
    mpi_df = spark.read.csv("data/raw/kiva_mpi_region_locations.csv", header=True, inferSchema=True, quote='"', escape='"', multiLine=True)
    themes_df = spark.read.csv("data/raw/loan_themes_by_region.csv", header=True, inferSchema=True, quote='"', escape='"', multiLine=True)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")
    print("[PROSES] Menyimpan Raw Data ke Iceberg (MinIO/S3)...")
    loans_df.writeTo("local.bronze.kiva_loans").using("iceberg").createOrReplace()
    mpi_df.writeTo("local.bronze.kiva_mpi_region_locations").using("iceberg").createOrReplace()
    themes_df.writeTo("local.bronze.loan_themes_by_region").using("iceberg").createOrReplace()
    print("Status: SUKSES (Bronze).")

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("Kiva_Bronze_MinIO")
             .master("local[*]")
             # Library AWS Iceberg Bundle diinjeksikan
             .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.postgresql:postgresql:42.6.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.local.type", "jdbc")
             .config("spark.sql.catalog.local.uri", "jdbc:postgresql://localhost:5432/iceberg_catalog")
             .config("spark.sql.catalog.local.jdbc.user", "admin")
             .config("spark.sql.catalog.local.jdbc.password", "password")
             # Konfigurasi S3 / MinIO (Versi Perbaikan)
            .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
            .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.local.s3.endpoint", "http://localhost:9000")
            .config("spark.sql.catalog.local.s3.region", "us-east-1")
            .config("spark.sql.catalog.local.s3.access-key-id", "admin")
            .config("spark.sql.catalog.local.s3.secret-access-key", "password")
            .config("spark.sql.catalog.local.s3.path-style-access", "true")
             .getOrCreate())
    execute_bronze(spark)
    spark.stop()