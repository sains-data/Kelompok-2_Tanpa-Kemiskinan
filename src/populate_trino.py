import os
import sys
import urllib.request
from pyspark.sql import SparkSession

# 1. OTOMATISASI PATH HADOOP (Standardisasi Windows)
dummy_hadoop_dir = os.path.join(os.getcwd(), ".hadoop_dummy")
bin_dir = os.path.join(dummy_hadoop_dir, "bin")
os.makedirs(bin_dir, exist_ok=True)
winutils_path = os.path.join(bin_dir, "winutils.exe")
hadoop_dll_path = os.path.join(bin_dir, "hadoop.dll")

if not os.path.exists(winutils_path):
    urllib.request.urlretrieve("https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe", winutils_path)
if not os.path.exists(hadoop_dll_path):
    urllib.request.urlretrieve("https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/hadoop.dll", hadoop_dll_path)

os.environ["HADOOP_HOME"] = dummy_hadoop_dir
os.environ["PATH"] += os.pathsep + bin_dir
sys.path.append(bin_dir)

# Meredam validasi AWS SDK di tingkat sistem operasi
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

def populate():
    print("--- Migrasi Data ke Arsitektur Enterprise (MinIO S3 + PostgreSQL) ---")
    
    print("[PROSES] Menyiapkan koneksi JVM dan mengunduh Pustaka Iceberg, Postgres, & AWS S3...")
    spark = (SparkSession.builder.appName("Iceberg_S3_Migration")
             .master("local[*]")
             # Menambahkan driver PostgreSQL dan AWS S3 untuk MinIO
             .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0,org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             
             # Konfigurasi Catalog LAMA (Sumber: Folder Lokal Windows)
             .config("spark.sql.catalog.local_hadoop", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.local_hadoop.type", "hadoop")
             .config("spark.sql.catalog.local_hadoop.warehouse", "warehouse")
             
             # Konfigurasi Catalog BARU (Target: PostgreSQL + MinIO)
             .config("spark.sql.catalog.trino_cat", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.trino_cat.type", "jdbc")
             .config("spark.sql.catalog.trino_cat.uri", "jdbc:postgresql://localhost:5432/iceberg_catalog")
             .config("spark.sql.catalog.trino_cat.jdbc.user", "admin")
             .config("spark.sql.catalog.trino_cat.jdbc.password", "password")
             .config("spark.sql.catalog.trino_cat.warehouse", "s3a://warehouse/")
             .config("spark.sql.catalog.trino_cat.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
             .config("spark.sql.catalog.trino_cat.s3.endpoint", "http://localhost:9000")
             .config("spark.sql.catalog.trino_cat.s3.path-style-access", "true")
             
             # AWS S3 Hadoop FileSystem Configs untuk backend
             .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
             .config("spark.sql.catalog.trino_cat.s3.region", "us-east-1")
             .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
             .config("spark.hadoop.fs.s3a.access.key", "admin")
             .config("spark.hadoop.fs.s3a.secret.key", "password")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .getOrCreate())

    print("[PROSES] Membaca data Star Schema dari Hadoop Catalog (Lokal)...")
    dim_region = spark.table("local_hadoop.gold.dim_region")
    fact_loans = spark.table("local_hadoop.gold.fact_loans")

    print("[PROSES] Membuat Namespace di JDBC Catalog (PostgreSQL)...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS trino_cat.gold")

    print("[PROSES] Menulis dan mengirimkan data ke S3 MinIO...")
    dim_region.writeTo("trino_cat.gold.dim_region").using("iceberg").createOrReplace()
    fact_loans.writeTo("trino_cat.gold.fact_loans").using("iceberg").createOrReplace()

    print("Status: SUKSES. Data mendarat di MinIO dan terdaftar di PostgreSQL.\n")
    spark.stop()

if __name__ == "__main__":
    populate()