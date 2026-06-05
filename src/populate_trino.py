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

def populate():
    print("--- Migrasi Data ke Arsitektur Enterprise (JDBC Catalog) ---")
    
    print("[PROSES] Menyiapkan koneksi JVM dan mengunduh PostgreSQL Driver...")
    spark = (SparkSession.builder.appName("Iceberg_JDBC_Migration")
             .master("local[*]")
             # Menambahkan driver PostgreSQL 
             .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.postgresql:postgresql:42.7.3")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             
             # Konfigurasi Catalog LAMA (Hadoop - Sumber)
             .config("spark.sql.catalog.local_hadoop", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.local_hadoop.type", "hadoop")
             .config("spark.sql.catalog.local_hadoop.warehouse", "warehouse")
             
             # Konfigurasi Catalog BARU (JDBC - Target Trino)
             .config("spark.sql.catalog.trino_cat", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.trino_cat.type", "jdbc")
             .config("spark.sql.catalog.trino_cat.uri", "jdbc:postgresql://localhost:5432/iceberg_catalog")
             .config("spark.sql.catalog.trino_cat.jdbc.user", "admin")
             .config("spark.sql.catalog.trino_cat.jdbc.password", "password")
             .config("spark.sql.catalog.trino_cat.warehouse", "warehouse")
             .getOrCreate())

    print("[PROSES] Membaca data Star Schema dari Hadoop Catalog...")
    dim_region = spark.table("local_hadoop.gold.dim_region")
    fact_loans = spark.table("local_hadoop.gold.fact_loans")

    print("[PROSES] Membuat Namespace di JDBC Catalog (PostgreSQL)...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS trino_cat.gold")

    print("[PROSES] Menulis dan mendaftarkan data ke PostgreSQL untuk Trino...")
    dim_region.writeTo("trino_cat.gold.dim_region").using("iceberg").createOrReplace()
    fact_loans.writeTo("trino_cat.gold.fact_loans").using("iceberg").createOrReplace()

    print("Status: SUKSES. Data berhasil didaftarkan ke PostgreSQL. Trino siap digunakan.\n")
    spark.stop()

if __name__ == "__main__":
    populate()