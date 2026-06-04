import os
import sys
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# 1. OTOMATISASI PATH HADOOP (Standardisasi)
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

def execute_silver(spark: SparkSession):
    print("--- Memulai Eksekusi Lapisan Silver ---")
    
    # Membaca dari Bronze
    print("[PROSES] Membaca data dari Lapisan Bronze...")
    loans_df = spark.table("local.bronze.kiva_loans")
    mpi_df = spark.table("local.bronze.kiva_mpi_region_locations")
    
    # Implementasi Metrik Evaluasi: Drop Kolom > 80% Null secara Dinamis
    print("[PROSES] Mengevaluasi dan membuang kolom dengan Null > 80%...")
    total_rows = loans_df.count()
    null_counts = loans_df.select([count(when(col(c).isNull(), c)).alias(c) for c in loans_df.columns]).collect()[0].asDict()
    
    cols_to_drop = [c for c, null_cnt in null_counts.items() if (null_cnt / total_rows) > 0.8]
    if cols_to_drop:
        print(f"[METRIK] Kolom yang di-drop karena Null > 80%: {cols_to_drop}")
        loans_df = loans_df.drop(*cols_to_drop)
    else:
        print("[METRIK] Tidak ada kolom yang melebihi batas Null 80%.")

    # Membuat namespace dan menyimpan ke Silver
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    
    print("[PROSES] Menyimpan ke Apache Iceberg (Silver)...")
    loans_df.writeTo("local.silver.kiva_loans_cleaned").using("iceberg").createOrReplace()
    mpi_df.writeTo("local.silver.kiva_mpi_cleaned").using("iceberg").createOrReplace()
    
    print("Status: SUKSES. Eksekusi Lapisan Silver Selesai.\n")

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("Kiva_Silver_Layer")
             .master("local[*]")
             .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.local.type", "hadoop")
             .config("spark.sql.catalog.local.warehouse", "warehouse")
             .getOrCreate())
    execute_silver(spark)
    spark.stop()