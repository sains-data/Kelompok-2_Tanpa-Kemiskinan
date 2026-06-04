import os
import sys
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col

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

def execute_gold(spark: SparkSession):
    print("--- Memulai Eksekusi Lapisan Gold (Star Schema) ---")
    
    print("[PROSES] Membaca data dari Lapisan Silver...")
    loans_df = spark.table("local.silver.kiva_loans_cleaned")
    
    # Membuat Tabel Dimensi (Contoh: Dimensi Region/Lokasi)
    print("[PROSES] Membangun Dimensi Region...")
    dim_region = loans_df.select("country", "region").distinct().dropna()
    dim_region = dim_region.withColumn("region_id", monotonically_increasing_id())
    
    # Membuat Tabel Fakta
    print("[PROSES] Membangun Tabel Fakta Pinjaman...")
    fact_loans = loans_df.join(dim_region, ["country", "region"], "left")
    
    # Pilih kolom esensial untuk analitik (Pastikan funded_amount ikut)
    fact_loans = fact_loans.select(
        col("id").alias("loan_id"),
        col("region_id"),
        col("funded_amount").cast("double"),
        col("sector"),
        col("date")
    )
    
    # Membuat namespace dan menyimpan ke Gold
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
    
    print("[PROSES] Menyimpan ke Apache Iceberg (Gold)...")
    dim_region.writeTo("local.gold.dim_region").using("iceberg").createOrReplace()
    fact_loans.writeTo("local.gold.fact_loans").using("iceberg").createOrReplace()
    
    print("Status: SUKSES. Eksekusi Lapisan Gold Selesai.\n")

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("Kiva_Gold_Layer")
             .master("local[*]")
             .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.local.type", "hadoop")
             .config("spark.sql.catalog.local.warehouse", "warehouse")
             .getOrCreate())
    execute_gold(spark)
    spark.stop()