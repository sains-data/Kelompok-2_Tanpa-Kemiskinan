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
    
    # 1. Membangun Dimensi Region
    print("[PROSES] Membangun Dimensi Region...")
    dim_region = loans_df.select("country", "region").distinct().dropna()
    dim_region = dim_region.withColumn("region_id", monotonically_increasing_id())

    # 2. Membangun Dimensi Sector
    print("[PROSES] Membangun Dimensi Sector...")
    dim_sector = loans_df.select("sector").distinct().dropna()
    dim_sector = dim_sector.withColumn("sector_id", monotonically_increasing_id())

    # 3. Membangun Dimensi Partner
    print("[PROSES] Membangun Dimensi Partner...")
    # Kiva dataset menggunakan partner_id, kita pastikan kolomnya ada dan bersih
    dim_partner = loans_df.select("partner_id").distinct().dropna()

    # 4. Membangun Tabel Fakta Pinjaman
    print("[PROSES] Membangun Tabel Fakta Pinjaman...")
    # Menggabungkan data dengan ID dari tabel dimensi
    fact_loans = loans_df.join(dim_region, ["country", "region"], "left") \
                         .join(dim_sector, ["sector"], "left")
    
    # Pilih kolom esensial untuk analitik (Pastikan Kunci Asing / Foreign Keys terhubung)
    fact_loans = fact_loans.select(
        col("id").alias("loan_id"),
        col("region_id"),
        col("sector_id"),
        col("partner_id"),
        col("funded_amount").cast("double"),
        col("date")
    )
    
    # Membuat namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
    
    # 5. Menyimpan Seluruh Tabel ke Iceberg
    print("[PROSES] Menyimpan Dimensi dan Fakta ke Apache Iceberg (Gold)...")
    dim_region.writeTo("local.gold.dim_region").using("iceberg").createOrReplace()
    dim_sector.writeTo("local.gold.dim_sector").using("iceberg").createOrReplace()
    dim_partner.writeTo("local.gold.dim_partner").using("iceberg").createOrReplace()
    fact_loans.writeTo("local.gold.fact_loans").using("iceberg").createOrReplace()
    
    print("Status: SUKSES. 4 Tabel Lapisan Gold Selesai Dibangun.\n")

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