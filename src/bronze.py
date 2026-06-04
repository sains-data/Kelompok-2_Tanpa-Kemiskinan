import os
import sys
import urllib.request
from pyspark.sql import SparkSession

# =========================================================================
# 1. OTOMATISASI PATH HADOOP (Standardisasi Anti-Error Windows)
# =========================================================================
dummy_hadoop_dir = os.path.join(os.getcwd(), ".hadoop_dummy")
bin_dir = os.path.join(dummy_hadoop_dir, "bin")
os.makedirs(bin_dir, exist_ok=True)

winutils_path = os.path.join(bin_dir, "winutils.exe")
hadoop_dll_path = os.path.join(bin_dir, "hadoop.dll")

if not os.path.exists(winutils_path):
    print("[SISTEM] Mengunduh winutils.exe...")
    url_winutils = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe"
    urllib.request.urlretrieve(url_winutils, winutils_path)

if not os.path.exists(hadoop_dll_path):
    print("[SISTEM] Mengunduh hadoop.dll...")
    url_dll = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/hadoop.dll"
    urllib.request.urlretrieve(url_dll, hadoop_dll_path)

os.environ["HADOOP_HOME"] = dummy_hadoop_dir
os.environ["PATH"] += os.pathsep + bin_dir
sys.path.append(bin_dir)

def execute_bronze(spark: SparkSession):
    print("--- Memulai Eksekusi Lapisan Bronze ---")

    # =========================================
    # Path File CSV (Terkoreksi)
    # =========================================
    loans_path = "data/raw/kiva_loans.csv"
    mpi_path = "data/raw/kiva_mpi_region_locations.csv"
    themes_path = "data/raw/loan_themes_by_region.csv"

    print("[PROSES] Membaca CSV mentah dengan penanganan karakter khusus (Escape, Quote, MultiLine)...")
    
    # =========================================
    # PERBAIKAN KRITIS: Parsing CSV Mencegah Data Shift
    # =========================================
    loans_df = spark.read.csv(
        loans_path,
        header=True,
        inferSchema=True,
        quote='"',
        escape='"',
        multiLine=True
    )

    mpi_df = spark.read.csv(
        mpi_path,
        header=True,
        inferSchema=True,
        quote='"',
        escape='"',
        multiLine=True
    )

    themes_df = spark.read.csv(
        themes_path,
        header=True,
        inferSchema=True,
        quote='"',
        escape='"',
        multiLine=True
    )

    print("\n[VALIDASI] Menampilkan skema kiva_loans (Pastikan funded_amount adalah DoubleType):")
    loans_df.printSchema() 

    # =========================================
    # Membuat namespace dan Menyimpan ke Iceberg
    # =========================================
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")

    print("[PROSES] Menyimpan ke Apache Iceberg...")
    loans_df.writeTo("local.bronze.kiva_loans").using("iceberg").createOrReplace()
    mpi_df.writeTo("local.bronze.kiva_mpi_region_locations").using("iceberg").createOrReplace()
    themes_df.writeTo("local.bronze.loan_themes_by_region").using("iceberg").createOrReplace()

    print("Status: SUKSES. Eksekusi Lapisan Bronze Selesai.\n")

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Kiva_Bronze_Layer")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "warehouse")
        .getOrCreate()
    )

    execute_bronze(spark)
    spark.stop()