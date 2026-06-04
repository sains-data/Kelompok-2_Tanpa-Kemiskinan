import os
import sys
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# =========================================================================
# 1. OTOMATISASI PATH HADOOP (ANTI-ERROR WINDOWS)
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

# =========================================
# 2. INISIALISASI SPARK & ICEBERG
# =========================================
spark = (
    SparkSession.builder
    .appName("Kiva_Gold_Layer")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "warehouse")
    .getOrCreate()
)

# =========================================
# 3. MEMBACA & MEMBERSIHKAN NAMA KOLOM
# =========================================
print("[PROSES] Membaca data dari Silver dan Bronze...")

# SOLUSI AMBIGUITAS: Ubah nama partner_id bawaan Kiva
loans_df = spark.read.table("local.silver.kiva_loans_clean").withColumnRenamed("partner_id", "kiva_partner_id")
mpi_df = spark.read.table("local.bronze.kiva_mpi_region_locations")
themes_df = spark.read.table("local.bronze.loan_themes_by_region").withColumnRenamed("Field Partner Name", "partner_name")

# =========================================
# 4. JOIN DATA MENTAH
# =========================================
print("[PROSES] Melakukan konsolidasi data...")

gold_df = (
    loans_df
    .join(mpi_df.select("country", "region", "MPI"), ["country", "region"], "left")
    .join(themes_df.select("country", "region", "partner_name"), ["country", "region"], "left")
)

# Menambal null hasil join eksternal (jika ada)
gold_df = gold_df.fillna({"MPI": 0.0, "partner_name": "Tidak Diketahui"})

# =========================================
# 5. PEMBUATAN TABEL DIMENSI (DENGAN SURROGATE KEYS)
# =========================================
print("[PROSES] Membangun Dimension Tables...")

# Dimensi Region
dim_region = gold_df.select("country", "region", "MPI").distinct()
dim_region = dim_region.withColumn("region_id", row_number().over(Window.orderBy("country", "region")))

# Dimensi Sektor
dim_sector = gold_df.select("sector").distinct()
dim_sector = dim_sector.withColumn("sector_id", row_number().over(Window.orderBy("sector")))

# Dimensi Partner
dim_partner = gold_df.select("partner_name").distinct()
dim_partner = dim_partner.withColumn("partner_id", row_number().over(Window.orderBy("partner_name")))

# =========================================
# 6. PEMBUATAN TABEL FAKTA (MAPPING ID)
# =========================================
print("[PROSES] Membangun Fact Table (Mapping Keys)...")

fact_loans = (
    gold_df
    .join(dim_region, ["country", "region", "MPI"], "left")
    .join(dim_sector, ["sector"], "left")
    .join(dim_partner, ["partner_name"], "left")
    .select(
        col("id").alias("loan_id"),
        "region_id",
        "sector_id",
        "partner_id", 
        "kiva_partner_id",
        "funded_amount",
        "loan_amount",
        "term_in_months",
        "borrower_genders"
    )
)

# =========================================
# 7. PENYIMPANAN KE ICEBERG (CREATE OR REPLACE)
# =========================================
print("[PROSES] Menyimpan arsitektur Star Schema ke namespace local.gold...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")

fact_loans.writeTo("local.gold.fact_loans").using("iceberg").createOrReplace()
dim_region.writeTo("local.gold.dim_region").using("iceberg").createOrReplace()
dim_sector.writeTo("local.gold.dim_sector").using("iceberg").createOrReplace()
dim_partner.writeTo("local.gold.dim_partner").using("iceberg").createOrReplace()

print("Status: SUKSES. Eksekusi Lapisan Gold Selesai.")
spark.stop()