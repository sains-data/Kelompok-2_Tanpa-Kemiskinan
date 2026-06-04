import os
import sys
import urllib.request

# =========================================================================
# OTOMATISASI PATH HADOOP (ANTI-ERROR WINDOWS)
# =========================================================================
dummy_hadoop_dir = os.path.join(os.getcwd(), ".hadoop_dummy")
bin_dir = os.path.join(dummy_hadoop_dir, "bin")
os.makedirs(bin_dir, exist_ok=True)

winutils_path = os.path.join(bin_dir, "winutils.exe")

if not os.path.exists(winutils_path):
    print("[SISTEM] Mengunduh winutils.exe...")
    url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe"
    urllib.request.urlretrieve(url, winutils_path)

os.environ["HADOOP_HOME"] = dummy_hadoop_dir
os.environ["PATH"] += os.pathsep + bin_dir
sys.path.append(bin_dir)

from pyspark.sql import SparkSession

print("--- Memulai Eksekusi Lapisan Gold ---")

# =========================================
# SPARK + ICEBERG
# =========================================
spark = (
    SparkSession.builder
    .appName("Kiva_Gold_Layer")
    .master("local[*]")

    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"
    )

    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    .config(
        "spark.sql.catalog.local.type",
        "hadoop"
    )

    .config(
        "spark.sql.catalog.local.warehouse",
        "warehouse"
    )

    .getOrCreate()
)

# =========================================
# MEMBACA DATA
# =========================================
print("[PROSES] Membaca data dari Silver dan Bronze...")

loans_df = spark.read.table(
    "local.silver.kiva_loans_clean"
)

mpi_df = spark.read.table(
    "local.bronze.kiva_mpi_region_locations"
)

themes_df = spark.read.table(
    "local.bronze.loan_themes_by_region"
)

# =========================================
# JOIN DATA
# =========================================
print("[PROSES] Melakukan join berdasarkan country dan region...")

gold_df = (
    loans_df
    .join(
        mpi_df.select(
            "country",
            "region",
            "MPI"
        ),
        ["country", "region"],
        "left"
    )
    .join(
        themes_df.select(
            "country",
            "region",
            "Field Partner Name"
        ),
        ["country", "region"],
        "left"
    )
)

print("Join berhasil dilakukan")

# =========================================
# MEMBUAT NAMESPACE GOLD
# =========================================
spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS local.gold"
)

# =========================================
# FACT TABLE
# =========================================
print("[PROSES] Membuat fact table...")

fact_loans = gold_df.select(
    "id",
    "country",
    "region",
    "sector",
    "funded_amount",
    "loan_amount",
    "term_in_months",
    "borrower_genders",
    "MPI",
    "Field Partner Name"
)

# =========================================
# DIM REGION
# =========================================
print("[PROSES] Membuat dim_region...")

dim_region = (
    gold_df
    .select(
        "country",
        "region",
        "MPI"
    )
    .distinct()
)

# =========================================
# DIM SECTOR
# =========================================
print("[PROSES] Membuat dim_sector...")

dim_sector = (
    gold_df
    .select(
        "sector"
    )
    .distinct()
)

# =========================================
# DIM PARTNER
# =========================================
print("[PROSES] Membuat dim_partner...")

dim_partner = (
    gold_df
    .select(
        "Field Partner Name"
    )
    .distinct()
)

# =========================================
# SIMPAN KE ICEBERG
# =========================================
print("[PROSES] Menyimpan Star Schema ke Gold Layer...")

fact_loans.writeTo(
    "local.gold.fact_loans"
).using(
    "iceberg"
).create()

dim_region.writeTo(
    "local.gold.dim_region"
).using(
    "iceberg"
).create()

dim_sector.writeTo(
    "local.gold.dim_sector"
).using(
    "iceberg"
).create()

dim_partner.writeTo(
    "local.gold.dim_partner"
).using(
    "iceberg"
).create()

print("Star Schema berhasil disimpan ke Gold Layer")

spark.stop()

print("--- Eksekusi Gold Layer Selesai ---")
