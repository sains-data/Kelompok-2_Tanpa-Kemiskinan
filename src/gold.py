import os
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col, coalesce, lit

# 1. PAKSAAN FISIK HADOOP
base_dir = os.getcwd().replace("\\", "/")
hadoop_home = f"{base_dir}/.hadoop_dummy"
bin_dir = f"{hadoop_home}/bin"
os.makedirs(bin_dir, exist_ok=True)
winutils_path = f"{bin_dir}/winutils.exe"
hadoop_dll_path = f"{bin_dir}/hadoop.dll"

if not os.path.exists(winutils_path):
    urllib.request.urlretrieve("https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe", winutils_path)
if not os.path.exists(hadoop_dll_path):
    urllib.request.urlretrieve("https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/hadoop.dll", hadoop_dll_path)

os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")

def execute_gold(spark: SparkSession):
    print("--- Memulai Eksekusi Lapisan Gold (Final: 4 Tabel) ---")
    loans_df = spark.table("local.silver.kiva_loans_cleaned")
    
    # Standardisasi Data
    loans_clean = loans_df.na.fill({
        "country": "Unknown", "region": "Unknown", 
        "sector": "Unknown", "partner_id": -1
    })

    # DETEKTOR KOLOM DINAMIS
    has_mpi = "mpi" in [c.lower() for c in loans_clean.columns]
    mpi_col = col("mpi") if has_mpi else lit(0.0).alias("mpi")

    # DIMENSI 1: Region & MPI
    print("[PROSES] dim_region...")
    dim_region = loans_clean.select("country", "region", mpi_col).distinct()
    dim_region = dim_region.withColumn("region_id", monotonically_increasing_id().cast("int"))

    # DIMENSI 2: Sector
    print("[PROSES] dim_sector...")
    dim_sector = loans_clean.select("sector").distinct()
    dim_sector = dim_sector.withColumn("sector_id", monotonically_increasing_id().cast("int"))

    # DIMENSI 3: Partner
    print("[PROSES] dim_partner...")
    dim_partner = loans_clean.select(col("partner_id").alias("kiva_partner_id")).distinct()
    dim_partner = dim_partner.withColumn("partner_id", monotonically_increasing_id().cast("int"))

    # FAKTA: Fact Loans (Dilengkapi Aliasing untuk mencegah Ambiguous Reference)
    print("[PROSES] fact_loans...")
    lc = loans_clean.alias("lc")
    dp = dim_partner.alias("dp")
    
    fact_loans = lc.join(dim_region, ["country", "region"], "left") \
                   .join(dim_sector, ["sector"], "left") \
                   .join(dp, col("lc.partner_id") == col("dp.kiva_partner_id"), "left")
    
    fact_loans = fact_loans.select(
        col("lc.id").alias("loan_id"),
        coalesce(col("region_id"), lit(-1)).alias("region_id"),
        coalesce(col("sector_id"), lit(-1)).alias("sector_id"),
        coalesce(col("dp.partner_id"), lit(-1)).alias("partner_id"),
        col("lc.funded_amount").cast("double"),
        col("lc.loan_amount").cast("double"),
        col("lc.term_in_months").cast("int"),
        col("lc.borrower_genders").cast("string"),
        col("lc.date")
    )
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
    
    print("[PROSES] Menulis ke MinIO...")
    dim_region.writeTo("local.gold.dim_region").using("iceberg").createOrReplace()
    dim_sector.writeTo("local.gold.dim_sector").using("iceberg").createOrReplace()
    dim_partner.writeTo("local.gold.dim_partner").using("iceberg").createOrReplace()
    fact_loans.writeTo("local.gold.fact_loans").using("iceberg").createOrReplace()
    
    print("Status: SUKSES. 4 Tabel Star Schema Siap.")

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("Kiva_Gold_Layer")
             .master("local[*]")
             .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.local.type", "hadoop")
             .config("spark.sql.catalog.local.warehouse", "warehouse")
             .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={hadoop_home}")
             .config("spark.executor.extraJavaOptions", f"-Dhadoop.home.dir={hadoop_home}")
             .getOrCreate())
    execute_gold(spark)
    spark.stop()