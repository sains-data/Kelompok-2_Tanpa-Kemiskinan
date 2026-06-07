import os
os.environ["AWS_REGION"] = "us-east-1"
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean

base_dir = os.getcwd().replace("\\", "/")
hadoop_home = f"{base_dir}/.hadoop_dummy"

def execute_silver(spark: SparkSession):
    print("--- Memulai Eksekusi Lapisan Silver (Target: MinIO) ---")
    loans_df = spark.table("local.bronze.kiva_loans")
    mpi_df = spark.table("local.bronze.kiva_mpi_region_locations")
    themes_df = spark.table("local.bronze.loan_themes_by_region")
    
    print("\n[DQ GATE] Mengevaluasi Kualitas Data MPI...")
    total_mpi_rows = mpi_df.count()
    valid_mpi_rows = mpi_df.filter(col("MPI").isNotNull()).count()
    dq_percentage = (valid_mpi_rows / total_mpi_rows) * 100 if total_mpi_rows > 0 else 0
    
    # OUTPUT METRIK KE TERMINAL
    print(f"-> Total Data: {total_mpi_rows} baris")
    print(f"-> Data Valid: {valid_mpi_rows} baris")
    print(f"-> SKOR KUALITAS DATA AWAL: {dq_percentage:.2f}%")
    
    if dq_percentage >= 80.0:
        print("-> [BRANCH A] Kualitas >= 80%. Lanjut ke tahap berikutnya tanpa intervensi manual.")
        mpi_clean = mpi_df
    else:
        print("-> [BRANCH B] Kualitas < 80%. Memicu Skrip Auto-Review dan Imputasi Statistik...")
        avg_mpi_row = mpi_df.select(mean(col("MPI"))).collect()[0][0]
        avg_mpi = float(avg_mpi_row) if avg_mpi_row else 0.0
        
        print(f"   [Auto-Review] Nilai rata-rata MPI regional ({avg_mpi:.4f}) disuntikkan ke missing values...")
        mpi_clean = mpi_df.na.fill({"MPI": avg_mpi})
        
        new_valid_rows = mpi_clean.filter(col("MPI").isNotNull()).count()
        new_dq_percentage = (new_valid_rows / total_mpi_rows) * 100
        print(f"-> SKOR KUALITAS PASCA-AUTO-REVIEW: {new_dq_percentage:.2f}%")
        
        if new_dq_percentage < 80.0:
            raise Exception("GAGAL! Kualitas data tetap di bawah 80%.")
        else:
            print("-> [TERIMA] Kualitas sudah memenuhi syarat. Melanjutkan pipeline.")

    print("\n[DQ CHECK] Membersihkan Data Transaksi dan Sektoral...")
    mpi_clean = mpi_clean.na.fill({"region": "Unknown", "country": "Unknown"})
    loans_clean = loans_df.filter(col("funded_amount") > 0).na.fill({"country": "Unknown", "region": "Unknown", "sector": "Unknown", "partner_id": -1})
    themes_clean = themes_df.na.fill({"sector": "Unknown", "region": "Unknown"})

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    print("\n[PROSES] Menyimpan Tabel Bersih ke Iceberg (MinIO/S3)...")
    loans_clean.writeTo("local.silver.kiva_loans_cleaned").using("iceberg").createOrReplace()
    mpi_clean.writeTo("local.silver.kiva_mpi_cleaned").using("iceberg").createOrReplace()
    themes_clean.writeTo("local.silver.loan_themes_cleaned").using("iceberg").createOrReplace()
    print("Status: SUKSES (Silver).")

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("Kiva_Silver_MinIO")
             .master("local[*]")
             .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.postgresql:postgresql:42.6.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0")
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.local.type", "jdbc")
             .config("spark.sql.catalog.local.uri", "jdbc:postgresql://localhost:5432/iceberg_catalog")
             .config("spark.sql.catalog.local.jdbc.user", "admin")
             .config("spark.sql.catalog.local.jdbc.password", "password")
             .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
             .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
             .config("spark.sql.catalog.local.s3.endpoint", "http://localhost:9000")
             .config("spark.sql.catalog.local.s3.access-key-id", "admin")
             .config("spark.sql.catalog.local.s3.secret-access-key", "password")
             .config("spark.sql.catalog.local.s3.path-style-access", "true")
             .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={hadoop_home}")
             .config("spark.executor.extraJavaOptions", f"-Dhadoop.home.dir={hadoop_home}")
             .config("spark.sql.catalog.local.s3.region", "us-east-1")
             .config("spark.hadoop.fs.s3a.signing-algorithm", "S3SignerType")
             .getOrCreate())
    
    execute_silver(spark)
    spark.stop()