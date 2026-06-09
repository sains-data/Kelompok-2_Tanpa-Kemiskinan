import os
os.environ["AWS_REGION"] = "us-east-1"
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col, coalesce, lit, sum, avg, lower, trim

base_dir = os.getcwd().replace("\\", "/")
hadoop_home = f"{base_dir}/.hadoop_dummy"

def execute_gold(spark: SparkSession):
    print("--- Memulai Eksekusi Lapisan Gold (Restorasi Tingkat Region Sesuai Proposal) ---")
    loans_clean = spark.table("local.silver.kiva_loans_cleaned")
    mpi_clean = spark.table("local.silver.kiva_mpi_cleaned")
    themes_clean = spark.table("local.silver.loan_themes_cleaned")

    # ====================================================================
    # 1. STAR SCHEMA: DIMENSI (Kembali ke Region)
    # ====================================================================
    dim_region = mpi_clean.select("country", "region", col("MPI").alias("mpi")).distinct() \
                          .withColumn("region_id", monotonically_increasing_id().cast("int"))
    
    dim_sector = themes_clean.select("sector").distinct() \
                             .withColumn("sector_id", monotonically_increasing_id().cast("int"))
    
    dim_partner = loans_clean.select(col("partner_id").alias("kiva_partner_id")).distinct() \
                             .withColumn("partner_id", monotonically_increasing_id().cast("int"))

    # ====================================================================
    # 2. STAR SCHEMA: FAKTA TRANSAKSIONAL (Join dengan Sanitasi)
    # ====================================================================
    lc = loans_clean.alias("lc")
    dr = dim_region.alias("dr")
    ds = dim_sector.alias("ds")
    dp = dim_partner.alias("dp")
    
    # Mencocokkan Country DAN Region secara bersamaan dengan pembersihan string
    fact_loans = lc.join(dr, (lower(trim(col("lc.country"))) == lower(trim(col("dr.country")))) & 
                             (lower(trim(col("lc.region"))) == lower(trim(col("dr.region")))), "left") \
                   .join(ds, lower(trim(col("lc.sector"))) == lower(trim(col("ds.sector"))), "left") \
                   .join(dp, col("lc.partner_id") == col("dp.kiva_partner_id"), "left")
    
    fact_loans = fact_loans.select(
        col("lc.id").alias("loan_id"),
        coalesce(col("dr.region_id"), lit(-1)).alias("region_id"),
        coalesce(col("ds.sector_id"), lit(-1)).alias("sector_id"),
        coalesce(col("dp.partner_id"), lit(-1)).alias("partner_id"),
        col("lc.funded_amount").cast("double"),
        col("lc.loan_amount").cast("double"),
        col("lc.term_in_months").cast("int"),
        col("lc.borrower_genders").cast("string"),
        col("lc.date")
    )

    # ====================================================================
    # 3. TABEL AGREGASI (Sesuai Proposal)
    # ====================================================================
    print("[PROSES] Membangun Tabel Agregasi (GroupBy Country & Region)...")
    agg_loans_region = lc.join(dr, (lower(trim(col("lc.country"))) == lower(trim(col("dr.country")))) & 
                                   (lower(trim(col("lc.region"))) == lower(trim(col("dr.region")))), "inner") \
                         .groupBy("dr.country", "dr.region") \
                         .agg(
                             sum("lc.funded_amount").alias("total_funded_amount"),
                             avg("dr.mpi").alias("average_mpi")
                         )
    
    # ====================================================================
    # 4. MENYIMPAN KE MINIO/ICEBERG
    # ====================================================================
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")
    print("[PROSES] Menyimpan Model Bintang & Agregasi ke Iceberg (MinIO/S3)...")
    
    dim_region.writeTo("local.gold.dim_region").using("iceberg").createOrReplace()
    dim_sector.writeTo("local.gold.dim_sector").using("iceberg").createOrReplace()
    dim_partner.writeTo("local.gold.dim_partner").using("iceberg").createOrReplace()
    fact_loans.writeTo("local.gold.fact_loans").using("iceberg").createOrReplace()
    agg_loans_region.writeTo("local.gold.agg_loans_by_region").using("iceberg").createOrReplace()
    
    print("Status: SUKSES. Arsitektur telah dikembalikan sesuai proposal.")

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("Kiva_Gold_MinIO")
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
             
    execute_gold(spark)
    spark.stop()