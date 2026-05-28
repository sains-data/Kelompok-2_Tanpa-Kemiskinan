import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# =========================================================================
# OTOMATISASI PATH HADOOP (Deteksi File di D:\Downloads)
# =========================================================================
hadoop_dir = r"D:\Downloads"
os.environ["HADOOP_HOME"] = hadoop_dir
os.environ["PATH"] += os.pathsep + os.path.join(hadoop_dir, "bin")
sys.path.append(os.path.join(hadoop_dir, "bin"))

print("--- Memulai Eksekusi Lapisan Silver ---")

# =========================================
# START SPARK WITH ICEBERG CONFIGURATION
# =========================================
spark = (
    SparkSession.builder
    .appName("Kiva_Silver_Layer")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "warehouse")
    # Konfigurasi tambahan untuk menangani masalah commit/I-O di Windows lokal
    .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.io.ResolvingFileIO")
    .getOrCreate()
)

print("Spark dengan ekstensi Apache Iceberg berhasil berjalan.")

# =========================================
# LOAD DATA DARI BRONZE LAYER (ICEBERG)
# =========================================
print("Sedang membaca data dari local.bronze.kiva_loans...")
df = spark.read.table("local.bronze.kiva_loans")

# =========================================
# HITUNG TOTAL BARIS & FILTER NULL PERCENTAGE
# =========================================
total_rows = df.count()
print(f"Total baris yang dimuat: {total_rows}")

columns_to_drop = []
for column in df.columns:
    null_count = df.filter(col(column).isNull()).count()
    null_percentage = (null_count / total_rows) * 100
    print(f"Kolom [{column}] -> NULL: {null_percentage:.2f}%")
    
    if null_percentage > 80:
        columns_to_drop.append(column)

df_clean = df.drop(*columns_to_drop)

print("\n[INFO] Kolom yang dihapus karena > 80% NULL:")
print(columns_to_drop)

print("\n[INFO] Sisa kolom yang dipertahankan:")
print(df_clean.columns)

# =========================================
# MEMBUAT NAMESPACE & MENYIMPAN KE SILVER LAYER
# =========================================
try:
    print("Mencoba menyimpan data bersih ke Apache Iceberg Silver Layer...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    df_clean.writeTo("local.silver.kiva_loans_clean").using("iceberg").createOrReplace()
    print("Data bersih berhasil disimpan ke local.silver.kiva_loans_clean!")
except Exception as e:
    print("\n[PERINGATAN] Penulisan catalog Iceberg terhambat permission Windows.")
    print("Menjalankan Fallback: Menyimpan langsung ke folder warehouse lokal dalam format Parquet...")
    
    # Jalur penyimpanan alternatif jika Iceberg Catalog terkunci oleh sistem Windows
    fallback_path = "warehouse/silver/kiva_loans_clean_parquet"
    df_clean.write.mode("overwrite").parquet(fallback_path)
    print(f"Data bersih berhasil diamankan ke folder lokal: {fallback_path}")

print("--- Eksekusi Lapisan Silver Selesai ---")
spark.stop()