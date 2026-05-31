import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import Imputer


# =========================================================================
# OTOMATISASI PATH HADOOP (Deteksi File di C:\hadoop)
# =========================================================================
hadoop_dir = r"C:\hadoop"
os.environ["HADOOP_HOME"] = hadoop_dir
os.environ["PATH"] += os.pathsep + os.path.join(hadoop_dir, "bin")
sys.path.append(os.path.join(hadoop_dir, "bin"))
print("--- Memulai Eksekusi Lapisan Silver ---")

# =========================================
# 2. INISIALISASI SPARK & ICEBERG
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
    .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.io.ResolvingFileIO")
    .getOrCreate()
)

# =========================================
# 3. EKSTRAKSI DARI BRONZE (ICEBERG)
# =========================================
print("\n[PROSES] Membaca data mentah dari Bronze Layer...")
df_bronze = spark.read.table("local.bronze.kiva_loans")
total_rows = df_bronze.count()

# =========================================
# 4. AUDIT KUALITAS DATA AWAL & PENENTUAN ATURAN
# =========================================
columns_to_drop = []
columns_to_impute = []
whitelist_columns = ['mpi_region', 'country_code', 'loan_id']

print(f"\n=== LAPORAN KUALITAS DATA (BRONZE) ===")
print(f"Total Baris Data: {total_rows}")
print("-" * 50)

for column in df_bronze.columns:
    null_count = df_bronze.filter(col(column).isNull()).count()
    null_percentage = (null_count / total_rows) * 100
    
    # Menampilkan metrik ke terminal dengan rapi
    if null_percentage > 0:
        print(f"[*] {column:<20} : {null_percentage:>5.2f}% Kosong")
    else:
        print(f"[+] {column:<20} : 100.00% Bersih")
    
    # Logika Aturan
    if null_percentage >= 80:
        if column not in whitelist_columns:
            columns_to_drop.append(column)
    elif null_percentage > 0 and null_percentage < 80:
        dtype = dict(df_bronze.dtypes)[column]
        if dtype in ['int', 'double', 'float', 'bigint']:
            columns_to_impute.append(column)

print("-" * 50)

# =========================================
# 5. EKSEKUSI PEMBERSIHAN (DROP & IMPUTASI)
# =========================================
print("\n[PROSES] Mengeksekusi Data Quality Gate...")

# Eksekusi 1: Buang kolom sampah (>80%)
df_silver = df_bronze.drop(*columns_to_drop)
print(f" -> BERHASIL: {len(columns_to_drop)} kolom dibuang secara permanen (>= 80% Kosong).")
if columns_to_drop:
    print(f"    (Kolom: {', '.join(columns_to_drop)})")

# Eksekusi 2: Tambal kolom numerik (<80%) menggunakan Median
if columns_to_impute:
    print(f" -> BERHASIL: Melakukan imputasi Median pada {len(columns_to_impute)} kolom numerik.")
    imputer = Imputer(
        inputCols=columns_to_impute, 
        outputCols=columns_to_impute
    ).setStrategy("median")
    df_silver = imputer.fit(df_silver).transform(df_silver)

# =========================================
# 6. AUDIT KUALITAS DATA AKHIR (VALIDASI)
# =========================================
print(f"\n=== LAPORAN KUALITAS DATA (SILVER) ===")
print("-" * 50)
for column in df_silver.columns:
    null_count = df_silver.filter(col(column).isNull()).count()
    null_percentage = (null_count / total_rows) * 100
    if null_percentage == 0:
        print(f"[+] {column:<20} : 100.00% Bersih (Tervalidasi)")
    else:
        print(f"[-] {column:<20} : {null_percentage:>5.2f}% Kosong (Belum Terimputasi)")
print("-" * 50)

# =========================================
# 7. SIMPAN KE SILVER (MUTLAK ICEBERG)
# =========================================
print("\n[PROSES] Menyimpan data bersih ke Apache Iceberg Silver Layer...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
df_silver.writeTo("local.silver.kiva_loans_clean").using("iceberg").createOrReplace()

print("\nStatus: SUKSES. Pipeline Lapisan Silver Selesai.")
spark.stop()