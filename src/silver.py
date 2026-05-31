import os
import sys
import urllib.request
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import Imputer

print("--- Memulai Eksekusi Lapisan Silver ---")

# =========================================================================
# 1. OTOMATISASI PATH HADOOP (ANTI-ERROR WINDOWS)
# =========================================================================
# Membuat folder 'hadoop_dummy/bin' tersembunyi di dalam folder proyek
dummy_hadoop_dir = os.path.join(os.getcwd(), ".hadoop_dummy")
bin_dir = os.path.join(dummy_hadoop_dir, "bin")
os.makedirs(bin_dir, exist_ok=True)

winutils_path = os.path.join(bin_dir, "winutils.exe")

# Unduh winutils.exe secara otomatis jika belum ada di folder proyek
if not os.path.exists(winutils_path):
    print("[SISTEM] Mengunduh dependensi Windows (winutils.exe)...")
    url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe"
    try:
        urllib.request.urlretrieve(url, winutils_path)
    except Exception as e:
        print(f"[PERINGATAN FATAL] Gagal mengunduh winutils.exe. Pastikan ada koneksi internet. Error: {e}")

# Paksa sistem Windows untuk melihat folder dummy ini sebagai HADOOP_HOME
os.environ["HADOOP_HOME"] = dummy_hadoop_dir
os.environ["PATH"] += os.pathsep + bin_dir
sys.path.append(bin_dir)

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
    
    if null_percentage > 0:
        print(f"[*] {column:<20} : {null_percentage:>5.2f}% Kosong")
    else:
        print(f"[+] {column:<20} : 100.00% Bersih")
    
    if null_percentage >= 80:
        if column not in whitelist_columns:
            columns_to_drop.append(column)
    elif null_percentage > 0 and null_percentage < 80:
        columns_to_impute.append(column)

print("-" * 50)

# =========================================
# 5. EKSEKUSI PEMBERSIHAN (DROP & IMPUTASI GANDA)
# =========================================
print("\n[PROSES] Mengeksekusi Data Quality Gate...")

# Eksekusi 1: Buang kolom sampah (>80%)
df_silver = df_bronze.drop(*columns_to_drop)
print(f" -> BERHASIL: {len(columns_to_drop)} kolom dibuang secara permanen.")

# Pemisahan Kolom untuk Imputasi (<80%)
numeric_cols = []
string_cols = []

for c in columns_to_impute:
    dtype = dict(df_bronze.dtypes)[c]
    if dtype in ['int', 'double', 'float', 'bigint']:
        numeric_cols.append(c)
    elif dtype == 'string':
        string_cols.append(c)

# Eksekusi 2a: Tambal kolom numerik dengan Median
if numeric_cols:
    imputer = Imputer(inputCols=numeric_cols, outputCols=numeric_cols).setStrategy("median")
    df_silver = imputer.fit(df_silver).transform(df_silver)
    print(f" -> BERHASIL: Imputasi Median pada {len(numeric_cols)} kolom angka.")

# Eksekusi 2b: Tambal kolom teks dengan String Konstan
if string_cols:
    df_silver = df_silver.fillna("Tidak Diketahui", subset=string_cols)
    print(f" -> BERHASIL: Imputasi Teks Konstan pada {len(string_cols)} kolom teks.")

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