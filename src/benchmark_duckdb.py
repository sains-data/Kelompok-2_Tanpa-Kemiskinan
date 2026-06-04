import duckdb
import time
import os

print("--- Memulai Pengujian DuckDB pada Arsitektur Iceberg ---")

# Inisialisasi koneksi in-memory DuckDB
con = duckdb.connect()

# Install dan load ekstensi iceberg (Otomatis)
print("[PROSES] Menyiapkan ekstensi Iceberg...")
con.execute("INSTALL iceberg;")
con.execute("LOAD iceberg;")

# Mendapatkan path metadata Iceberg terbaru dari tabel fakta
metadata_dir = "warehouse/gold/fact_loans/metadata"

try:
    # Mengambil file .json metadata Iceberg terbaru
    metadata_files = [f for f in os.listdir(metadata_dir) if f.endswith(".json")]
    latest_metadata = sorted(metadata_files)[-1]
    metadata_path = os.path.join(metadata_dir, latest_metadata).replace("\\", "/")
except FileNotFoundError:
    print("ERROR: Folder metadata tidak ditemukan. Pastikan path Iceberg benar.")
    exit()

print(f"[PROSES] Membaca metadata dari: {metadata_path}")

# Kueri Agregasi Skala Besar (Murni di atas Data Bersih Star Schema)
query = f"""
    SELECT 
        region_id,
        COUNT(loan_id) as jumlah_transaksi,
        SUM(funded_amount) as total_pendanaan
    FROM iceberg_scan('{metadata_path}')
    GROUP BY region_id
    ORDER BY total_pendanaan DESC
    LIMIT 10;
"""

print("[PROSES] Mengeksekusi komputasi kueri analitik...")

# Memulai perekaman waktu (Benchmarking)
start_time = time.time()

# Eksekusi kueri dengan native Python fetch (Bypass Pandas Error)
result = con.execute(query).fetchall()

# Menghentikan perekaman waktu
end_time = time.time()
latency = end_time - start_time

print("\n=== HASIL BENCHMARK DUCKDB ===")
for row in result:
    print(row)
print(f"⏱️ Waktu Eksekusi (Latency): {latency:.4f} detik")
print("==============================\n")