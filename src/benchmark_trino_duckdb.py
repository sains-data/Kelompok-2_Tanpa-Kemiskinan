import time
import trino
import duckdb

# ============================================================
# KONFIGURASI
# ============================================================
TRINO_HOST   = "localhost"
TRINO_PORT   = 8080
TRINO_USER   = "admin"
TRINO_CATALOG = "iceberg"

MINIO_ENDPOINT   = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
WAREHOUSE_PATH   = "s3://warehouse"

# ============================================================
# QUERY BENCHMARK (sesuai KPI proposal)
# ============================================================
QUERIES = {
    "Q1_total_pendanaan_per_region": {
        "trino": """
            SELECT region_id,
                   COUNT(loan_id)       AS jumlah_transaksi,
                   SUM(funded_amount)   AS total_pendanaan
            FROM iceberg.gold.fact_loans
            GROUP BY region_id
            ORDER BY total_pendanaan DESC
            LIMIT 10
        """,
        "duckdb_table": "fact_loans",
        "duckdb_sql": """
            SELECT region_id,
                   COUNT(loan_id)       AS jumlah_transaksi,
                   SUM(funded_amount)   AS total_pendanaan
            FROM fact_loans
            GROUP BY region_id
            ORDER BY total_pendanaan DESC
            LIMIT 10
        """
    },
    "Q2_avg_mpi_per_country": {
        "trino": """
            SELECT country,
                   AVG(average_mpi)            AS avg_mpi,
                   SUM(total_funded_amount)     AS total_dana
            FROM iceberg.gold.agg_loans_by_region
            GROUP BY country
            ORDER BY avg_mpi DESC
            LIMIT 10
        """,
        "duckdb_table": "agg_loans_by_region",
        "duckdb_sql": """
            SELECT country,
                   AVG(average_mpi)            AS avg_mpi,
                   SUM(total_funded_amount)     AS total_dana
            FROM agg_loans_by_region
            GROUP BY country
            ORDER BY avg_mpi DESC
            LIMIT 10
        """
    },
    "Q3_transaksi_per_sektor": {
        "trino": """
            SELECT ds.sector,
                   COUNT(fl.loan_id)    AS jumlah_transaksi,
                   SUM(fl.funded_amount) AS total_pendanaan
            FROM iceberg.gold.fact_loans fl
            JOIN iceberg.gold.dim_sector ds ON fl.sector_id = ds.sector_id
            GROUP BY ds.sector
            ORDER BY total_pendanaan DESC
        """,
        "duckdb_table": "fact_loans",
        "duckdb_sql": """
            SELECT ds.sector,
                   COUNT(fl.loan_id)     AS jumlah_transaksi,
                   SUM(fl.funded_amount) AS total_pendanaan
            FROM fact_loans fl
            JOIN dim_sector ds ON fl.sector_id = ds.sector_id
            GROUP BY ds.sector
            ORDER BY total_pendanaan DESC
        """
    },
    "Q4_avg_tenor_per_mpi_region": {
        "trino": """
            SELECT dr.country,
                   dr.region,
                   AVG(fl.term_in_months) AS avg_tenor,
                   AVG(dr.mpi)            AS avg_mpi
            FROM iceberg.gold.fact_loans fl
            JOIN iceberg.gold.dim_region dr ON fl.region_id = dr.region_id
            GROUP BY dr.country, dr.region
            ORDER BY avg_mpi DESC
            LIMIT 10
        """,
        "duckdb_table": "fact_loans",
        "duckdb_sql": """
            SELECT dr.country,
                   dr.region,
                   AVG(fl.term_in_months) AS avg_tenor,
                   AVG(dr.mpi)            AS avg_mpi
            FROM fact_loans fl
            JOIN dim_region dr ON fl.region_id = dr.region_id
            GROUP BY dr.country, dr.region
            ORDER BY avg_mpi DESC
            LIMIT 10
        """
    }
}

# ============================================================
# BAGIAN 1: DIAGNOSTIK KATALOG TRINO
# ============================================================
def run_diagnostik(cur):
    print("\n" + "="*60)
    print("DIAGNOSTIK KATALOG TRINO")
    print("="*60)

    print("\n[DAFTAR SCHEMA/NAMESPACE DI TRINO]")
    cur.execute("SHOW SCHEMAS FROM iceberg")
    schemas = cur.fetchall()
    for s in schemas:
        print(f"  -> Schema: {s[0]}")

    print("\n[DAFTAR TABEL YANG DITEMUKAN]")
    for s in schemas:
        schema_name = s[0]
        if schema_name not in ('information_schema', 'system'):
            try:
                cur.execute(f"SHOW TABLES FROM iceberg.{schema_name}")
                tables = cur.fetchall()
                for t in tables:
                    print(f"  [✓] iceberg.{schema_name}.{t[0]}")
            except Exception as e:
                print(f"  [✗] Gagal baca schema {schema_name}: {e}")

# ============================================================
# BAGIAN 2: BENCHMARK TRINO
# ============================================================
def run_benchmark_trino(cur):
    print("\n" + "="*60)
    print("BENCHMARK LATENSI - TRINO")
    print("="*60)

    hasil = {}
    for nama, q in QUERIES.items():
        try:
            start = time.time()
            cur.execute(q["trino"])
            rows = cur.fetchall()
            elapsed = time.time() - start
            hasil[nama] = elapsed
            print(f"  [{nama}]")
            print(f"    Latensi : {elapsed:.4f} detik")
            print(f"    Baris   : {len(rows)}")
        except Exception as e:
            hasil[nama] = None
            print(f"  [{nama}] GAGAL: {e}")
    return hasil

# ============================================================
# BAGIAN 3: BENCHMARK DUCKDB
# ============================================================
def run_benchmark_duckdb():
    print("\n" + "="*60)
    print("BENCHMARK LATENSI - DUCKDB")
    print("="*60)

    hasil = {}
    try:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""
            SET s3_endpoint='{MINIO_ENDPOINT}';
            SET s3_access_key_id='{MINIO_ACCESS_KEY}';
            SET s3_secret_access_key='{MINIO_SECRET_KEY}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            SET s3_region='us-east-1';
        """)

        # Daftarkan semua tabel gold sebagai view DuckDB
        gold_tables = [
            "fact_loans", "dim_region", "dim_sector",
            "dim_partner", "agg_loans_by_region"
        ]
        for tbl in gold_tables:
            con.execute(f"""
                CREATE OR REPLACE VIEW {tbl} AS
                SELECT * FROM parquet_scan('{WAREHOUSE_PATH}/gold/{tbl}/data/*.parquet')
            """)

        for nama, q in QUERIES.items():
            try:
                start = time.time()
                rows = con.execute(q["duckdb_sql"]).fetchall()
                elapsed = time.time() - start
                hasil[nama] = elapsed
                print(f"  [{nama}]")
                print(f"    Latensi : {elapsed:.4f} detik")
                print(f"    Baris   : {len(rows)}")
            except Exception as e:
                hasil[nama] = None
                print(f"  [{nama}] GAGAL: {e}")

        con.close()
    except Exception as e:
        print(f"  DuckDB gagal inisialisasi: {e}")

    return hasil

# ============================================================
# BAGIAN 4: RINGKASAN PERBANDINGAN
# ============================================================
def print_ringkasan(hasil_trino, hasil_duckdb):
    print("\n" + "="*60)
    print("RINGKASAN PERBANDINGAN LATENSI (detik)")
    print("="*60)
    print(f"  {'Query':<40} {'Trino':>10} {'DuckDB':>10} {'Lebih Cepat':>12}")
    print(f"  {'-'*40} {'-'*10} {'-'*10} {'-'*12}")

    for nama in QUERIES:
        t = hasil_trino.get(nama)
        d = hasil_duckdb.get(nama)
        t_str = f"{t:.4f}s" if t is not None else "GAGAL"
        d_str = f"{d:.4f}s" if d is not None else "GAGAL"

        if t is not None and d is not None:
            winner = "Trino ✓" if t < d else "DuckDB ✓"
        else:
            winner = "-"

        print(f"  {nama:<40} {t_str:>10} {d_str:>10} {winner:>12}")

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    # Koneksi Trino
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG
        )
        cur = conn.cursor()
        print("[✓] Koneksi Trino berhasil")
    except Exception as e:
        print(f"[✗] Gagal koneksi Trino: {e}")
        exit()

    # Jalankan semua bagian
    run_diagnostik(cur)
    hasil_trino = run_benchmark_trino(cur)
    hasil_duckdb = run_benchmark_duckdb()
    print_ringkasan(hasil_trino, hasil_duckdb)

    print("\n[✓] Benchmark selesai.")