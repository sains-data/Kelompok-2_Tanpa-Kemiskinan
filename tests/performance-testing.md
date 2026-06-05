# Laporan Pengujian Performa (Benchmarking)
**Arsitektur Target:** Apache Iceberg (Lapisan Gold - Star Schema)
**Dataset:** Kiva Loans

## 1. Lingkungan Pengujian
Pengujian latensi (*latency benchmarking*) ini dieksekusi secara langsung di atas direktori Lapisan Gold yang telah distrukturkan ke dalam bentuk *Star Schema* (penggabungan Tabel Fakta dan Tabel Dimensi). Pendekatan ini menguji kemampuan murni dari mesin pemroses analitik dalam membaca *metadata* Iceberg tanpa lapisan perantara.

## 2. Kueri Uji (ANSI SQL)
Kueri agregasi skala besar berikut digunakan untuk menghitung jumlah transaksi dan total pendanaan yang dikelompokkan berdasarkan region:

```sql
SELECT 
    region_id,
    COUNT(loan_id) as jumlah_transaksi,
    SUM(funded_amount) as total_pendanaan
FROM iceberg_scan('warehouse/gold/fact_loans/metadata/v2.metadata.json')
GROUP BY region_id
ORDER BY total_pendanaan DESC
LIMIT 10;

```
### Hasil Komparasi Mesin Analitik

| Engine Analitik | Arsitektur Pemrosesan | Waktu Eksekusi (Latensi) | Analisis Komparatif |
| :--- | :--- | :--- | :--- |
| **DuckDB** | *In-Process / In-Memory* | **0.0674 detik** | **Menang mutlak di skala lokal.** Performa mesin vektorisasi *in-process* mengeksekusi metadata Iceberg secara langsung dari disk/memory lokal tanpa perantara peladen HTTP, menghindari seluruh *overhead* komunikasi jaringan. |
| **Trino** | *Decoupled Distributed JVM* | **5.87 detik** | **Terjadi *bottleneck* arsitektural.** Memaksa mesin komputasi terdistribusi yang dirancang untuk skala-*petabyte* agar mengeksekusi data berukuran mikro (~4 MB) melalui lapisan jaringan S3 MinIO di dalam lingkungan *single-node* justru mencekik latensi. Waktu dihabiskan untuk *overhead* API (HTTP/REST) dan inisialisasi *split* pekerja (*workers*), bukan untuk komputasi data itu sendiri. |
