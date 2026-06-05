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

3. Hasil Komparasi EngineEngine AnalitikTipe PemrosesanStatus EksekusiWaktu Eksekusi (Latensi)Analisa SingkatDuckDBIn-MemorySelesai0.0674 detikMenunjukkan performa vektorisasi in-memory yang sangat cepat, dibantu oleh kondisi data yang sudah dibersihkan dari null values pada Lapisan Silver.TrinoDistributed JVM(Menunggu)[TBD]
