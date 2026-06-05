# Arsitektur Data & Manajemen Penyimpanan (Hybrid S3 MinIO)

Dokumen ini mendeskripsikan topologi penyimpanan fisik dan logis dari sistem *Data Lakehouse* lokal. Sistem ini mengadopsi pendekatan **Hybrid Object Storage**, di mana beban kerja transformasi dilakukan di lokal (*Local Hadoop Catalog*), sementara lapisan penyajian analitik dipusatkan pada penyimpanan objek S3 (MinIO).

## 1. Peran Sentral MinIO S3
MinIO bertindak sebagai *Single Source of Truth* (SSOT) untuk lapisan agregasi bisnis. Komputasi telah di-*decouple* (dipisah). MinIO murni menyimpan berkas data Apache Iceberg (`.parquet`, `.avro`, `.json`), memungkinkan mesin analitik terdistribusi (Trino) membaca data secara serentak tanpa membebani sistem ETL *backend*.

## 2. Metrik Penyimpanan Lapisan Medali (Medallion Architecture)

### A. Lapisan Bronze (Local Hadoop Catalog)
* **Lokasi Fisik:** Direktori lokal `warehouse/bronze/`
* **Estimasi Ukuran Penyimpanan:** 58.9 MB
* **Fungsi:** Titik pendaratan (landing zone) untuk data mentah Kiva Loans. Representasi 1:1 dari data sumber tanpa alterasi struktural.
* **Karakteristik:** Tipe data belum divalidasi, masih mengandung *null values* dan inkonsistensi teks.

### B. Lapisan Silver (Local Hadoop Catalog)
* **Lokasi Fisik:** Direktori lokal `warehouse/silver/`
* **Estimasi Ukuran Penyimpanan:** 85.3 MB
* **Fungsi:** Lapisan konsolidasi (cleansing zone). Data difilter dan tipe data dikonversi ke format analitik. Anomali dasar tingkat baris telah direduksi di zona ini.

### C. Lapisan Gold (MinIO S3 Object Storage)
* **Lokasi Fisik:** S3 MinIO Endpoint (`s3a://warehouse/gold/`)
* **Ukuran Penyimpanan Aktual:** 3.9 MiB (Terdiri dari 19 objek, memuat data dan metadata Iceberg).
* **Fungsi:** Lapisan agregasi bisnis (consumption zone). Data menyeberangi jaringan untuk disimpan di S3 setelah dipecah menjadi *Star Schema* untuk kueri OLAP.
* **Tabel Iceberg:** 1. `dim_region` (Tabel Dimensi)
  2. `fact_loans` (Tabel Fakta)
* **Karakteristik:** Terbebas dari anomali *referential integrity* menggunakan teknik *Unknown Dimension Handling* untuk mencegah reduksi agregasi finansial saat proses *Left Join*.
