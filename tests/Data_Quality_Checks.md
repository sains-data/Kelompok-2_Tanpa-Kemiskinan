# Laporan Kualitas Data (Data Quality Checks)
**Tahap:** Lapisan Silver (Data Cleansing)
**Sumber Data Mentah:** `kiva_loans.csv`

## 1. Metrik Evaluasi (Business Rule)
Untuk memastikan integritas model analitik pada Lapisan Gold (Star Schema), kami menetapkan metrik kepatuhan kualitas data (*Data Quality Compliance*) yang ketat: **Toleransi maksimal untuk nilai kosong (*Null Values*) dalam satu kolom adalah 80%.** Kolom dengan rasio kekosongan melebihi batas tersebut dianggap tidak representatif secara statistik dan akan dihapus (*dropped*) dari arsitektur.

## 2. Implementasi Sistem
Evaluasi ini tidak dilakukan secara manual. Kami mengimplementasikan skrip `src/silver.py` yang menggunakan Apache Spark (PySpark) untuk memindai seluruh kolom pada Lapisan Bronze, menghitung persentase *null* secara dinamis, dan mengeksekusi penghapusan kolom secara otomatis sebelum data dimasukkan ke dalam *namespace* Silver.

## 3. Hasil Eksekusi (Log Bukti Empiris)
Berdasarkan eksekusi *pipeline* pada Lapisan Silver, sistem membaca profil data dan mengonfirmasi bahwa seluruh parameter berada dalam batas aman. Berikut adalah kutipan log mesin (*Standard Output*) dari proses tersebut:

> `[PROSES] Mengevaluasi dan membuang kolom dengan Null > 80%...`
> `[METRIK] Tidak ada kolom yang melebihi batas Null 80%.`
> `[PROSES] Menyimpan ke Apache Iceberg (Silver)...`
> `Status: SUKSES. Eksekusi Lapisan Silver Selesai.`

**Kesimpulan:** Tingkat kelengkapan atribut (kolom) pada *Data Warehouse* kami mencapai **100% patuh** terhadap batas toleransi yang ditetapkan. Arsitektur data dinyatakan steril dari kolom kosong yang tidak valid dan siap diproses untuk pembentukan Dimensi dan Fakta di Lapisan Gold.
