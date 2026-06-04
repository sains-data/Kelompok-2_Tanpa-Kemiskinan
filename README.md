# 📊 Arsitektur Data Lakehouse: Kiva Loans & MPI
**Evaluasi Kinerja Query Engine (Trino vs DuckDB) pada Format Apache Iceberg**

Repositori ini berisi implementasi *Batch Data Pipeline* yang dirancang untuk mengintegrasikan, membersihkan, dan memodelkan data pembiayaan mikro Kiva beserta Indeks Kemiskinan Multidimensi (MPI). Proyek ini berfokus pada pembangunan arsitektur ETL/ELT yang tangguh dan evaluasi metrik performa (*latency*) dari dua mesin analitik terkemuka: **Trino** dan **DuckDB**.

---

## 🏗️ Arsitektur Sistem (Medallion Architecture)

Sistem ini memproses data melalui tiga lapisan kualitas menggunakan **Apache Spark** dan format penyimpanan persisten **Apache Iceberg** (mendukung *ACID Transactions*). Eksekusi *pipeline* dilakukan secara sekuensial.

* **1. Bronze Layer (Raw Data):** Titik masuk data mentah. Data dari Kiva dan MPI dimuat secara utuh (*as-is*) tanpa transformasi untuk menjaga integritas *single source of truth*.
* **2. Silver Layer (Cleansed & Conformed):** Lapisan *Data Quality Gate*. Sistem secara otomatis membuang atribut dengan tingkat kekosongan ekstrem (>= 80%) dan melakukan "Imputasi Ganda" pada atribut berlubang minor (< 80%). Nilai numerik ditambal dengan algoritma Median, sementara data spasial/teks ditambal dengan konstanta "Tidak Diketahui".
* **3. Gold Layer (Business-Ready):** Lapisan pemodelan dimensional. Data bersih dari Silver dipecah menjadi **Star Schema** (Tabel Fakta dan Tabel Dimensi) yang secara khusus dioptimalkan untuk beban kerja *Online Analytical Processing* (OLAP).

---

## 🎯 Tujuan Analitik & Indikator Kinerja Utama (KPI Bisnis)

Arsitektur data ini dibangun secara spesifik untuk menyuplai mesin kueri (Trino & DuckDB) agar mampu mengevaluasi lima sasaran analitik bisnis berikut:

| Analisis Proses Bisnis | Tujuan Analitik (Objective) | Key Performance Indicator (KPI) | Metrik Teknis (SQL) |
| :--- | :--- | :--- | :--- |
| **1. Ketimpangan Alokasi Pendanaan** | Memetakan korelasi antara tingkat kerentanan wilayah dengan volume distribusi pinjaman mikro. | **Rasio Penetrasi Dana:** Persentase total pendanaan di region MPI tinggi dibandingkan region MPI rendah. | `SUM(funded_amount)`, `AVG(mpi_score)` berdasarkan `region` & `country`. |
| **2. Pemetaan Sektoral Pinjaman** | Mengukur keselarasan aliran dana dengan sektor esensial di zona miskin. | **Konsentrasi Sektor Prioritas:** Persentase volume pinjaman pada sektor esensial (Pertanian/Kesehatan). | `SUM(funded_amount)` berdasarkan `sector`, `theme`, & kategori MPI. |
| **3. Evaluasi Kinerja Mitra** | Memantau agresivitas penyaluran dana oleh mitra lapangan di zona-zona rentan. | **Kontribusi Mitra Rentan:** Total volume dana & jumlah transaksi per mitra khusus di wilayah ber-MPI ekstrem. | `SUM(funded_amount)`, `COUNT(loan_id)` berdasarkan `partner_id` & `region`. |
| **4. Analisis Beban Tenor** | Menganalisis sebaran tenggat waktu pelunasan (tenor) di berbagai tingkat kemiskinan wilayah. | **Indeks Beban Tenor:** Rata-rata durasi pelunasan (bulan) yang dikorelasikan dengan tingkat kemiskinan. | `AVG(term_in_months)` berdasarkan klasifikasi skor MPI. |
| **5. Kesetaraan Pendanaan (Gender)** | Mengevaluasi proporsi jangkauan dana mikro terhadap kelompok perempuan di area miskin. | **Rasio Kesetaraan Gender:** Persentase total pendanaan untuk peminjam perempuan di zona kemiskinan ekstrem. | `SUM(funded_amount)` berdasarkan `borrower_genders` & kategori MPI. |

---

## ✨ Fitur Utama Arsitektur

* **Automated Data Quality Gate:** Pemindaian dan perbaikan *missing values* secara dinamis di Lapisan Silver tanpa *hardcoding* pembuangan atribut.
* **Robust File Handling:** Implementasi skrip *auto-download dummy environment* untuk PySpark di OS Windows (mencegah *fatal error* absennya `winutils.exe`).
* **Iceberg Time Travel & ACID:** Penyimpanan data persisten yang kebal terhadap korupsi file saat terjadi proses penimpaan (*overwrite*) tabel secara masif.
* **Analytical Engine Benchmarking:** Desain *Star Schema* di Lapisan Gold yang disiapkan khusus untuk membebani dan menguji batas performa komputasi antara Trino dan DuckDB.

---

## ⚙️ Persyaratan Sistem & Eksekusi

Sistem ini dirancang untuk dijalankan dalam *Virtual Environment* secara terisolasi.
* **Inti Sistem:** Python 3.10+, PySpark 3.5.x, Apache Iceberg Runtime 3.5

**Urutan Eksekusi Pipeline:**
```bash
# 1. Ekstraksi Data Mentah
python src/bronze.py

# 2. Eksekusi Pembersihan dan Imputasi Ganda
python src/silver.py

# 3. Transformasi ke Model Dimensional
python src/gold.py

```
👥 Identitas Tim Pengembang
Kelompok 2 - Program Studi Sains Data, Institut Teknologi Sumatera (ITERA)

Vania Claresta – Lead Data Engineer & Architect

Raihana Adelia Putri – Data Engineer (Bronze Layer)

Ihsan Maulana Yusuf  – Data Engineer (Silver Layer & Data Quality)

Fairuz Ary Syifa – Data Modeler (Gold Layer / Star Schema)

Muhammad Hanif Faros  – BI Analyst & Performance Evaluator (Trino vs DuckDB)
