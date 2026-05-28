from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# =========================
# START SPARK
# =========================
spark = (
    SparkSession.builder
    .appName("Silver Layer")
    .master("local[*]")
    .getOrCreate()
)

print("Spark berhasil berjalan")

# =========================
# LOAD DATA
# =========================
df = spark.read.csv(
    "data/kiva_loans.csv",
    header=True,
    inferSchema=True
)

# =========================
# HITUNG TOTAL BARIS
# =========================
total_rows = df.count()

print(f"Total rows: {total_rows}")

# =========================
# CEK PERSENTASE NULL
# =========================
columns_to_drop = []

for column in df.columns:

    null_count = df.filter(
        col(column).isNull()
    ).count()

    null_percentage = (null_count / total_rows) * 100

    print(f"{column} -> NULL {null_percentage:.2f}%")

    if null_percentage > 80:
        columns_to_drop.append(column)

# =========================
# DROP COLUMN > 80%
# =========================
df_clean = df.drop(*columns_to_drop)

print("\nKolom yang dihapus:")
print(columns_to_drop)

print("\nSisa kolom:")
print(df_clean.columns)

# =========================
# TAMPILKAN DATA
# =========================
df_clean.show(5)

spark.stop()