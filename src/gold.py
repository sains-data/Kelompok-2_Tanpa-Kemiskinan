from pyspark.sql import SparkSession

def execute_gold(spark):

    print("--- Memulai Eksekusi Lapisan Gold ---")

    # baca data silver
    silver_df = spark.read.table(
        "local.silver.kiva_loans_clean"
    )

    # baca tabel referensi
    mpi_df = spark.read.table(
        "local.bronze.kiva_mpi_region_locations"
    )

    themes_df = spark.read.table(
        "local.bronze.loan_themes_by_region"
    )

    print("Data berhasil dibaca")

    # TODO:
    # Join region + country
    # Buat fact table
    # Buat dim table
    # Agregasi country + sector

    print("--- Gold Layer Selesai ---")


if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("Kiva_Gold")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            "spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.local.type",
            "hadoop"
        )
        .config(
            "spark.sql.catalog.local.warehouse",
            "warehouse"
        )
        .getOrCreate()
    )

    execute_gold(spark)

    spark.stop()