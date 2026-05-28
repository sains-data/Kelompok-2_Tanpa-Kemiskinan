import os
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = r'C:\hadoop\bin;' + os.environ.get('PATH', '')
# Bronze Layer Apache Iceberg
from pyspark.sql import SparkSession


def execute_bronze(spark: SparkSession):
    print("--- Memulai Eksekusi Lapisan Bronze ---")

    # =========================================
    # Path file CSV mentah
    # =========================================
    loans_path = "data/raw/kiva_loans.csv"
    mpi_path = "data/raw/kiva_mpi_region_locations.csv"
    themes_path = "data/raw/loan_themes_by_region.csv"

    # =========================================
    # Membaca CSV mentah TANPA cleaning
    # =========================================
    loans_df = spark.read.csv(
        loans_path,
        header=True,
        inferSchema=True
    )

    mpi_df = spark.read.csv(
        mpi_path,
        header=True,
        inferSchema=True
    )

    themes_df = spark.read.csv(
        themes_path,
        header=True,
        inferSchema=True
    )

    print("CSV berhasil dibaca")

    # =========================================
    # Membuat namespace bronze
    # =========================================
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")

    # =========================================
    # Menyimpan ke Apache Iceberg
    # =========================================
    loans_df.writeTo(
        "local.bronze.kiva_loans"
    ).using("iceberg").createOrReplace()

    mpi_df.writeTo(
        "local.bronze.kiva_mpi_region_locations"
    ).using("iceberg").createOrReplace()

    themes_df.writeTo(
        "local.bronze.loan_themes_by_region"
    ).using("iceberg").createOrReplace()

    print("Data berhasil disimpan ke Apache Iceberg")
    print("--- Eksekusi Lapisan Bronze Selesai ---")


if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("Kiva_Bronze_Test")
        .master("local[*]")

        # =========================================
        # ICEBERG CONFIG
        # =========================================
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

    execute_bronze(spark)

    spark.stop()

