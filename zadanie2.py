from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Inicjalizacja sesji Spark
spark = SparkSession.builder \
    .appName("DataFrameExample") \
    .getOrCreate()

# Wczytanie danych z CSV
df = spark.read.option("header", "true").option("inferSchema", "true").csv("dane.csv")

# Wyświetlenie danych i schematu
df.show()
df.printSchema()

# Selekcja kolumn
df.select("produkt", "sprzedaz").show()

# Filtrowanie wierszy (np. tylko Elektronika)
df.filter(col("kategoria") == "Elektronika").show()

# Grupowanie i agregacja (suma sprzedaży w każdej kategorii)
df.groupBy("kategoria").agg(sum("sprzedaz").alias("suma_sprzedazy")).show()

# Zapisz do pliku Parquet
df.write.mode("overwrite").parquet("wynik.parquet")

spark.stop()
