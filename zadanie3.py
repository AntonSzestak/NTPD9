from pyspark.sql import SparkSession

# Start sesji Spark
spark = SparkSession.builder.appName("RDDExample").getOrCreate()

# Wczytanie pliku jako RDD
rdd = spark.sparkContext.textFile("dane.csv")

# Pominięcie nagłówka
header = rdd.first()
rdd_data = rdd.filter(lambda row: row != header)

# Parsowanie: zamień każdą linię na listę pól
parsed = rdd_data.map(lambda line: line.split(","))

# Zlicz liczbę wierszy
print("Liczba wierszy:", parsed.count())

# Suma wartości w kolumnie 'sprzedaz' (indeks 3)
sprzedaz_sum = parsed.map(lambda x: int(x[3])).sum()
print("Suma wartości w kolumnie 'sprzedaz':", sprzedaz_sum)

# Filtruj tylko produkty z kategorii 'Meble' (indeks 2)
meble = parsed.filter(lambda x: x[2] == "Meble").collect()
print("Produkty z kategorii 'Meble':")
for produkt in meble:
    print(produkt)

spark.stop()
