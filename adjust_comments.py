from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

# Crea una SparkSession
spark = SparkSession.builder \
    .appName("Unisci e modifica JSON") \
    .getOrCreate()

# Percorso della directory contenente i file JSON di input
input_directory = "Data/dataset_updated_parts"

# Legge tutti i file JSON nella directory
json_df = spark.read.json(input_directory)

# Aggiunge il campo "comments" con valore 0 se non è presente
updated_df = json_df.withColumn(
    "comments",
    when(col("comments").isNull(), lit(0)).otherwise(col("comments"))
)

# Salva il risultato in un unico file JSON
output_path = "Data/flickr_definitivo.json"
updated_df.coalesce(1).write.json(output_path, mode="overwrite")

print(f"Output salvato nella directory: {output_path}")
