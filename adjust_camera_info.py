from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, struct

# Crea una SparkSession
spark = SparkSession.builder \
    .appName("Aggiungi camera_info predefinito") \
    .getOrCreate()

# Percorso della directory contenente i file JSON di input
input_file = "Data/adjusted_comments.json"

# Leggi tutti i file JSON nella directory
json_df = spark.read.json(input_file)

# Aggiungi i valori predefiniti al campo "camera_info" se è vuoto o mancante
updated_df = json_df.withColumn(
    "camera_info",
    when(
        col("camera_info").isNull() | (col("camera_info.make").isNull()) | (col("camera_info.model").isNull()),
        struct(
            lit("Marca fotocamera non disponibile").alias("make"),
            lit("Modello fotocamera non disponibile").alias("model")
        )
    ).otherwise(col("camera_info"))
)

# Salva il risultato in un unico file JSON
output_path = "Data/output"
updated_df.coalesce(1).write.json(output_path, mode="overwrite")

print(f"Output salvato nella directory: {output_path}")
