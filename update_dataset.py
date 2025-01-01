from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, struct
from pyspark.sql.types import StringType, BooleanType, IntegerType, StructType, StructField, DoubleType, ArrayType

# Importa le funzioni di utilità
from flickrapi_utils import (
    get_photo_comments_count,
    construct_photo_url,
    has_people,
    is_owner_pro,
    get_camera_info
)

# Creazione della sessione Spark
spark = SparkSession.builder \
    .appName("Flickr Dataset Updater") \
    .getOrCreate()

print("Iniziando il caricamento del dataset...")

# Schema del dataset
schema = StructType([
    StructField("comments", IntegerType(), True),
    StructField("datePosted", StringType(), True),
    StructField("dateTaken", StringType(), True),
    StructField("description", StringType(), True),
    StructField("familyFlag", BooleanType(), True),
    StructField("farm", StringType(), True),
    StructField("favorite", BooleanType(), True),
    StructField("friendFlag", BooleanType(), True),
    StructField("geoData", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("accuracy", IntegerType(), True)
    ]), True),
    StructField("hasPeople", BooleanType(), True),
    StructField("iconFarm", StringType(), True),
    StructField("iconServer", StringType(), True),
    StructField("id", StringType(), True),
    StructField("lastUpdate", StringType(), True),
    StructField("license", StringType(), True),
    StructField("media", StringType(), True),
    StructField("mediaStatus", StringType(), True),
    StructField("notes", ArrayType(StringType()), True),
    StructField("originalFormat", StringType(), True),
    StructField("originalHeight", IntegerType(), True),
    StructField("originalSecret", StringType(), True),
    StructField("originalWidth", IntegerType(), True),
    StructField("owner", StructType([
        StructField("username", StringType(), True),
        StructField("bandwidthUsed", IntegerType(), True),
        StructField("revFamily", BooleanType(), True),
        StructField("photosCount", IntegerType(), True),
        StructField("admin", BooleanType(), True),
        StructField("pro", BooleanType(), True),
        StructField("bandwidthMax", IntegerType(), True),
        StructField("iconServer", IntegerType(), True),
        StructField("revContact", BooleanType(), True),
        StructField("revFriend", BooleanType(), True),
        StructField("id", StringType(), True),
        StructField("filesizeMax", IntegerType(), True),
        StructField("iconFarm", IntegerType(), True)
    ]), True),
    StructField("pathAlias", StringType(), True),
    StructField("placeId", StringType(), True),
    StructField("primary", BooleanType(), True),
    StructField("publicFlag", BooleanType(), True),
    StructField("rotation", IntegerType(), True),
    StructField("secret", StringType(), True),
    StructField("server", StringType(), True),
    StructField("tags", ArrayType(
        StructType([
            StructField("count", IntegerType(), True),
            StructField("value", StringType(), True)
        ])
    ), True),
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("urls", ArrayType(StringType()), True),
    StructField("views", IntegerType(), True)
])

# Caricamento del dataset
dataset_path = "Data/flickr_cleaned.json"
df = spark.read.schema(schema).json(dataset_path)

print(f"Dataset caricato con {df.count()} record.")

# UDF per le operazioni
@udf(IntegerType())
def update_comments(photo_id):
    try:
        print(f"Aggiornando commenti per foto ID {photo_id}")
        return get_photo_comments_count(photo_id)
    except Exception as e:
        print(f"Errore aggiornando commenti: {e}")
        return None

@udf(StringType())
def add_photo_url(farm, server, photo_id, secret):
    try:
        print(f"Costruendo URL per foto ID {photo_id}")
        return construct_photo_url(farm, server, photo_id, secret)
    except Exception as e:
        print(f"Errore costruendo URL: {e}")
        return None

@udf(BooleanType())
def update_has_people(photo_id):
    try:
        print(f"Controllando presenza persone per foto ID {photo_id}")
        return has_people(photo_id)
    except Exception as e:
        print(f"Errore controllando presenza persone: {e}")
        return None

@udf(BooleanType())
def update_owner_pro(user_id):
    try:
        print(f"Aggiornando stato PRO per utente ID {user_id}")
        return is_owner_pro(user_id)
    except Exception as e:
        print(f"Errore aggiornando stato PRO: {e}")
        return None

@udf("struct<make:string, model:string>")
def add_camera_info(photo_id, secret):
    try:
        print(f"Aggiornando informazioni fotocamera per foto ID {photo_id}")
        camera_info = get_camera_info(photo_id, secret)
        return camera_info
    except Exception as e:
        print(f"Errore aggiornando informazioni fotocamera: {e}")
        return {"make": None, "model": None}

# Applicazione delle UDF
print("Iniziando l'aggiornamento del dataset...")
df = df.withColumn("comments", update_comments(col("id")))
df = df.withColumn("photo_url", add_photo_url(col("farm"), col("server"), col("id"), col("secret")))
df = df.withColumn("hasPeople", update_has_people(col("id")))
df = df.withColumn("owner", struct(col("owner.username"),
                                     col("owner.bandwidthUsed"),
                                     col("owner.revFamily"),
                                     col("owner.photosCount"),
                                     col("owner.admin"),
                                     update_owner_pro(col("owner.id")).alias("pro"),
                                     col("owner.bandwidthMax"),
                                     col("owner.iconServer"),
                                     col("owner.revContact"),
                                     col("owner.revFriend"),
                                     col("owner.id"),
                                     col("owner.filesizeMax"),
                                     col("owner.iconFarm")))
df = df.withColumn("camera_info", add_camera_info(col("id"), col("secret")))

print("Aggiornamento completato. Salvando il dataset aggiornato...")

# Salvataggio del dataset aggiornato
updated_dataset_path = "Data/path_to_updated_dataset1.json"
df.write.mode("overwrite").json(updated_dataset_path)

print(f"Dataset aggiornato salvato in {updated_dataset_path}")

# Lettura del dataset aggiornato per conferma
print("Rileggendo il dataset aggiornato per verifica...")
updated_df = spark.read.json(updated_dataset_path)
updated_df.show(truncate=False)
print(f"Dataset aggiornato contiene {updated_df.count()} record.")
