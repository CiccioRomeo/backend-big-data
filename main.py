from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp, when, lit
from endpoints import create_app

def main():
    # SparkSession
    spark = (SparkSession.builder
        .appName("BigData_Romeo_Ruggiero_PySpark")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.network.timeout", "600s")  
        .config("spark.executor.heartbeatInterval", "100s") 
        .config("spark.driver.maxResultSize", "2g") 
        .getOrCreate())

    # Definizione Schema
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

    # Leggi il file JSON
    path = "Data/flickr2x.json"
    df = spark.read.schema(schema).json(path)

    # 4) Pulizia e trasformazioni
    # Rimuove duplicati
    df = df.dropDuplicates()

    # Converte datePosted
    # (formato "MMM d, yyyy h:mm:ss a", es. "Jul 29, 2014 12:00:33 PM")
    df = df.withColumn(
        "datePosted",
        to_timestamp(col("datePosted"), "MMM d, yyyy h:mm:ss a")
    )

    # Converte dateTaken (escludendo date "Jan 1, 0001 ..." e "Jan 1, 1000 ...")
    df = df.withColumn(
        "dateTaken",
        when(
            col("dateTaken").isin("Jan 1, 0001 12:00:00 AM", "Jan 1, 1000 12:00:00 AM"),
            lit(None)
        ).otherwise(
            to_timestamp(col("dateTaken"), "MMM d, yyyy h:mm:ss a")
        )
    )

    # Converte lastUpdate
    df = df.withColumn(
        "lastUpdate",
        to_timestamp(col("lastUpdate"), "MMM d, yyyy h:mm:ss a")
    )

    df = df.cache()
    print(df.count())

    #Avvio  della web app Flask, passando il DataFrame
    app = create_app(df)
    app.run(host="127.0.0.1", port=8080, debug=False)


if __name__ == "__main__":
    main()