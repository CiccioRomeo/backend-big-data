from pyspark.sql import SparkSession
from pyspark.sql.types import *
from endpoints import create_app

def main():
    # SparkSession
    spark = SparkSession.builder \
    .appName("BigData_Romeo_Ruggiero_PySpark") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "100s") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

    # Definizione Schema
    # schema = StructType([
    #     StructField("comments", IntegerType(), True),
    #     StructField("datePosted", StringType(), True),
    #     StructField("dateTaken", StringType(), True),
    #     StructField("description", StringType(), True),
    #     StructField("familyFlag", BooleanType(), True),
    #     StructField("farm", StringType(), True),
    #     StructField("favorite", BooleanType(), True),
    #     StructField("friendFlag", BooleanType(), True),
    #     StructField("geoData", StructType([
    #         StructField("latitude", DoubleType(), True),
    #         StructField("longitude", DoubleType(), True),
    #         StructField("accuracy", IntegerType(), True)
    #     ]), True),
    #     StructField("hasPeople", BooleanType(), True),
    #     StructField("iconFarm", StringType(), True),
    #     StructField("iconServer", StringType(), True),
    #     StructField("id", StringType(), True),
    #     StructField("lastUpdate", StringType(), True),
    #     StructField("license", StringType(), True),
    #     StructField("media", StringType(), True),
    #     StructField("mediaStatus", StringType(), True),
    #     StructField("notes", ArrayType(StringType()), True),
    #     StructField("originalFormat", StringType(), True),
    #     StructField("originalHeight", IntegerType(), True),
    #     StructField("originalSecret", StringType(), True),
    #     StructField("originalWidth", IntegerType(), True),
    #     StructField("owner", StructType([
    #         StructField("username", StringType(), True),
    #         StructField("bandwidthUsed", IntegerType(), True),
    #         StructField("revFamily", BooleanType(), True),
    #         StructField("photosCount", IntegerType(), True),
    #         StructField("admin", BooleanType(), True),
    #         StructField("pro", BooleanType(), True),
    #         StructField("bandwidthMax", IntegerType(), True),
    #         StructField("iconServer", IntegerType(), True),
    #         StructField("revContact", BooleanType(), True),
    #         StructField("revFriend", BooleanType(), True),
    #         StructField("id", StringType(), True),
    #         StructField("filesizeMax", IntegerType(), True),
    #         StructField("iconFarm", IntegerType(), True)
    #     ]), True),
    #     StructField("pathAlias", StringType(), True),
    #     StructField("placeId", StringType(), True),
    #     StructField("primary", BooleanType(), True),
    #     StructField("publicFlag", BooleanType(), True),
    #     StructField("rotation", IntegerType(), True),
    #     StructField("secret", StringType(), True),
    #     StructField("server", StringType(), True),
    #     StructField("tags", ArrayType(
    #         StructType([
    #             StructField("count", IntegerType(), True),
    #             StructField("value", StringType(), True)
    #         ])
    #     ), True),
    #     StructField("title", StringType(), True),
    #     StructField("url", StringType(), True),
    #     StructField("urls", ArrayType(StringType()), True),
    #     StructField("views", IntegerType(), True)
    # ])


    schema = StructType([
    StructField("camera_info", StructType([
        StructField("make", StringType(), True),
        StructField("model", StringType(), True)
    ]), True),
    StructField("comments", IntegerType(), True),
    StructField("datePosted", StringType(), True),
    StructField("dateTaken", StringType(), True),
    StructField("familyFlag", BooleanType(), True),
    StructField("farm", StringType(), True),
    StructField("favorite", BooleanType(), True),
    StructField("friendFlag", BooleanType(), True),
    StructField("geoData", StructType([
        StructField("accuracy", IntegerType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
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
        StructField("admin", BooleanType(), True),
        StructField("bandwidthMax", IntegerType(), True),
        StructField("bandwidthUsed", IntegerType(), True),
        StructField("filesizeMax", IntegerType(), True),
        StructField("iconFarm", IntegerType(), True),
        StructField("iconServer", IntegerType(), True),
        StructField("id", StringType(), True),
        StructField("photosCount", IntegerType(), True),
        StructField("pro", BooleanType(), True),
        StructField("revContact", BooleanType(), True),
        StructField("revFamily", BooleanType(), True),
        StructField("revFriend", BooleanType(), True),
        StructField("username", StringType(), True)
    ]), True),
    StructField("pathAlias", StringType(), True),
    StructField("photo_url", StringType(), True),
    StructField("placeId", StringType(), True),
    StructField("primary", BooleanType(), True),
    StructField("publicFlag", BooleanType(), True),
    StructField("rotation", IntegerType(), True),
    StructField("secret", StringType(), True),
    StructField("server", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("urls", ArrayType(StringType()), True),
    StructField("views", IntegerType(), True)
])

    # Leggi il file JSON
    path = "Data/definitivo.json"
    df = spark.read.schema(schema).json(path)


    df = df.cache()
    print(df.count())

    #Avvio  della web app Flask, passando il DataFrame
    app = create_app(df)
    app.run(host="127.0.0.1", port=8080, debug=False)


if __name__ == "__main__":
    main()