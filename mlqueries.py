import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import col, expr,size, array_contains
import pandas as pd
from pyspark.sql.types import array
from pyspark.ml.fpm import FPGrowth

# Inizializzazione della SparkSession
spark = SparkSession.builder.appName("MLQueries").getOrCreate()

def run_kmeans_clustering(df: DataFrame, k: int) -> dict:
    """
    Esegue il KMeans sul dataset, utilizzando come features la latitudine e la longitudine.
    Restituisce un dizionario con:
    - "labels": una lista di liste contenente latitude, longitude e label assegnata.
    - "centroids": una lista di liste contenente le coordinate dei centroidi.
    """
    # Appiattiamo latitude/longitude
    df_with_coords = (
        df.withColumn("latitude", col("geoData.latitude"))
          .withColumn("longitude", col("geoData.longitude"))
    )

    # Filtriamo i null e, se vuoi, i 0,0
    filtered_df = df_with_coords.filter(
        col("latitude").isNotNull() &
        col("longitude").isNotNull() &
        (col("latitude").between(-90, 90)) &
        (col("longitude").between(-180, 180))
    )

    # Assembler
    assembler = VectorAssembler(
        inputCols=["latitude", "longitude"],
        outputCol="features"
    )
    feature_df = assembler.transform(filtered_df).select("latitude", "longitude", "features")

    # KMeans
    kmeans = KMeans().setK(k).setFeaturesCol("features").setPredictionCol("prediction")
    model = kmeans.fit(feature_df)

    # Preparazione delle label
    labeled_coordinates_df = model.transform(feature_df).select("latitude", "longitude", "prediction").distinct()
    labels = labeled_coordinates_df.collect()
    labels_list = [[row.latitude, row.longitude, row.prediction] for row in labels]

    # Preparazione dei centroidi
    centroids = model.clusterCenters()
    centroids_list = [[center[0], center[1]] for center in centroids]

    # Creazione del dizionario di output
    result = {
        "labels": labels_list,
        "centroids": centroids_list
    }

    return result

def run_kmeans_clustering2(df: DataFrame, k: int) -> dict:
    """
    Esegue il KMeans sul dataset e calcola le distanze dai centroidi ai monumenti forniti.
    Restituisce un dizionario con:
    - "labels": lista di liste con latitude, longitude e label assegnata.
    - "centroids": lista di liste con le coordinate dei centroidi.
    - "distanze": lista di distanze tra monumenti e centroidi.
    """
    monuments_path= "Data/monuments.json"
    # Carica i monumenti da un file JSON
    with open(monuments_path, 'r') as f:
        monuments = json.load(f)

    # UDF per calcolare la distanza
    def calculate_distance(lat1, lon1, lat2, lon2):
        import math
        R = 6371  # Raggio terrestre in km
        dLat = math.radians(lat2 - lat1)
        dLon = math.radians(lon2 - lon1)
        a = math.sin(dLat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon / 2) ** 2
        return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Appiattiamo latitude/longitude
    df_with_coords = (
        df.withColumn("latitude", col("geoData.latitude"))
          .withColumn("longitude", col("geoData.longitude"))
    )

    # Filtriamo i null e i valori fuori dai range
    filtered_df = df_with_coords.filter(
        col("latitude").isNotNull() &
        col("longitude").isNotNull() &
        (col("latitude").between(-90, 90)) &
        (col("longitude").between(-180, 180))
    )

    # Preparazione delle feature per il KMeans
    assembler = VectorAssembler(
        inputCols=["latitude", "longitude"],
        outputCol="features"
    )
    feature_df = assembler.transform(filtered_df).select("latitude", "longitude", "features")

    # Eseguiamo il KMeans
    kmeans = KMeans().setK(k).setFeaturesCol("features").setPredictionCol("prediction")
    model = kmeans.fit(feature_df)

    # Preparazione delle label
    labeled_coordinates_df = model.transform(feature_df).select("latitude", "longitude", "prediction").distinct()
    labels = labeled_coordinates_df.collect()
    labels_list = [[row.latitude, row.longitude, row.prediction] for row in labels]

    # Preparazione dei centroidi
    centroids = model.clusterCenters()
    centroids_list = [[center[0], center[1]] for center in centroids]

    # Calcolo delle distanze dai monumenti ai centroidi
    distances_list = []
    for i, center in enumerate(centroids):
        distances = []
        for monument in monuments:
            distanza = calculate_distance(
                monument['latitudine'], monument['longitudine'],
                center[0], center[1]
            )
            distances.append({
                "monumento": monument['monumento'],
                "latitudeM": monument['latitudine'],
                "longitudeM": monument['longitudine'],
                "distanza": distanza
            })
        distances_list.append({
            "centroid": {"label": i, "latitude": center[0], "longitude": center[1]},
            "distances": distances
        })

    # Creazione del risultato finale
    result = {
        "labels": labels_list,
        "centroids": centroids_list,
        "distanze": distances_list
    }

    return result

def calculate_and_filter_association_rules(
    df, 
    min_support=0.2, 
    min_confidence=0.6, 
    target_tags=None
):
    df = df.withColumn("tags_list", expr("transform(tags, x -> x['value'])"))
    df_filtered = df.filter((col("tags_list").isNotNull()) & (size(col("tags_list")) > 0))
    df_filtered = df_filtered.withColumn("unique_tags_list", expr("array_distinct(tags_list)"))
    transactions_df = df_filtered.select("unique_tags_list").withColumnRenamed("unique_tags_list", "items")

    fpGrowth = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence)
    model = fpGrowth.fit(transactions_df)

    association_rules = model.associationRules

    if target_tags:
        # Filtra le regole di associazione per gli antecedenti
        association_rules = association_rules.filter(
            array_contains(col("antecedent"), target_tags[0])
        )

    return association_rules

