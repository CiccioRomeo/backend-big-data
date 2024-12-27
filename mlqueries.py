from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col, radians, sin, cos, sqrt, atan2, lit
from pyspark.ml.functions import vector_to_array
import pandas as pd

# Inizializzazione della SparkSession
spark = SparkSession.builder.appName("KMeansClustering").getOrCreate()

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calcola la distanza tra due punti sulla superficie della Terra in metri usando la formula dell'Haversine.
    """
    R = 6371000  # Raggio medio della Terra in metri
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def run_kmeans_clustering(df: DataFrame, k: int) -> DataFrame:
    """
    Esegue il KMeans sul dataset, utilizzando come features la latitudine e la longitudine.
    Restituisce un DataFrame con colonne: centroidX, centroidY, numElements, radius.
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
    feature_df = assembler.transform(filtered_df).select("features")

    # KMeans
    kmeans = KMeans().setK(k).setFeaturesCol("features").setPredictionCol("prediction")
    model = kmeans.fit(feature_df)

    # Centri
    centers = model.clusterCenters()

    # Creazione di un DataFrame con i centroidi
    centers_df = spark.createDataFrame(
        pd.DataFrame([(idx, float(center[0]), float(center[1])) for idx, center in enumerate(centers)],
                     columns=["prediction", "centroidX", "centroidY"])
    )

    # Calcolo dei numeri di elementi per ciascun cluster
    clustered_df = model.transform(feature_df)
    cluster_sizes = clustered_df.groupBy("prediction").count()

    # Convertiamo il vettore features in un array per estrarre i valori
    clustered_df = clustered_df.withColumn("features_array", vector_to_array(col("features")))

    # Unione con i centroidi
    joined_df = clustered_df.join(centers_df, on="prediction")

    # Calcolo della distanza utilizzando l'Haversine formula
    distances_df = joined_df.withColumn(
        "distance",
        haversine_distance(
            col("features_array")[0], col("features_array")[1],  # latitudine e longitudine del punto
            col("centroidX"), col("centroidY")                 # latitudine e longitudine del centroide
        )
    )

    # Calcolo del raggio massimo (distanza massima in metri)
    max_distances = distances_df.groupBy("prediction").agg({"distance": "max"})
    max_distances = max_distances.withColumnRenamed("max(distance)", "radius")

    # Unione finale per ottenere dimensioni e raggi
    final_df = cluster_sizes.join(centers_df, on="prediction").join(max_distances, on="prediction")

    # Restituisce il DataFrame finale
    return final_df.select("centroidX", "centroidY", "count", "radius")

