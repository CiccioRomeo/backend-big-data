from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import col, expr,size
import pandas as pd
from pyspark.sql.types import ArrayType, StringType


# Inizializzazione della SparkSession
spark = SparkSession.builder.appName("KMeansClustering").getOrCreate()

def run_kmeans_clustering(df: DataFrame, k: int) -> DataFrame:
    """
    Esegue il KMeans sul dataset, utilizzando come features la latitudine e la longitudine.
    Restituisce un DataFrame con colonne: centroidX, centroidY, numElements.
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

    # Unione finale per ottenere dimensioni
    final_df = cluster_sizes.join(centers_df, on="prediction")

    # Restituisce il DataFrame finale
    return final_df.select("centroidX", "centroidY", "count")


from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, size
from pyspark.ml.fpm import FPGrowth

def calculate_association_rules(df: DataFrame, 
                                 min_support: float = 0.2, 
                                 min_confidence: float = 0.6) -> DataFrame:
    """
    Calcola le regole di associazione dai dati usando FP-Growth, selezionando solo le colonne necessarie.
    Gestisce valori nulli, liste vuote e rimuove duplicati.
    
    Args:
        df (DataFrame): DataFrame Spark contenente i dati del dataset originale.
        min_support (float): Soglia minima per considerare un itemset come frequente.
        min_confidence (float): Soglia minima per considerare una regola valida.
    
    Returns:
        DataFrame: DataFrame contenente le prime 10 regole di associazione con confidenza e lift.
    """
    # Estrai i valori di tag e ignora i valori nulli
    df = df.withColumn("tags_list", expr("transform(tags, x -> x['value'])"))

    # Filtra righe dove `tags_list` è null o lista vuota
    df_filtered = df.filter((col("tags_list").isNotNull()) & (size(col("tags_list")) > 0))

    # Rimuove duplicati all'interno di ogni lista
    df_filtered = df_filtered.withColumn("unique_tags_list", expr("array_distinct(tags_list)"))

    # Mantieni solo la colonna necessaria per FP-Growth
    transactions_df = df_filtered.select("unique_tags_list").withColumnRenamed("unique_tags_list", "items")
    
    # Applica l'algoritmo FP-Growth
    fpGrowth = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence)
    model = fpGrowth.fit(transactions_df)
    
    # Estrai le regole di associazione
    association_rules = model.associationRules
    

    
    return association_rules
