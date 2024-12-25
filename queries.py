from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, hour, count, explode, desc, sum, array_contains, avg, when, month, size







def get_first_n_rows(df: DataFrame, n: int) -> DataFrame:
    return df.limit(n)



def count_photos_by_coordinates(df: DataFrame, limit: int) -> DataFrame:
    """
    Raggruppa per coordinate e restituisce il numero di occorrenze per ciascuna combinazione di latitude e longitude,
    limitando il numero di risultati.

    :param df: Il DataFrame PySpark contenente i dati.
    :param limit: Numero massimo di risultati da restituire.
    :return: DataFrame con le coordinate e il conteggio delle occorrenze, limitato a un certo numero di righe.
    """
    return (df.groupBy(col("geoData.latitude").alias("latitude"), col("geoData.longitude").alias("longitude"))
              .agg(count("*").alias("occurrences"))
              .orderBy("occurrences", ascending=False)
              .limit(limit))


#RICERCA
def get_by_year(df: DataFrame, year_value: int, n: int) -> DataFrame:
    return df.filter(year(col("datePosted")) == year_value).limit(n)

def get_by_date_posted(df: DataFrame, year_value: int, n: int) -> DataFrame:
    return df.filter(year(col("datePosted")) == year_value).limit(n)

def get_photos_by_date_range(df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    return df.filter((col("dateTaken") >= start_date) & (col("dateTaken") <= end_date))

def get_photos_by_tag(df: DataFrame, tag: str) -> DataFrame:
    """
    Restituisce le foto che contengono un determinato tag.
    :param df: DataFrame PySpark contenente i dati.
    :param tag: Il tag da cercare.
    :return: DataFrame filtrato per il tag specifico.
    """
    # Filtra righe dove il campo tags è non nullo e contiene elementi
    filtered_df = df.filter(col("tags").isNotNull()).filter(size(col("tags")) > 0)

    # Esplodi l'array dei tag in righe separate
    exploded_df = filtered_df.withColumn("tag", explode(col("tags")))

    # Filtra per il valore del tag
    result_df = exploded_df.filter(col("tag.value") == tag)

    return result_df


def get_photos_by_location(df: DataFrame, lat: float, lon: float, radius: float) -> DataFrame:
    return df.filter((abs(col("geoData.latitude") - lat) <= radius) & (abs(col("geoData.longitude") - lon) <= radius))

def get_photos_by_description_keyword(df: DataFrame, keyword: str) -> DataFrame:
    return df.filter(col("description").contains(keyword))

#Serie temporali

def photo_count_by_month(df: DataFrame) -> DataFrame:
    return (df.filter(col("dateTaken").isNotNull())
              .groupBy(month(col("dateTaken")).alias("month"))
              .agg(count("id").alias("count"))
              .orderBy("month"))


def photo_count_by_year(df: DataFrame) -> DataFrame:
    return (df.filter(col("datePosted").isNotNull())
              .groupBy(year(col("datePosted")).alias("year"))
              .agg(count("id").alias("count"))
              .orderBy("year"))

def photo_posted_per_month_by_year(df: DataFrame, input_year: int) -> DataFrame:
    return (df.filter((col("datePosted").isNotNull()) & (year(col("datePosted")) == input_year))
              .groupBy(month(col("datePosted")).alias("month"))
              .count()
              .orderBy("month"))

#######TOP

def top10_tags(df: DataFrame) -> DataFrame:
    return (df
      .withColumn("tagValue", explode(col("tags.value")))
      .groupBy("tagValue")
      .count()
      .orderBy(desc("count"))
      .limit(2000))

def most_viewed_photos(df: DataFrame, n: int) -> DataFrame:
    return (df.select("url", "owner.username", "views", "comments")
              .orderBy(desc("views"))
              .limit(n))

# QUERY GEOGRAFICHE

def count_photos_by_location(df: DataFrame) -> DataFrame:
    return (df.groupBy(col("geoData.latitude"), col("geoData.longitude"))
              .agg(count("id").alias("photoCount"))
              .orderBy(desc("photoCount")))

def count_photos_with_geotag(df: DataFrame) -> DataFrame:
    return (df.withColumn("hasGeotag", when(col("geoData.latitude").isNotNull() & col("geoData.longitude").isNotNull(), 1).otherwise(0))
              .groupBy("hasGeotag")
              .agg(count("id").alias("photoCount")))

def accuracy_distribution(df: DataFrame) -> DataFrame:
    return (df.groupBy(col("geoData.accuracy"))
              .agg(count("id").alias("photoCount"))
              .orderBy("geoData.accuracy"))

# QUERY TEMPORALI



def average_time_to_post(df: DataFrame) -> DataFrame:
    return (df.withColumn("timeToPost", (col("datePosted").cast("long") - col("dateTaken").cast("long")) / 3600)
              .agg(avg("timeToPost").alias("avgHoursToPost")))

# QUERY SUI METADATI

def photo_public_private_distribution(df: DataFrame) -> DataFrame:
    return (df.groupBy("publicFlag")
              .agg(count("id").alias("photoCount"))
              .orderBy("publicFlag"))

def average_comments_and_views(df: DataFrame) -> DataFrame:
    return (df.agg(
        avg("comments").alias("avgComments"),
        avg("views").alias("avgViews")
    ))

def photo_count_with_people(df: DataFrame) -> DataFrame:
    return (df.groupBy("hasPeople")
              .agg(count("id").alias("photoCount")))

# QUERY SUI TAG

def photo_count_by_tag_count(df: DataFrame) -> DataFrame:
    return (df.withColumn("tagCount", col("tags.value").size)
              .groupBy("tagCount")
              .agg(count("id").alias("photoCount"))
              .orderBy("tagCount"))

# QUERY SUGLI UTENTI

def top_n_owners_by_views(df: DataFrame, n: int) -> DataFrame:
    return (df.groupBy("owner.username")
              .agg(
                  sum("views").alias("total_views"),
                  count("*").alias("photos_posted")
              )
              .orderBy(desc("total_views"))
              .limit(n))

def most_active_users(df: DataFrame, n: int) -> DataFrame:
    return (df.groupBy("owner.username")
              .agg(count("id").alias("photoCount"))
              .orderBy(desc("photoCount"))
              .limit(n))

def pro_users_vs_non_pro(df: DataFrame) -> DataFrame:
    return (df.groupBy("owner.pro")
              .agg(count("id").alias("photoCount"))
              .orderBy("owner.pro"))

def average_photos_per_user(df: DataFrame) -> DataFrame:
    return (df.groupBy("owner.username")
              .agg(count("id").alias("photoCount"))
              .agg(avg("photoCount").alias("avgPhotosPerUser")))
