from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, year, month, count, explode, desc, sum, avg, max, size, abs, lit, row_number, udf, coalesce, struct, lower
)
from flickrapi_utils import fetch_avatar, construct_photo_url
from pyspark.sql.types import StringType


# Utility function for efficient paging
def paginate_dataframe_sql(df: DataFrame, page: int, page_size: int, order_col: str = None) -> DataFrame:
    start_index = (page - 1) * page_size
    end_index = start_index + page_size

    if order_col:
        window = Window.orderBy(col(order_col))
        df = df.withColumn("row_number", row_number().over(window))
    else:
        df = df.withColumn("row_number", row_number().over(Window.orderBy(lit(1))))

    return df.filter((col("row_number") > start_index) & (col("row_number") <= end_index)).drop("row_number")

# Core queries
def get_first_n_rows(df: DataFrame, n: int) -> DataFrame:
    return df.limit(n)

def count_photos_by_coordinates(df: DataFrame) -> DataFrame:
    return (df.groupBy(col("geoData.latitude"), col("geoData.longitude"))
              .agg(count("id").alias("photoCount"))
              .orderBy(desc("photoCount")))

def get_photos_by_tag(df: DataFrame, tag: str) -> DataFrame:
    return (df.filter(col("tags").isNotNull())
              .filter(size(col("tags")) > 0)
              .withColumn("tag", explode(col("tags.value")))
              .filter(col("tag") == tag))

def get_photos_by_date_range(df: DataFrame, start_date: str, end_date: str) -> DataFrame:
    return df.filter((col("dateTaken") >= start_date) & (col("dateTaken") <= end_date))

def get_photos_by_location(df: DataFrame, lat: float, lon: float, radius: float) -> DataFrame:
    return df.filter((abs(col("geoData.latitude") - lat) <= radius) &
                     (abs(col("geoData.longitude") - lon) <= radius))

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
              .agg(count("id").alias("count"))
              .orderBy("month"))

def average_time_to_post(df: DataFrame) -> DataFrame:
    return (df.withColumn("timeToPost", (col("datePosted").cast("long") - col("dateTaken").cast("long")) / 3600)
              .agg(avg("timeToPost").alias("avgHoursToPost")))

def get_top_tags(df: DataFrame) -> DataFrame:
    return (df.withColumn("tagValue", explode(col("tags.value")))
              .groupBy("tagValue")
              .agg(count("id").alias("count"))
              .orderBy(desc("count")))

def calculate_top_owners(df):
    # Preparazione dei dati
    owners_df = df.select(
        col("owner.id").alias("user_id"),
        col("owner.username").alias("username"),  # Aggiunto username
        col("views"),
        col("farm"),
        col("server"),
        col("id").alias("photo_id"),
        col("secret")
    )

    # Calcolo delle statistiche per ogni owner
    aggregated_df = owners_df.groupBy("user_id", "username").agg(  # Aggiunto username nel groupBy
        sum("views").alias("total_views"),
        count("photo_id").alias("total_photos"),
        max(col("views")).alias("max_views")
    )

    window_spec = Window.partitionBy("user_id").orderBy(col("views").desc())
    most_viewed_photo_df = owners_df.withColumn("rank", row_number().over(window_spec))
    most_viewed_photo_df = most_viewed_photo_df.filter(col("rank") == 1).select(
        "user_id",
        col("farm").alias("best_farm"),
        col("server").alias("best_server"),
        col("photo_id").alias("best_photo_id"),
        col("secret").alias("best_secret")
    )

    final_df = aggregated_df.join(most_viewed_photo_df, "user_id")

    window_spec_global = Window.orderBy(col("total_views").desc())
    final_df = final_df.withColumn("rank", row_number().over(window_spec_global))

    final_df = final_df.filter(col("rank") <= 5)

    # Registrazione delle funzioni come UDF
    fetch_avatar_udf = udf(fetch_avatar, StringType())
    construct_photo_url_udf = udf(construct_photo_url, StringType())

    final_df = final_df.withColumn("avatar_url", fetch_avatar_udf(col("user_id")))
    final_df = final_df.withColumn(
        "best_photo_url",
        construct_photo_url_udf(col("best_farm"), col("best_server"), col("best_photo_id"), col("best_secret"))
    )

    result_df = final_df.select(
        "rank",
        "user_id",
        "username",  # Aggiunto username nella selezione finale
        "avatar_url",
        "total_photos",
        "best_photo_url",
        "total_views"
    )

    return result_df

def calculate_top_owners_v2(df):
    # Validazione input
    if df is None or df.rdd.isEmpty():
        return None

    # Registrazione UDF
    fetch_avatar_udf = udf(fetch_avatar, StringType())
    construct_photo_url_udf = udf(construct_photo_url, StringType())

    # Ottimizzazione: Combina tutte le operazioni in un'unica pipeline
    result_df = (df
        .select(
            col("owner.id").alias("user_id"),
            col("owner.username").alias("username"),
            coalesce(col("views"), lit(0)).alias("views"),
            struct(
                col("farm"),
                col("server"),
                col("id").alias("photo_id"),
                col("secret"),
                col("views")
            ).alias("photo_data")
        )
        # Calcola le metriche per ogni owner
        .groupBy("user_id", "username")
        .agg(
            sum("views").alias("total_views"),
            count("photo_data.photo_id").alias("total_photos"),
            max(struct("views", "photo_data")).alias("max_view_data")
        )
        # Estrai i dati della foto con più views
        .select(
            col("user_id"),
            col("username"),
            col("total_views"),
            col("total_photos"),
            col("max_view_data.photo_data.farm").alias("best_farm"),
            col("max_view_data.photo_data.server").alias("best_server"),
            col("max_view_data.photo_data.photo_id").alias("best_photo_id"),
            col("max_view_data.photo_data.secret").alias("best_secret")
        )
        # Calcola il rank globale
        .withColumn(
            "rank",
            row_number().over(Window.orderBy(col("total_views").desc()))
        )
        .filter(col("rank") <= 5)
        # Aggiungi gli URL
        .withColumn(
            "avatar_url",
            fetch_avatar_udf(col("user_id"))
        )
        .withColumn(
            "best_photo_url",
            construct_photo_url_udf(
                col("best_farm"),
                col("best_server"),
                col("best_photo_id"),
                col("best_secret")
            )
        )
        # Seleziona e ordina le colonne finali
        .select(
            "rank",
            "user_id",
            "username",
            "avatar_url",
            "total_photos",
            "best_photo_url",
            "total_views"
        )
        .orderBy("rank")
    )

    return result_df

def search_photos(df, keyword=None, dataInizio=None, dataFine=None, tag_list=None):
    # Keyword in lowercase per confronti insensibili al case
    keyword_lower = keyword.lower() if keyword else None
    construct_photo_url_udf = udf(construct_photo_url, StringType())

    # Iniziamo con un DataFrame base che contiene tutte le righe
    filtered_df = df

    # Filtra per keyword
    if keyword:
        # Preparazione: Aggiunge una colonna esplosa per i tag
        df_with_exploded_tags = filtered_df.withColumn("exploded_tag", explode(col("tags.value")))

        # Filtra in base alla keyword
        filtered_df = df_with_exploded_tags.filter(
            (lower(col("title")).contains(keyword_lower)) |
            (lower(col("description")).contains(keyword_lower)) |
            (lower(col("exploded_tag")).contains(keyword_lower)) |
            (lower(col("owner.username")).contains(keyword_lower))
        ).drop("exploded_tag")  # Rimuove la colonna temporanea

    # Filtra per intervallo di date, se specificato
    if dataInizio or dataFine:
        if dataInizio and not dataFine:
            filtered_df = filtered_df.filter(col("datePosted") >= lit(dataInizio))
        elif dataFine and not dataInizio:
            filtered_df = filtered_df.filter(col("datePosted") <= lit(dataFine))
        elif dataInizio and dataFine:
            filtered_df = filtered_df.filter(
                (col("datePosted") >= lit(dataInizio)) & (col("datePosted") <= lit(dataFine))
            )

    # Filtra per tag, se specificato
    if tag_list:
        tag_list_lower = [tag.lower() for tag in tag_list]

        # Usa un filtro per verificare se il tag esploso è in tag_list
        df_with_exploded_tags = filtered_df.withColumn("exploded_tag", explode(col("tags.value")))
        tag_filter = df_with_exploded_tags.filter(
            lower(col("exploded_tag")).isin(tag_list_lower)
        ).drop("exploded_tag")  # Rimuove la colonna temporanea

        # Unione dei risultati se esiste un filtro keyword
        if keyword:
            filtered_df = filtered_df.union(tag_filter).distinct()
        else:
            filtered_df = tag_filter

    # Rimuovere duplicati in base alla chiave primaria logica (esempio: 'id')
    filtered_df = filtered_df.dropDuplicates(["id"])

    # Aggiunge la colonna con l'URL costruito
    filtered_df = filtered_df.withColumn(
        "urlFoto", 
        construct_photo_url_udf(col("farm"), col("server"), col("id"), col("secret"))
    )

    # Seleziona i campi richiesti per l'output
    result_df = filtered_df.select(
        col("urlFoto").alias("url"),
        col("owner.username").alias("username"),
        col("tags.value").alias("tags"),
        col("views").alias("views"),
        col("title").alias("title"),
        col("description").alias("description"),
        col("dateTaken")
    )

    return result_df

def get_years(df: DataFrame) -> DataFrame:
    """
    Restituisce una lista degli anni univoci basati sul campo 'datePosted'.
    """
    return (df.filter(col("datePosted").isNotNull())
              .select(year(col("datePosted")).alias("year"))
              .distinct()
              .orderBy("year"))

def get_all_tags(df: DataFrame) -> DataFrame:
    """
    Restituisce tutti i tag univoci presenti nel dataset.
    """
    return (df.filter(col("tags").isNotNull())
              .withColumn("tag", explode(col("tags.value")))
              .select("tag")
              .distinct()
              .orderBy("tag"))

