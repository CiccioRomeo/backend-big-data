from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, year, month, count, explode, desc, sum, avg, max, size, abs, lit, row_number, udf, coalesce, struct, lower, date_format, hour, count, asc, unix_timestamp, to_timestamp, percentile_approx, countDistinct
)
from pyspark.sql.functions import min as minp
from pyspark.sql.types import StringType

from flickrapi_utils import fetch_avatar, construct_photo_url

def paginate_dataframe_sql(df: DataFrame, page: int, page_size: int, order_col: str = None) -> DataFrame:
    """
    Permette la paginazione efficiente di un DataFrame PySpark.
    :param df: DataFrame PySpark contenente i dati.
    :param page: Numero della pagina da visualizzare.
    :param page_size: Numero di righe per pagina.
    :param order_col: (Opzionale) Colonna per l'ordinamento.
    :return: DataFrame contenente solo le righe della pagina selezionata.
    """
    start_index = (page - 1) * page_size
    end_index = start_index + page_size

    if order_col:
        window = Window.orderBy(col(order_col))
        df = df.withColumn("row_number", row_number().over(window))
    else:
        df = df.withColumn("row_number", row_number().over(Window.orderBy(lit(1))))

    return df.filter((col("row_number") > start_index) & (col("row_number") <= end_index)).drop("row_number")


def get_years(df: DataFrame) -> DataFrame:
    """
    Restituisce una lista degli anni univoci basati sul campo 'datePosted'.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con gli anni univoci ordinati.
    """
    return (df.filter(col("datePosted").isNotNull())
              .select(year(col("datePosted")).alias("year"))
              .distinct()
              .orderBy("year"))

def get_first_n_rows(df: DataFrame, n: int) -> DataFrame:
    """
    Restituisce le prime n righe del DataFrame.
    :param df: DataFrame PySpark contenente i dati.
    :param n: Numero di righe da restituire.
    :return: DataFrame con le prime n righe.
    """
    return df.limit(n)

def count_photos_by_coordinates(df: DataFrame) -> DataFrame:
    """
    Conta le foto per combinazione di coordinate geografiche (latitudine e longitudine).
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto per coordinate.
    """
    return (df.groupBy(col("geoData.latitude"), col("geoData.longitude"))
              .agg(count("id").alias("photoCount"))
              .orderBy(desc("photoCount")))

def get_photos_by_tag(df: DataFrame, tag: str) -> DataFrame:
    """
    Filtra le foto che contengono uno specifico tag.
    :param df: DataFrame PySpark contenente i dati.
    :param tag: Tag da ricercare.
    :return: DataFrame con le foto corrispondenti al tag specificato.
    """
    return (df.filter(col("tags").isNotNull())
              .filter(size(col("tags")) > 0)
              .withColumn("tag", explode(col("tags.value")))
              .filter(col("tag") == tag))

def photo_count_by_month_posted(df: DataFrame) -> DataFrame:
    """
    Conta le foto postate per mese.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto postate per mese.
    """
    return (df.filter(col("datePosted").isNotNull())
              .groupBy(month(col("datePosted")).alias("month"))
              .agg(count("id").alias("count"))
              .orderBy("month"))

def photo_count_by_year_posted(df: DataFrame) -> DataFrame:
    """
    Conta le foto postate per anno.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto postate per anno.
    """
    return (df.filter(col("datePosted").isNotNull())
              .groupBy(year(col("datePosted")).alias("year"))
              .agg(count("id").alias("count"))
              .orderBy("year"))

def photo_count_by_month_taken(df: DataFrame) -> DataFrame:
    """
    Conta le foto scattate per mese.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto scattate per mese.
    """
    return (df.filter(col("dateTaken").isNotNull())
              .groupBy(month(col("dateTaken")).alias("month"))
              .agg(count("id").alias("count"))
              .orderBy("month"))

def photo_count_by_year_taken(df: DataFrame) -> DataFrame:
    """
    Conta le foto scattate per anno.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto scattate per anno.
    """
    return (df.filter(col("dateTaken").isNotNull())
              .groupBy(year(col("dateTaken")).alias("year"))
              .agg(count("id").alias("count"))
              .orderBy("year"))

def photo_posted_per_month_by_year_posted(df: DataFrame, input_year: int) -> DataFrame:
    """
    Conta le foto postate per mese in un anno specifico.
    :param df: DataFrame PySpark contenente il dataset.
    :param input_year: Anno da analizzare.
    :return: DataFrame con il conteggio mensile delle foto postate nell'anno specificato.
    """
    return (df.filter((col("datePosted").isNotNull()) & (year(col("datePosted")) == input_year))
              .groupBy(month(col("datePosted")).alias("month"))
              .agg(count("id").alias("count"))
              .orderBy("month"))

def photo_posted_per_month_by_year_taken(df: DataFrame, input_year: int) -> DataFrame:
    """
    Conta le foto scattate per mese in un anno specifico.
    :param df: DataFrame PySpark contenente il dataset.
    :param input_year: Anno da analizzare.
    :return: DataFrame con il conteggio mensile delle foto scattate nell'anno specificato.
    """
    return (df.filter((col("dateTaken").isNotNull()) & (year(col("dateTaken")) == input_year))
              .groupBy(month(col("datePosted")).alias("month"))
              .agg(count("id").alias("count"))
              .orderBy("month"))

def count_photos_posted_per_hour(df):
    """
    Conta le foto postate per ora del giorno.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto per ora.
    """
    return (df.withColumn("hourPosted", hour(col("datePosted")))
                .filter(col("hourPosted").isNotNull())
                .groupBy("hourPosted")
                .agg(count("*").alias("count"))
                .orderBy(asc("hourPosted")))

def count_photos_taken_per_hour(df):
    """
    Conta le foto scattate per ora del giorno.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il conteggio delle foto per ora.
    """
    return (df.withColumn("hourTaken", hour(col("dateTaken")))
              .filter(col("hourTaken").isNotNull())
              .groupBy("hourTaken")
              .agg(count("*").alias("photosTakenCount"))
              .orderBy(asc("hourTaken")))

def calculate_views_by_year(df):
    """
    Raggruppa le foto per anno di pubblicazione e calcola la media delle visualizzazioni per anno.
    :param df: DataFrame PySpark contenente il dataset
    :return: DataFrame con anno e media delle visualizzazioni
    """
    return df.groupBy(year(col("datePosted")).alias("yearPosted")).agg(
        avg("views").alias("average_views")
    ).orderBy(col("yearPosted").asc())


def calculate_comments_by_year(df):
    """
    Raggruppa le foto per anno di pubblicazione e calcola la media dei commenti per anno.
    :param df: DataFrame PySpark contenente il dataset
    :return: DataFrame con anno e media delle commenti
    """
    return df.groupBy(year(col("datePosted")).alias("yearPosted")).agg(
        avg("comments").alias("average_comments")
    ).orderBy(col("yearPosted").asc())


def calculate_pro_user_distribution(df):
    """
    Conta il numero di utenti pro e non-pro.
    :param df: DataFrame PySpark contenente il dataset
    :return: DataFrame con il conteggio di utenti pro e non-pro
    """
    return df.filter(col("owner.pro").isNotNull()).groupBy("owner.pro").agg(
        count("*").alias("user_count")
    ).orderBy(col("owner.pro").desc())


def calculate_average_time_to_post(df):
    """
    Calcola il tempo medio (in minuti) tra lo scatto e la pubblicazione delle foto.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con il tempo medio di pubblicazione.
    """
    df_filtered = df.filter((col("datePosted").isNotNull()) & (col("dateTaken").isNotNull()))

    df_with_time_diff = df_filtered.withColumn(
        "timeToPost",
        (unix_timestamp(to_timestamp(col("datePosted"))) - unix_timestamp(to_timestamp(col("dateTaken")))) / 60
    )
    average_time_to_post = df_with_time_diff.agg(avg("timeToPost").alias("averageTimeToPostMinutes"))
    return average_time_to_post


def first_post_per_year_month(df):
    """
    Calcola il numero di primi post effettuati dagli utenti in ciascun anno e mese.
    :param df: DataFrame PySpark contenente il dataset
    :return: DataFrame con anno, mese e numero di primi post
    """
    
    df_ts = df.withColumn(
        "posted_timestamp",
        to_timestamp("datePosted", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    )

    # 2. Raggruppiamo per utente e troviamo la data minima (primo post) di ogni utente
    first_posts_df = (
        df_ts.groupBy("owner.id")
             .agg(minp("posted_timestamp").alias("first_post_ts"))
    )

    # 3. Aggiungiamo le colonne year e month al DataFrame dei primi post
    first_posts_df = first_posts_df.withColumn("year", year("first_post_ts")) \
                                   .withColumn("month", month("first_post_ts"))

    # 4. Raggruppiamo per anno e mese, contando il numero di utenti 
    #    che hanno il loro "primo post" in quell'anno/mese
    result = (
        first_posts_df.groupBy("year", "month")
                      .agg(count("*").alias("count"))
                      .orderBy("year", "month")
    )

    return result

def calculate_views_stats(df):
    """
    Calcola la media e la mediana di 'views'.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con media e mediana di 'views'.
    """
    return (df.select(
        avg("views").alias("average_views"),
        percentile_approx("views", 0.5).alias("median_views")
    ))

def calculate_comments_stats(df):
    """
    Calcola la media e la mediana di 'comments'.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con media e mediana di 'comments'.
    """
    return (df.select(
        avg("comments").alias("average_comments"),
        percentile_approx("comments", 0.5).alias("median_comments")
    ))

def calculate_accuracy_distribution(df):
    """
    Calcola la distribuzione dei valori di 'accuracy'.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con la distribuzione dei valori di 'accuracy'.
    """
    return (df.groupBy("geoData.accuracy")
            .agg(count("*").alias("count"))
            .orderBy(col("geoData.accuracy").asc()))

def get_top_tags(df: DataFrame) -> DataFrame:
    """
    Restituisce i tag più frequenti nel dataset.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con i tag e il loro conteggio.
    """
    return (df.withColumn("tagValue", explode(col("tags.value")))
              .groupBy("tagValue")
              .agg(count("id").alias("count"))
              .orderBy(desc("count")))

def count_user(df: DataFrame)-> DataFrame:
    """
    Restituisce il numero di utenti presenti nel dataset.
    :param df: DataFrame PySpark contenente il dataset.
    :return: Numero totale di utenti unici.
    """
    return(df.select("owner.id").distinct().count())


def search_owner(df, username):
    """
    Cerca informazioni sull'utente basandosi sul nome utente.
    :param df: DataFrame PySpark contenente i dati.
    :param username: Nome utente da cercare.
    :return: DataFrame con informazioni dettagliate sull'utente.
    """
    construct_photo_url_udf = udf(construct_photo_url, StringType())

    # Estrarre i campi nidificati dalla colonna owner
    df = df.withColumn("owner_id", col("owner.id")).withColumn("owner_username", col("owner.username"))

    result_df = (df
        .groupBy("owner_id", "owner_username")
        .agg(
            sum(coalesce(col("views"), lit(0))).alias("total_views"),
            sum(coalesce(col("comments"), lit(0))).alias("total_comments"),
            count("id").alias("total_photos"),
            max(struct(
                coalesce(col("views"), lit(0)).alias("views"),
                coalesce(col("comments"), lit(0)).alias("comments"),
                col("farm"), col("server"), col("id").alias("photo_id"), col("secret")
            )).alias("max_photo")
        )
        .withColumn("rank", row_number().over(Window.orderBy(col("total_views").desc())))
        .withColumn(
            "best_photo_url",
            construct_photo_url_udf(
                col("max_photo.farm"),
                col("max_photo.server"),
                col("max_photo.photo_id"),
                col("max_photo.secret")
            )
        )
        .select(
            col("rank"),
            col("owner_id").alias("user_id"),
            col("owner_username").alias("username"),
            col("total_photos"),
            col("total_comments"),
            col("max_photo.views").alias("most_viewed_photo_views"),
            col("max_photo.comments").alias("most_viewed_photo_comments"),
            col("best_photo_url"),
            col("total_views")
        )
    )

    if username:
        result_df = result_df.filter(lower(col("username")).contains(username.lower()))

    # Aggiunta della chiamata fetch_avatar solo sul DataFrame finale
    result_df = result_df.withColumn("avatar_url", udf(fetch_avatar, StringType())(col("user_id")))

    return result_df


def top_50_owners(df):
    """
    Restituisce i top 50 owner per views totali.
    :param df: DataFrame PySpark contenente il dataset.
    :return: DataFrame con gli username e il numero di views totali.
    """
    return (df.groupBy("owner.username")
          .agg(sum("views").alias("total_views"))
          .orderBy(col("total_views").desc())
          .limit(50)
    )


def search_photos(df, keyword=None, dataInizio=None, dataFine=None, tag_list=None):
  
    keyword_lower = keyword.lower() if keyword else None
    construct_photo_url_udf = udf(construct_photo_url, StringType())


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

        # Usa un filtro per verificare se il tag esploso Ã¨ in tag_list
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

    # Formatta le date nel formato richiesto
    formatted_df = filtered_df.withColumn("dateTakenFormatted", date_format(col("dateTaken"), "HH:mm - dd/MM/yyyy"))
    formatted_df = formatted_df.withColumn("datePostedFormatted", date_format(col("datePosted"), "HH:mm - dd/MM/yyyy"))

    # Seleziona i campi richiesti per l'output
    result_df = formatted_df.select(
        col("urlFoto").alias("url"),
        col("owner.username").alias("username"),
        col("tags.value").alias("tags"),
        col("views").alias("views"),
        col("title").alias("title"),
        col("dateTakenFormatted").alias("dateTaken"),
        col("datePostedFormatted").alias("datePosted")
    )

    return result_df


def top_brands_with_models(df: DataFrame):
    """
    Restituisce i top 5 brand e per ogni top brand i 5 modelli più utilizzati.
    :param df: DataFrame PySpark contenente i dati delle foto.
    :return: DataFrame con top 5 brand e top 5 modelli per ogni brand.
    """
    df = df.withColumn("make", col("camera_info.make"))
    df = df.withColumn("model", col("camera_info.model"))

    filtered_df = df.filter(
        (col("make") != "Marca fotocamera non disponibile") &
        (col("model") != "Modello fotocamera non disponibile")
    )

    # Filtra per camera_info disponibile
    filtered_df = df.filter(col("make").isNotNull() & col("model").isNotNull())

    # Conta il numero di foto per ogni brand e modello
    brand_model_counts = filtered_df.groupBy("make", "model").count()

    # Trova i top 5 brand
    top_brands = brand_model_counts.groupBy("make").agg(sum("count").alias("total_count"))
    top_brands = top_brands.orderBy(desc("total_count")).limit(5)

    # Unisce per filtrare solo i modelli dei top brand
    top_brand_models = brand_model_counts.join(top_brands, "make", "inner")

    # Finestra per calcolare la classifica dei modelli all'interno di ogni brand
    window_spec = Window.partitionBy("make").orderBy(desc("count"))
    top_brand_models = top_brand_models.withColumn("rank", row_number().over(window_spec))

    # Filtra per i top 5 modelli di ogni brand
    result = top_brand_models.filter(col("rank") <= 5).select(
        "make", "model", "count"
    ).orderBy("make", "rank")

    return result


def top_models_per_year(df: DataFrame):
    """
    Restituisce i top 5 modelli e il numero di foto scattate per ogni anno, includendo il marchio.
    :param df: DataFrame PySpark contenente i dati delle foto.
    :return: DataFrame con top 5 modelli, numero di foto e marchio per anno.
    """
    df = df.withColumn("make", col("camera_info.make"))
    df = df.withColumn("model", col("camera_info.model"))


    filtered_df = df.filter(
        (col("make") != "Marca fotocamera non disponibile") &
        (col("model") != "Modello fotocamera non disponibile")
    )

    # Filtra per camera_info e data disponibile
    filtered_df = df.filter(
        col("model").isNotNull() & col("make").isNotNull() & col("datePosted").isNotNull()
    )

    # Estrai l'anno da datePosted
    filtered_df = filtered_df.withColumn("year", year(to_timestamp("datePosted")))

    # Conta il numero di foto per modello, marchio e anno
    model_year_counts = filtered_df.groupBy("year", "make", "model").count()

    # Finestra per calcolare la classifica dei modelli all'interno di ogni anno
    window_spec = Window.partitionBy("year").orderBy(desc("count"))
    model_year_counts = model_year_counts.withColumn("rank", row_number().over(window_spec))

    # Filtra per i top 5 modelli di ogni anno
    result = model_year_counts.filter(col("rank") <= 5).select(
        "year", "make", "model", "count"
    ).orderBy("year", "rank")

    return result
