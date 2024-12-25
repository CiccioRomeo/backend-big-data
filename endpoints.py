from flask import Flask, request, jsonify
from flask_cors import CORS
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, abs, array_contains, count, explode, desc, sum, avg, when, month, year, hour
from pyspark.sql.window import Window
import queries
import json

mappa_mesi = {
            1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
            5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
            9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
        }



def paginate_dataframe_sql(df: DataFrame, page: int, page_size: int) -> DataFrame:
    """
    Paginazione SQL per un DataFrame PySpark.
    :param df: Il DataFrame da paginare
    :param page: Numero della pagina
    :param page_size: Dimensione della pagina
    :return: DataFrame paginato
    """
    start_index = (page - 1) * page_size
    end_index = page * page_size

    # Filtro preliminare: escludi valori nulli
    df = df.filter(col("dateTaken").isNotNull())

    # Calcola il numero totale di righe
    total_rows = df.count()

    # Restituisci un DataFrame vuoto se la pagina supera il limite
    if start_index >= total_rows:
        return df.limit(0)

    window = Window.orderBy(col("dateTaken"))
    return (df.withColumn("row_number", row_number().over(window))
              .filter((col("row_number") > start_index) & (col("row_number") <= end_index)))


def create_app(df: DataFrame) -> Flask:
    app = Flask(__name__)

    CORS(app)

    @app.route("/data", methods=["GET"])
    def get_data():
        limit_str = request.args.get("limit", default="10")
        limit_val = int(limit_str)
        result_df = queries.get_first_n_rows(df, limit_val)
        data_rows = result_df.collect()
        rows_str = [str(row.asDict()) for row in data_rows]
        return "\n".join(rows_str)

    @app.route("/photosByCoordinates", methods=["GET"])
    def photos_by_coordinates():
        # Ottieni il parametro 'limit' dai parametri della richiesta, con un valore predefinito di 10000
        limit = request.args.get("limit", default=10000, type=int)

        # Chiama la funzione di query passando il DataFrame
        result_df = queries.count_photos_by_coordinates(df, limit)

        # Colleziona i dati e trasformali in una lista di valori
        data_rows = result_df.collect()

        results = [list(row.asDict().values()) for row in data_rows]

        # Salva i risultati in un file di testo locale
        output_file = "results.txt"
        with open(output_file, "w") as file:
            for result in results:
                file.write(json.dumps(result) + "\n")

        print(f"Risultati salvati nel file: {output_file}")

        # Restituisci un messaggio di conferma
        return f"Risultati salvati nel file: {output_file}", 200
    

    @app.route("/photosByTag", methods=["GET"])
    def photos_by_tag():
        try:
            tag = request.args.get('tag')
            if not tag:
                return jsonify({"error": "Missing 'tag' parameter"}), 400

            page = int(request.args.get('page', 1))
            page_size = int(request.args.get('page_size', 100))

            filtered_df = queries.get_photos_by_tag(df, tag)
            paginated_df = paginate_dataframe_sql(filtered_df, page, page_size)

            data_rows = paginated_df.collect()
            return jsonify([row.asDict() for row in data_rows])
        except Exception as e:
            return jsonify({"error": str(e)}), 500

        

    @app.route("/photosByDateRange", methods=["GET"])
    def photos_by_date_range():
        start_date = request.args.get("startDate")
        end_date = request.args.get("endDate")
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 100))

        if not start_date or not end_date:
            return "Error: Missing 'startDate' or 'endDate' parameter.", 400

        filtered_df = queries.get_photos_by_date_range(df, start_date, end_date)
        paginated_df = paginate_dataframe_sql(filtered_df, page, page_size)

        data_rows = paginated_df.collect()
        return jsonify([row.asDict() for row in data_rows])

    @app.route("/photosByDateRangeN", methods=["GET"])
    def photos_by_date_range_NOPAGE():
        start_date = request.args.get("startDate")
        end_date = request.args.get("endDate")
        if not start_date or not end_date:
            return "Error: Missing 'startDate' or 'endDate' parameter.", 400

        filtered_df = queries.get_photos_by_date_range(df, start_date, end_date)
        data_rows = filtered_df.collect()
        return jsonify([row.asDict() for row in data_rows])

    @app.route("/photosByLocation", methods=["GET"])
    def photos_by_location():
        try:
            lat = float(request.args.get("lat"))
            lon = float(request.args.get("lon"))
            radius = float(request.args.get("radius"))
            page = int(request.args.get('page', 1))
            page_size = int(request.args.get('page_size', 100))
        except (TypeError, ValueError):
            return "Error: Invalid 'lat', 'lon', or 'radius' parameter.", 400

        filtered_df = queries.get_photos_by_location(df, lat, lon, radius)
        paginated_df = paginate_dataframe_sql(filtered_df, page, page_size)

        data_rows = paginated_df.collect()
        return jsonify([row.asDict() for row in data_rows])

    @app.route("/photosByDescriptionKeyword", methods=["GET"])
    def photos_by_description_keyword():
        keyword = request.args.get("keyword")
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 100))

        if not keyword:
            return "Error: Missing 'keyword' parameter.", 400

        filtered_df = queries.get_photos_by_description_keyword(df, keyword)
        paginated_df = paginate_dataframe_sql(filtered_df, page, page_size)

        data_rows = paginated_df.collect()
        return jsonify([row.asDict() for row in data_rows])



    #photo trends

    @app.route("/photoCountByMonth", methods=["GET"])
    def photo_count_by_month():
        # Mappa dei mesi


        # Recupera le righe dal DataFrame
        result_df = queries.photo_count_by_month(df)
        rows = result_df.collect()

        # Trasforma le righe mappando i numeri dei mesi ai nomi
        rows_dict = [
            {
                "count": row["count"],
                "month": mappa_mesi.get(row["month"])

            }
            for row in rows
        ]

        # Restituisce i dati come JSON
        return jsonify(rows_dict)

    @app.route("/photoCountByYear", methods=["GET"])
    def photo_count_by_year():
        result_df = queries.photo_count_by_year(df)
        data_rows = result_df.collect()
        rows_dict = [row.asDict() for row in data_rows]
        return jsonify(rows_dict)

    @app.route("/photoCountPerMonthByYear", methods=["GET"])
    def photo_posted_per_month_by_year():
        try:
            year_param = request.args.get("year")
            if not year_param:
                return jsonify({"error": "Missing 'year' parameter"}), 400

            # Usa una variabile intermedia per evitare conflitti con il nome 'year'
            try:
                input_year = int(year_param)
            except ValueError:
                return jsonify({"error": "'year' must be an integer"}), 400

            result_df = queries.photo_posted_per_month_by_year(df, input_year)

            if result_df is None:
                return jsonify({"error": "No data found for the given year"}), 404

            rows = result_df.collect()

            rows_dict = [
                {
                    "count": row["count"],
                    "month": mappa_mesi.get(row["month"])

                }
                for row in rows
            ]

            # Restituisce i dati come JSON
            return jsonify(rows_dict)

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/averageTimeToPost", methods=["GET"])
    def average_time_to_post():
        result_df = queries.average_time_to_post(df)
        return jsonify(result_df.collect()[0].asDict())

    @app.route("/photoPublicPrivateDistribution", methods=["GET"])
    def photo_public_private_distribution():
        result_df = queries.photo_public_private_distribution(df)
        data_rows = result_df.collect()
        rows_str = [str(row.asDict()) for row in data_rows]
        return "\n".join(rows_str)

    @app.route("/averageCommentsAndViews", methods=["GET"])
    def average_comments_and_views():
        result_df = queries.average_comments_and_views(df)
        return jsonify(result_df.collect()[0].asDict())

    @app.route("/photoCountWithPeople", methods=["GET"])
    def photo_count_with_people():
        result_df = queries.photo_count_with_people(df)
        data_rows = result_df.collect()
        rows_str = [str(row.asDict()) for row in data_rows]
        return "\n".join(rows_str)

    @app.route("/photoCountByTagCount", methods=["GET"])
    def photo_count_by_tag_count():
        result_df = queries.photo_count_by_tag_count(df)
        data_rows = result_df.collect()
        rows_str = [str(row.asDict()) for row in data_rows]
        return "\n".join(rows_str)

    @app.route("/proUsersVsNonPro", methods=["GET"])
    def pro_users_vs_non_pro():
        result_df = queries.pro_users_vs_non_pro(df)
        data_rows = result_df.collect()
        rows_str = [str(row.asDict()) for row in data_rows]
        return "\n".join(rows_str)

    @app.route("/averagePhotosPerUser", methods=["GET"])
    def average_photos_per_user():
        result_df = queries.average_photos_per_user(df)
        return jsonify(result_df.collect()[0].asDict())

    @app.route("/accuracyDistribution", methods=["GET"])
    def accuracy_distribution():
        result_df = queries.accuracy_distribution(df)
        data_rows = result_df.collect()
        rows_str = [str(row.asDict()) for row in data_rows]
        return "\n".join(rows_str)

    return app
