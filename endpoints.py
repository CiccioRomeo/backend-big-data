from flask import Flask, request, jsonify
from flask_cors import CORS
import queries
import mlqueries
from flask_compress import Compress

import math
mappa_mesi = {
        1: "Gennaio", 2: "Febbraio", 3: "Marzo", 4: "Aprile",
        5: "Maggio", 6: "Giugno", 7: "Luglio", 8: "Agosto",
        9: "Settembre", 10: "Ottobre", 11: "Novembre", 12: "Dicembre"
    }


def create_app(df):
    app = Flask(__name__)
    CORS(app)
    Compress(app)


    @app.route("/top5Owners", methods=["GET"])
    def top_owners():
        result_df = queries.calculate_top_owners_v2(df)
        return jsonify([row.asDict() for row in result_df.collect()])

    @app.route("/getFirstRows", methods=["GET"])
    def get_first_rows():
        limit = int(request.args.get("limit", 10))
        result_df = queries.get_first_n_rows(df, limit)
        return jsonify([row.asDict() for row in result_df.collect()])

    @app.route("/photosByCoordinates", methods=["GET"])
    def photos_by_coordinates():
        result_df = queries.count_photos_by_coordinates(df)
        # Utilizziamo delle tuple per risparmiare spazio
        data = [[row["latitude"], row["longitude"], row["photoCount"]] for row in result_df.collect()]
        return jsonify(data)


    @app.route("/photosByTag", methods=["GET"])
    def photos_by_tag():
        tag = request.args.get("tag")
        if not tag:
            return jsonify({"error": "Missing 'tag' parameter"}), 400
        page = int(request.args.get("page", 1))
        page_size = int(request.args.get("page_size", 100))
        filtered_df = queries.get_photos_by_tag(df, tag)
        paginated_df = queries.paginate_dataframe_sql(filtered_df, page, page_size)
        return jsonify([row.asDict() for row in paginated_df.collect()])
    


    @app.route("/photosByDateRange", methods=["GET"])
    def photos_by_date_range():
        start_date = request.args.get("startDate")
        end_date = request.args.get("endDate")
        if not start_date or not end_date:
            return jsonify({"error": "Missing 'startDate' or 'endDate' parameter"}), 400
        result_df = queries.get_photos_by_date_range(df, start_date, end_date)
        return jsonify([row.asDict() for row in result_df.collect()])

    @app.route("/photoCountByMonth", methods=["GET"])
    def photo_count_by_month():
        result_df = queries.photo_count_by_month(df)
        return jsonify([{ "month": mappa_mesi.get(row["month"]), "count": row["count"] } for row in result_df.collect()])

    @app.route("/photoCountByYear", methods=["GET"])
    def photo_count_by_year():
        result_df = queries.photo_count_by_year(df)
        return jsonify([{ "year": row["year"], "count": row["count"] } for row in result_df.collect()])

    @app.route("/photoPostedPerMonthByYear", methods=["GET"])
    def photo_posted_per_month_by_year():
        input_year = int(request.args.get("year"))
        result_df = queries.photo_posted_per_month_by_year(df, input_year)

        query_result = {row["month"]: row["count"] for row in result_df.collect()}

        complete_result = [
            {"month": mappa_mesi[month], "count": query_result.get(month, 0)}
            for month in range(1, 13)
        ]

        return jsonify(complete_result)


    @app.route("/averageTimeToPost", methods=["GET"])
    def average_time_to_post():
        result_df = queries.average_time_to_post(df)
        return jsonify(result_df.collect()[0].asDict())

    @app.route("/topTags", methods=["GET"])
    def top_tags():
        page = int(request.args.get("page", 1))
        page_size = int(request.args.get("page_size", 100))
        result_df = queries.get_top_tags(df)
        paginated_df = queries.paginate_dataframe_sql(result_df, page, page_size)
        return jsonify([row.asDict() for row in paginated_df.collect()])

    @app.route("/mostViewedPhotos", methods=["GET"])
    def most_viewed_photos():
        n = int(request.args.get("limit", 10))
        result_df = queries.most_viewed_photos(df, n)
        return jsonify([row.asDict() for row in result_df.collect()])


    @app.route("/runKMeans", methods=["GET"])
    def run_kmeans():
        try:
            k = int(request.args.get("k", 10))
            if not k:
                return jsonify({"error": "Parameter 'k' is required"}), 400

            if not isinstance(k, int) or k <= 0:
                return jsonify({"error": "Parameter 'k' must be a positive integer"}), 400

            # Esegui il clustering utilizzando il metodo definito
            result_df = mlqueries.run_kmeans_clustering(df, k)

            # Restituisci i centroidi come risposta
            return jsonify([row.asDict() for row in result_df.collect()])
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        



    @app.route("/tagAssociationRules", methods=["GET"])
    def run_association_rules_algorithms():
        try:
            min_support = float(request.args.get("minSupport", 0.2))
            min_confidence = float(request.args.get("minConfidence", 0.6))
            
            if (not isinstance(min_support, float) or min_support <= 0) or (not isinstance(min_confidence, float) or min_confidence <= 0):
                return jsonify({"error": "Parameters must be positive floats"}), 400

            # Esegui il metodo per calcolare le regole di associazione
            result_df = mlqueries.calculate_association_rules(df, min_support, min_confidence)

            # Restituisci i primi 10 risultati come risposta
            return jsonify([row.asDict() for row in result_df.collect()])
        except Exception as e:
            return jsonify({"error": str(e)}), 500

        


    @app.route('/search_photos', methods=['POST'])
    def search_photos_endpoint():
        try:
            # Recupera i parametri dal corpo della richiesta JSON
            data = request.get_json()  # Corretto per POST con dati JSON
            if data is None:
                return jsonify({"error": "No JSON data provided"}), 400

            keyword = data.get('keyword')
            data_inizio = data.get('dataInizio')
            data_fine = data.get('dataFine')
            tag_list = data.get('tag_list', [])  # Valore predefinito come lista vuota
            page = int(request.args.get("page", 1))
            page_size = int(request.args.get("page_size", 100))
            # Esegui la query
            result_df = queries.search_photos(df, keyword=keyword, dataInizio=data_inizio, dataFine=data_fine, tag_list=tag_list)
            paginated_df = queries.paginate_dataframe_sql(result_df, page, page_size)
            # Converto il risultato in un formato leggibile
            return jsonify([row.asDict() for row in paginated_df.collect()])
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        


    @app.route('/years', methods=['GET'])
    def get_years_list():
        try:
            # Esegui la query
            result_df = queries.get_years(df)
            return jsonify([row.asDict() for row in result_df.collect()])
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/tags', methods=['GET'])
    def get_tag():
        try:
            page = int(request.args.get("page", 1))
            page_size = int(request.args.get("page_size", 100))
            # Esegui la query
            result_df = queries.get_all_tags(df)
            paginated_df = queries.paginate_dataframe_sql(result_df, page, page_size)
            return jsonify([row.asDict() for row in paginated_df.collect()])
        except Exception as e:
            return jsonify({"error": str(e)}), 500



    return app


