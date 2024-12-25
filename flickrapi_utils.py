# flickrapi_utils.py

import requests
import json

API_KEY = "3c10f204adbefe00f9dcbcf2a760a3d6"

def get_comments(photo_id: str):
    """
    Richiama l'endpoint flickr.photos.comments.getList
    per ottenere i commenti di una foto.
    """
    url = "https://api.flickr.com/services/rest/"
    params = {
        "method": "flickr.photos.comments.getList",
        "api_key": API_KEY,
        "photo_id": photo_id,
        "format": "json",
        "nojsoncallback": "1"
    }
    resp = requests.get(url, params=params)
    if resp.status_code == 200:
        data = resp.json()
        # data["comments"]["comment"] è la lista di commenti, in genere
        if "comments" in data and "comment" in data["comments"]:
            return data["comments"]["comment"]
        else:
            return []
    else:
        raise Exception(f"Errore API Flickr: {resp.status_code}, {resp.text}")

def get_place_info(place_id: str):
    """
    Richiama l'endpoint flickr.places.getInfo
    per ottenere info su un place.
    """
    url = "https://api.flickr.com/services/rest/"
    params = {
        "method": "flickr.places.getInfo",
        "api_key": API_KEY,
        "place_id": place_id,
        "format": "json",
        "nojsoncallback": "1"
    }
    resp = requests.get(url, params=params)
    if resp.status_code == 200:
        data = resp.json()
        if "place" in data:
            return data["place"]
        else:
            return {}
    else:
        raise Exception(f"Errore API Flickr: {resp.status_code}, {resp.text}")

# Esempio di utilizzo "standalone"
if __name__ == "__main__":
    test_photo_id = "2573762303"
    test_place_id = "62ufGyBUUb26u2g"

    try:
        comments = get_comments(test_photo_id)
        print("Esempio di commenti:", comments)

        place_info = get_place_info(test_place_id)
        print("Esempio di place info:", place_info)

    except Exception as ex:
        print("Errore:", ex)
