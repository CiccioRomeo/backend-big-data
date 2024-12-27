import requests 

API_KEY = "3c10f204adbefe00f9dcbcf2a760a3d6"
BASE_URL = "https://api.flickr.com/services/rest/"


def fetch_avatar(user_id):
    params = {
        "method": "flickr.people.getInfo",
        "api_key": API_KEY,
        "user_id": user_id,
        "format": "json",
        "nojsoncallback": 1,
    }
    try:
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            iconfarm = data['person']['iconfarm']
            iconserver = data['person']['iconserver']
            if iconfarm and iconserver:
                return f"https://farm{iconfarm}.staticflickr.com/{iconserver}/buddyicons/{user_id}.jpg"
        return "https://www.flickr.com/images/buddyicon.gif"  # Avatar predefinito
    except Exception as e:
        return None


def construct_photo_url(farm, server, photo_id, secret):
    if farm and server and photo_id and secret:
        return f"https://farm{farm}.staticflickr.com/{server}/{photo_id}_{secret}.jpg"
    return None


def get_comments(photo_id: str):
    """
    Richiama l'endpoint flickr.photos.comments.getList
    per ottenere i commenti di una foto.
    """
    params = {
        "method": "flickr.photos.comments.getList",
        "api_key": API_KEY,
        "photo_id": photo_id,
        "format": "json",
        "nojsoncallback": "1"
    }
    resp = requests.get(BASE_URL, params=params)
    if resp.status_code == 200:
        data = resp.json()
        # data["comments"]["comment"] è la lista di commenti, in genere
        if "comments" in data and "comment" in data["comments"]:
            return data["comments"]["comment"]
        else:
            return []
    else:
        raise Exception(f"Errore API Flickr: {resp.status_code}, {resp.text}")
    




def get_photo_comments_count( photo_id):
    """
    Recupera il numero di commenti per una foto su Flickr utilizzando l'API Flickr.
    
    Parametri:
    photo_id (str): L'ID della foto di cui vuoi contare i commenti
    
    Ritorna:
    int: Il numero di commenti della foto
    """

    params = {
        "method": "flickr.photos.getInfo",
        "api_key": API_KEY,
        "photo_id": photo_id,
        "format": "json",
        "nojsoncallback": 1
    }
    
    try:
        # Effettua la richiesta all'API
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Solleva un'eccezione per risposte non 2xx
        
        # Parsing della risposta JSON
        data = response.json()
        
        # Verifica se la richiesta ha avuto successo
        if data["stat"] == "ok":
            # Estrae il numero di commenti
            comments_count = data["photo"]["comments"]["_content"]
            return int(comments_count)
        else:
            raise Exception(f"Errore API Flickr: {data.get('message', 'Errore sconosciuto')}")
            
    except requests.exceptions.RequestException as e:
        raise Exception(f"Errore nella richiesta HTTP: {str(e)}")
    except (KeyError, ValueError) as e:
        raise Exception(f"Errore nel parsing della risposta: {str(e)}")
    

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
    user_id = "89107298@N00"
    photo_id = "7121887983"
    try:
       #print(fetch_avatar(user_id))
       print(get_photo_comments_count(photo_id))
    except Exception as ex:
        print("Errore:", ex)
