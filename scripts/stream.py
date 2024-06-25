### stream.py

import requests
import base64
import json
import logging
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stream_album")

def get_access_token(client_id, client_secret):
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + base64.b64encode((client_id + ":" + client_secret).encode()).decode()
    }
    data = {
        "grant_type": "client_credentials"
    }
    response = requests.post(url, headers=headers, data=data)
    return response.json().get("access_token")

def get_artist_albums(access_token, artist_id):
    url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
    headers = {
        "Authorization": "Bearer " + access_token
    }
    albums = []
    params = {"limit": 50}
    while url:
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        albums.extend(data["items"])
        url = data.get("next")
    return albums

def get_album_data(access_token, album_id):
    url = f"https://api.spotify.com/v1/albums/{album_id}"
    headers = {
        "Authorization": "Bearer " + access_token
    }
    response = requests.get(url, headers=headers)
    album_data = response.json()

    # Przetworzenie danych albumu
    processed_data = {
        "album_name": album_data["name"],
        "pub_date": album_data["release_date"],
        "artist_name": album_data["artists"][0]["name"],
        "tracks": [
            {
                "title": track["name"],
                "duration": track["duration_ms"]
            }
            for track in album_data["tracks"]["items"]
        ]
    }
    return processed_data

def get_artist_id(access_token, artist_name):
    url = "https://api.spotify.com/v1/search"
    headers = {
        "Authorization": "Bearer " + access_token
    }
    params = {
        "q": artist_name,
        "type": "artist",
        "limit": 1
    }
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    return data["artists"]["items"][0]["id"] if data["artists"]["items"] else None

producer_conf = {
    'bootstrap.servers': 'localhost:29092'
}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def send_to_kafka(topic, key, value):
    producer.produce(topic, key=key, value=json.dumps(value), callback=delivery_report)
    producer.flush()

# Twój kod API
access_token = get_access_token("twój_kod_API", "twój_kod_API")
logger.info("Access token obtained")

artist_names = ["Pantera", "Doyle", "Misfits", "Crowbar", "Meshuggah", "Down"]

for artist_name in artist_names:
    artist_id = get_artist_id(access_token, artist_name)
    if not artist_id:
        logger.error(f"Artist not found: {artist_name}")
        continue

    albums = get_artist_albums(access_token, artist_id)
    logger.info(f"Found {len(albums)} albums for artist {artist_name}")

    for album in albums:
        album_id = album["id"]
        album_data = get_album_data(access_token, album_id)
        logger.info(f"Album data: {album_data}")

        send_to_kafka("albums", key=album_id, value=album_data)
        logger.info("Album data sent to Kafka")
