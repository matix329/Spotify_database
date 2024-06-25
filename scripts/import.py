import sqlite3
import json
from confluent_kafka import Consumer, KafkaException, KafkaError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("import_album")

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'album_import_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['albums'])

def create_tables(conn):
    c = conn.cursor()

    c.execute('''
    CREATE TABLE IF NOT EXISTS Artist (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        genre TEXT NOT NULL
    )
    ''')

    c.execute('''
    CREATE TABLE IF NOT EXISTS album (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        album_name TEXT NOT NULL,
        pub_date TEXT NOT NULL,
        artist_id INTEGER NOT NULL,
        FOREIGN KEY(artist_id) REFERENCES Artist(id)
    )
    ''')

    c.execute('''
    CREATE TABLE IF NOT EXISTS track (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        duration INTEGER NOT NULL,
        album_id INTEGER NOT NULL,
        FOREIGN KEY(album_id) REFERENCES album(id)
    )
    ''')
    conn.commit()

def insert_album_data(conn, album_data):
    album_name = album_data["album_name"]
    pub_date = album_data["pub_date"]
    artist_name = album_data["artist_name"]

    c = conn.cursor()

    # Sprawdzenie, czy artysta już istnieje
    c.execute('SELECT id FROM Artist WHERE name = ?', (artist_name,))
    artist = c.fetchone()

    if artist is None:
        # Wstawienie nowego artysty do tabeli "Artist"
        c.execute('INSERT INTO Artist (name, genre) VALUES (?, ?)', (artist_name, ""))
        artist_id = c.lastrowid
    else:
        # Pobranie istniejącego artist_id
        artist_id = artist[0]

    # Wstawienie danych do tabeli "album"
    c.execute('''
    INSERT INTO album (album_name, pub_date, artist_id)
    VALUES (?, ?, ?)
    ''', (album_name, pub_date, artist_id))

    # Pobranie album_id nowo wstawionego albumu
    album_id = c.lastrowid

    # Wstawienie danych do tabeli "track"
    for track in album_data['tracks']:
        track_name = track['title']
        duration = track['duration']
        c.execute('''
        INSERT INTO track (title, duration, album_id)
        VALUES (?, ?, ?)
        ''', (track_name, duration, album_id))

    conn.commit()

def consume_and_import():
    conn = sqlite3.connect('music.db')
    create_tables(conn)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                try:
                    logger.info(f'Received message: {msg.value().decode("utf-8")}')
                    album_data = json.loads(msg.value().decode('utf-8'))
                    insert_album_data(conn, album_data)
                except json.JSONDecodeError as e:
                    logger.error(f'JSON decode error: {e}')
                except KeyError as e:
                    logger.error(f'Key error: {e}')
                except Exception as e:
                    logger.error(f'Error processing message: {e}')

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()

if __name__ == '__main__':
    consume_and_import()
