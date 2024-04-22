import configparser
import logging
import mysql.connector
import pandas as pd


logging.basicConfig(level=logging.INFO)

def create_connection():
    config = configparser.ConfigParser()
    config.read('../DB_Scripts/db_config.ini')
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']
    database = config['mysql']['database']
    
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            print("Successful Connection")
            return connection
    except mysql.connector.Error as e:
        print("Connection Error:", e)
        return None

def use_db():
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        db_table = 'SELECT * FROM grammys'
        cursor.execute(db_table)
        rows = cursor.fetchall()
        columns = cursor.column_names
        df = pd.DataFrame(rows, columns=columns)
        connection.close()
        return df.to_json(orient='records')



def create_table(cursor):
    cursor.execute("USE music_db")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS music_awards (
        year INT NOT NULL,
        title VARCHAR(255),
        published_at DATETIME,
        updated_at DATETIME,
        category VARCHAR(255),
        nominee VARCHAR(255),
        artist VARCHAR(255),
        was_nominated BOOLEAN,
        track_id VARCHAR(255),
        artists VARCHAR(255),
        album_name VARCHAR(255),
        track_name VARCHAR(255),
        popularity INT,
        duration_minutes INT,
        explicit BOOLEAN,
        danceability FLOAT,
        energy FLOAT,
        loudness FLOAT,
        speechines FLOAT,
        acousticness FLOAT,
        instrumentalness FLOAT,
        liveness FLOAT,
        valence FLOAT,
        tempo FLOAT,
        time_signature FLOAT,
        track_genre VARCHAR(255)
        grouped_genre VARCHAR(255),
        decade INT)""")
    print("Table created successfully")

def insert_data(json_data):
    connection = create_connection()
    if connection is not None:
        df = pd.read_json(json_data)
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO awards (year, title, published_at, updated_at, category, nominee, artist, was_nominated, track_id, artists, album_name 
        track_name, popularity, duration_minutes, explicit, danceability, energy, loudness, speechines, acousticness, instrumentalness, liveness, 
        valence, tempo, time_signature, track_genre, grouped_genre, decade)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            data_tuples = [tuple(x) for x in df.to_numpy()]
            cursor.executemany(insert_query, data_tuples)
            connection.commit()
            print("Data inserted successfully")
        except mysql.connector.Error as e:
            print("Failed to insert data: %s", e)
            connection.rollback()
        finally:
            cursor.close()
            connection.close()