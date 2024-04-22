import json
import logging
import pandas as pd


def extract_csv():
    logging.info("Extract Data...")
    df_spotify = pd.read_csv("./Data/spotify_dataset.csv")
    logging.info("Data Extract Successfully.")
    return df_spotify.to_json(orient='records')


def drop_row(df):
    try:
        logging.info(f"Eliminating the row...")
        index_to_drop = 65900 
        df.drop(index_to_drop)
        logging.info("Correctly deleted row.")
        return df
    except Exception as e:
        logging.error(f"Error while deleting the row: {e}")
        return df


def remove_duplicates(df):
    try:
        logging.info(f"Deleting duplicate records based on column...")
        df.drop_duplicates(subset=['track_id'], keep='first')
        logging.info("Duplicate records removed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error while deleting duplicate records: {e}")
        return df


def convert_to_minutes(df):
    try:
        logging.info(f"Converting the duration from milliseconds to minutes...")
        df['duration_ms'] = df['duration_ms'] / 60000
        df.rename(columns={'duration_ms': 'duration_minutes'}, inplace=True)
        logging.info("Successful conversion and renamed column.")
        return df
    except Exception as e:
        logging.error(f"Error converting duration to minutes: {e}")
        return df


genre_mapping = {
    'rock': ['alt-rock', 'hard-rock', 'punk-rock', 'punk', 'grunge', 'rock-n-roll', 'goth', 'psych-rock', 'rock', 'alternative'],
    'Metal':['metal', 'heavy-metal', 'black-metal', 'death-metal', 'metalcore'],
    'pop': ['pop', 'power-pop', 'dance'],
    'hip-hop/r&b': ['hip-hop', 'r-n-b', 'trip-hop'],
    'electronic': ['electronic', 'electro', 'edm', 'techno', 'trance', 'club', 'detroit-techno', 'dub', 'hardstyle', 'idm'],
    'house':['deep-house', 'house', 'progressive-house', 'chicago-house'],
    'jazz': ['jazz', 'groove'],
    'classical': ['classical', 'opera'],
    'j-pop/more':['j-dance', 'j-idol', 'j-pop', 'j-rock'],
    'latin':['latin', 'salsa', 'samba', 'sertanejo', 'forro', 'mpb', 'pagode', 'reggaeton', 'tango', 'latino', 'spanish'],
    'folk': ['folk', 'country', 'honky-tonk', 'bluegrass'],
    'other': ['ambient', 'disco', 'funk', 'blues', 'new-age', 'gospel', 'disney', 'pop-film', 'brazil', 'anime',
              'k-pop', 'cantopop', 'children', 'comedy', 'dancehall', 'drum-and-bass', 'dubstep', 'emo', 'french', 'breakbeat', 'british',
              'german', 'grindcore', 'guitar', 'happy', 'indian', 'indie-pop', 'indie', 'industrial', 'iranian,', 'kids', 'malay', 'mandopop', 
              'minimal-techno', 'party', 'piano', 'reggae', 'rockabilly', 'romance', 'sad', 'show-tunes', 'hardcore', 'chill', 'afrobeat',
              'singer-songwriter', 'ska', 'sleep', 'soul', 'study', 'swedish', 'synth-pop', 'turkish', 'world-music', 'garage', 'acoustic']
}

def group_genres(df):
    try:
        logging.info("Grouping genres according to the mapping dictionary...")
        def map_genre(genre):
            for key, value in genre_mapping.items():
                if genre in value:
                    return key
            return 'other'

        df['grouped_genre'] = df['track_genre'].apply(map_genre)
        logging.info("Gender grouping completed correctly.")
        return df
    except Exception as e:
        logging.error(f"Error when grouping the genres: {e}")
        return df


def drop_columns_csv(df):
    logging.info("Removing columns...")
    try:
        df.drop('Unnamed: 0', axis=1, inplace=True)
        df.drop('mode', axis=1, inplace=True)
        df.drop('key', axis=1, inplace=True)
        return df
    
    except Exception as e:
        print(f"Error in removing columns: {e}")
        return df  


def normalize_names_csv(df):
    logging.info("normalizing the name of the columns...")
    try:
        df['artists'] = df['artists'].str.title()
        return df
    except Exception as e:
        print(f"Error in normalize column names(): {e}")
        return df  

  
def transformation_csv(df_spotify):
    try:
        # Perform data transformations step by step
        df_spotify = drop_row(df_spotify)
        df_spotify = remove_duplicates(df_spotify)
        df_spotify = convert_to_minutes(df_spotify)
        df_spotify = group_genres(df_spotify)
        df_spotify = drop_columns_csv(df_spotify)
        df_spotify = normalize_names_csv(df_spotify)

        logging.info("All data transformations completed successfully.")
        return df_spotify.to_json(orient='records')
    except Exception as e:
        logging.error(f"Error in data transformation: {e}")
        return None
