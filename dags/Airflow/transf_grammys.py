import db_connection
import json
import logging
import pandas as pd
import re


def extract_db():
    try:
        logging.info("Extracting data from the MySQL database...")
        data_grammys = db_connection.use_db()
        logging.info("Successfully extracted data.")
        return data_grammys.to_json(orient='records')
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        return None


def convert_to_datetime(df):
    try:
        logging.info("Converting columns 'published_at' y 'updated_at' to datetime data type...")
        df['published_at'] = pd.to_datetime(df['published_at'])
        df['updated_at'] = pd.to_datetime(df['updated_at'])
        logging.info("Conversion successfully completed.")
    except Exception as e:
        logging.error(f"Error when converting columns to type datetime: {e}")
    return df


def replace_nulls(df):
    try:
        logging.info("Replacing nulls from the DataFrame...")

        def process_row(row):
            workers_str = str(row['workers'])
            match = re.search(r'\((.*?)\)', workers_str)
            if match:
                return match.group(1).strip()  

            if pd.notnull(row['artist']):
                return row['artist']

            # If 'nominee' has a non-zero value and matches 'artist', we return it
            if pd.notnull(row['nominee']) and row['nominee'] == row['artist']:
                return row['nominee']

            if pd.notnull(row['nominee']):
                return row['nominee']

            return None  

        df['artist'] = df.apply(process_row, axis=1)
        logging.info("Process completed correctly.")
        return df
    except Exception as e:
        logging.error(f"Error in replacing the nulls: {e}")
        return df  


def remove_null_rows(df):
    logging.info("Deleting rows with null values...")
    try:
        df.dropna(subset=['artist', 'nominee'])
        return df
    except Exception as e:
        print(f"Error in deleting the rows: {e}")
        return df 


def drop_columns(df):
    logging.info("Removing columns...")
    try:
        df.drop('img', axis=1, inplace=True)
        df.drop('workers', axis=1, inplace=True)
        df.drop('grammy_id', axis=1, inplace=True)
        return df
    
    except Exception as e:
        print(f"Error in removing columns: {e}")
        return df  


def rename_column(df):
    df.rename(columns={'winner': 'was_nominated'}, inplace=True)
    return df


def normalize_names(df):
    logging.info("normalizing the name of the columns...")
    try:
        df['artist'] = df['artist'].str.title()
        df['nominee'] = df['nominee'].str.title()
        return df
    except Exception as e:
        print(f"Error in normalize column names(): {e}")
        return df  


def transformation_db(df_grammys):
    try:
        df_grammys = convert_to_datetime(df_grammys)
        df_grammys = replace_nulls(df_grammys)
        df_grammys = remove_null_rows(df_grammys)
        df_grammys = drop_columns(df_grammys)
        df_grammys = rename_column(df_grammys)
        df_grammys = normalize_names(df_grammys)

        logging.info("All data transformations completed successfully.")
        return df_grammys.to_json(orient='records')
    except Exception as e:
        logging.error(f"Error in data transformation: {e}")
        return None
