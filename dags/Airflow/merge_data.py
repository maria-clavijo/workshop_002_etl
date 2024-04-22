import db_connection
import json
import logging
import pandas as pd


def convert_decade(year):
    return (year // 10) * 10


# Combine datasets (CSV and DB)
def merge(**kwargs):
    ti = kwargs['ti']
    
    def data_normalize(task_id):
        merge_dataset = json.loads(ti.xcom_pull(task_ids=task_id))
        return pd.json_normalize(merge_dataset)
    logging.info("By combining data...")

    df_grammys = data_normalize('transf_grammys')
    df_spotify = data_normalize('transf_spotify')

    merged_dataset = pd.merge(df_grammys, df_spotify, left_on='artist', right_on='artists', how='inner')
    merged_dataset['decade'] = merged_dataset['year'].apply(convert_decade)
    logging.info("Data merging process successfully completed.")
    
    return merged_dataset.to_json(orient='records')



# Load merged to db
def load_to_db(**kwargs):
    ti = kwargs["ti"]
    json_data = ti.xcom_pull(task_ids="merge")
    if json_data:
        data = pd.json_normalize(json_data)
        try:
            db_connection.insert_data(data)
            logging.info("Data has been successfully loaded into the database.")
        except Exception as e:
            logging.error(f"Error loading data: {str(e)}")
    else:
        logging.error("No data available to load.")
