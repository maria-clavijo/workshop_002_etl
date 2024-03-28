import configparser
import mysql.connector
import pandas as pd


config = configparser.ConfigParser()
config.read('db_config.ini')

host = config['mysql']['host']
user = config['mysql']['user']
password = config['mysql']['password']


# Connect to the MySQL database
connection = mysql.connector.connect(
    host=host,
    user=user,
    password=password
)

print("¡Connected to the database!")
cursor = connection.cursor()


# Create the database in MySQL
cursor.execute("CREATE DATABASE IF NOT EXISTS music_db")
cursor.execute("USE music_db")

create_table_query = """
    CREATE TABLE IF NOT EXISTS grammys (
        grammy_id INT AUTO_INCREMENT PRIMARY KEY,
        year INT,
        title VARCHAR(255),
        published_at DATE,
        updated_at DATE,
        category VARCHAR(255),
        nominee VARCHAR(255),
        artist VARCHAR(255),
        workers VARCHAR(725),
        img VARCHAR(725),
        winner ENUM('True', 'False')
    )
"""
cursor.execute(create_table_query)


# Read the CSV file
df = pd.read_csv("../Data/the_grammy_awards.csv", delimiter=",")

df = df.where(pd.notna(df), None)


# Insertion query
query_insert = "INSERT INTO grammys ({}) VALUES ({})".format(
    ', '.join(df.columns),
    ', '.join(['%s'] * len(df.columns))
)

values_row = [tuple(row) for row in df.values]

try:
    # Insert the data into the table
    cursor.executemany(query_insert, values_row)

    print("¡Data entered correctly into the database!")

    connection.commit()
    connection.close()

except Exception as e:
    print("Error! Could not enter data into database:", e)
