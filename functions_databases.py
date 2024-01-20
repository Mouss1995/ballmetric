"""Function script in order to interact with database"""

from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2 import sql
from pymongo import MongoClient


def open_connexion(host="localhost", port=27017, db_name="ballmetric"):
    """Create connexion"""
    client = MongoClient(host, port)
    db = client[db_name]
    return client, db


def insert_match(collection_name, match, db):
    """Insert match to collection"""
    collection = db[collection_name]
    collection.insert_one(match)


def get_matchs_collection(collection_name, db):
    """Get matchs to dataframe"""
    collection = db[collection_name]
    cursor = collection.find()
    documents_list = list(cursor)
    df = pd.DataFrame(documents_list)

    return df


def open_connection_postgresql():
    # Paramètres de connexion à la base de données
    db_params = {
        "dbname": "ballmetric",
        "user": "alancuzon",
        "password": "bilbao1995",
        # 'host': 'votre_host',
        # 'port': 'votre_port'
    }

    # Connexion à la base de données
    connection = psycopg2.connect(**db_params)

    return connection


def insert_postgresql(connection, match):
    try:
        # Création d'un curseur
        with connection.cursor() as cursor:
            req = """
                INSERT INTO match_info
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            date_formatted = match.get("Date", None)
            if date_formatted is not None:
                date_formatted = datetime.strptime(date_formatted, "%d-%m-%Y").strftime(
                    "%Y-%m-%d"
                )

            # Utiliser la méthode de formatage pour insérer les valeurs
            valeurs = (
                match.get("Teams", {}).get("Home", None),
                match.get("Teams", {}).get("Away", None),
                match.get("Season", None),
                match.get("Goals", {}).get("Home", None),
                match.get("Goals", {}).get("Away", None),
                match.get("Competition", None),
                date_formatted,
                match.get("Time", None),
                match.get("Round", None),
                match.get("Manager", {}).get("Home", None),
                match.get("Manager", {}).get("Away", None),
                match.get("Captain", {}).get("Home", None),
                match.get("Captain", {}).get("Away", None),
                match.get("Attendance", None),
                match.get("Venue", None),
                match.get("Season_History", {}).get("Home", {}).get("Victory", None),
                match.get("Season_History", {}).get("Home", {}).get("Defeat", None),
                match.get("Season_History", {}).get("Home", {}).get("Draw", None),
                match.get("Season_History", {}).get("Away", {}).get("Victory", None),
                match.get("Season_History", {}).get("Away", {}).get("Defeat", None),
                match.get("Season_History", {}).get("Away", {}).get("Draw", None),
                match.get("Officials", {}).get("Referee", None),
                match.get("Officials", {}).get("AR1", None),
                match.get("Officials", {}).get("AR2", None),
                match.get("Officials", {}).get("4th", None),
                match.get("Officials", {}).get("VAR", None),
                match.get("Lineup", {}).get("Home", {}).get("Formation", None),
                match.get("Lineup", {}).get("Away", {}).get("Formation", None),
                match.get("xG", {}).get("Home", None),
                match.get("xG", {}).get("Away", None),
                match.get("Fouls", {}).get("Home", None),
                match.get("Fouls", {}).get("Away", None),
                match.get("Corners", {}).get("Home", None),
                match.get("Corners", {}).get("Away", None),
                match.get("Crosses", {}).get("Home", None),
                match.get("Crosses", {}).get("Away", None),
                match.get("Touches", {}).get("Home", None),
                match.get("Touches", {}).get("Away", None),
                match.get("Tackles", {}).get("Home", None),
                match.get("Tackles", {}).get("Away", None),
                match.get("Interceptions", {}).get("Home", None),
                match.get("Interceptions", {}).get("Away", None),
                match.get("Aerials Won", {}).get("Home", None),
                match.get("Aerials Won", {}).get("Away", None),
                match.get("Clearances", {}).get("Home", None),
                match.get("Clearances", {}).get("Away", None),
                match.get("Offsides", {}).get("Home", None),
                match.get("Offsides", {}).get("Home", None),
                match.get("Goal Kicks", {}).get("Home", None),
                match.get("Goal Kicks", {}).get("Away", None),
                match.get("Throw Ins", {}).get("Home", None),
                match.get("Throw Ins", {}).get("Away", None),
                match.get("Long Balls", {}).get("Home", None),
                match.get("Long Balls", {}).get("Away", None),
                match.get("Possession", {}).get("Home", None),
                match.get("Possession", {}).get("Away", None),
                match.get("Passing Accuracy", {}).get("Home", {}).get("Success", None),
                match.get("Passing Accuracy", {}).get("Home", {}).get("Failed", None),
                match.get("Passing Accuracy", {}).get("Away", {}).get("Success", None),
                match.get("Passing Accuracy", {}).get("Away", {}).get("Failed", None),
                match.get("Shots on Target", {}).get("Home", {}).get("Success", None),
                match.get("Shots on Target", {}).get("Home", {}).get("Failed", None),
                match.get("Shots on Target", {}).get("Away", {}).get("Success", None),
                match.get("Shots on Target", {}).get("Away", {}).get("Failed", None),
                match.get("Saves", {}).get("Home", {}).get("Success", None),
                match.get("Saves", {}).get("Home", {}).get("Failed", None),
                match.get("Saves", {}).get("Away", {}).get("Success", None),
                match.get("Saves", {}).get("Away", {}).get("Failed", None),
            )

            cursor.execute(req, valeurs)

            # Validation des changements
            connection.commit()

    except Exception as e:
        print(f"Erreur: {e}")
        # Annulation des changements en cas d'erreur
        connection.rollback()

# def select_data_postgre(query):
#     try:
#         # Connexion à la base de données
#         connection = psycopg2.connect(**db_params)

#         # Utilisation de pandas pour lire les résultats de la requête directement dans un DataFrame
#         df = pd.read_sql_query(query, connection)

#         return df

#     except Exception as e:
#         print(f"Erreur: {e}")

def close_connection_postgresql(connection):
    # Fermer la connexion
    if connection:
        connection.close()