"""Function script in order to interact with database"""

import pandas as pd
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
