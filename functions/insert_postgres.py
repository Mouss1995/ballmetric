# pylint: disable=too-many-locals

"""Function script in order to interact with database"""

import os
from datetime import datetime

import psycopg2
import yaml
from colorama import Fore, Style
from psycopg2 import IntegrityError


def create_config_file() -> None:
    """
    Create config file for
    db connection
    Return : None
    """
    database = input("Database: ")
    host = input("Host: ")
    username = input("User: ")
    password = input("Password: ")
    port = input("Port : ")

    config = {
        "database": {
            "dbname": database,
            "user": username,
            "password": password,
            "host": host,
            "port": port,
        }
    }

    with open("config/db_config.yaml", "w", encoding="utf-8") as file:
        yaml.dump(config, file)


def load_config_db() -> None:
    """
    Load config file for
    db connection
    Return : None
    """
    if not os.path.exists("config/db_config.yaml"):
        create_config_file()
    with open("config/db_config.yaml", "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    return config


def test_connection_db() -> None:
    """
    Load config file for
    db connection
    Return : None
    """
    config = load_config_db()
    dbname = config["database"]["dbname"]
    user = config["database"]["user"]
    password = config["database"]["password"]
    host = config["database"]["host"]
    port = config["database"]["port"]
    try:
        conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        print(
            "\t"
            + Fore.GREEN
            + "Connection successfully established to PostgreSQL database"
            + Style.RESET_ALL
        )
        conn.close()
    except psycopg2.OperationalError as e:
        print("\t" + Fore.RED + f"Erreur de connexion : {e}" + Style.RESET_ALL)


def open_connection_postgresql():
    """Open connexion Postgresql"""
    config = load_config_db()

    conn = psycopg2.connect(
        dbname=config["database"]["dbname"],
        user=config["database"]["user"],
        password=config["database"]["password"],
        host=config["database"]["host"],
        port=config["database"]["port"],
    )

    return conn


def insert_data(match: dict, cur, conn) -> None:
    """
    Insert data to postgresql
    Args:
        match (dict): match data
        cur: cursor from open_connection_postgresql
            function
        conn: connection from open_connection_postgresql
            function
    Return:
        None
    """
    try:
        #########################
        ### INSERT MATCH INFOS ###
        #########################
        match_keys = [
            "team_h",
            "team_a",
            "date_match",
            "season",
            "competition",
            "time_match",
            "matchweek",
            "round",
            "notes",
            "manager_h",
            "manager_a",
            "captain_h",
            "captain_a",
            "attendance",
            "venue",
            "formation_home",
            "formation_away",
            "referee",
            "ar1",
            "ar2",
            "fourth",
            "var",
            "penalties_h",
            "penalties_a",
        ]
        dict_match = dict((k, match[k]) for k in match_keys if k in match)
        dict_match["date_match"] = datetime.strptime(
            match["date_match"], "%d-%m-%Y"
        ).strftime("%Y-%m-%d")

        insert_query_players = f"""INSERT INTO matchs
            ({', '.join(dict_match.keys())}) VALUES ({', '.join(['%s'] * len(dict_match))});"""

        cur.execute(insert_query_players, list(dict_match.values()))

        for type_team in ["home", "away"]:
            ############################
            ### INSERT PLAYERS STATS ###
            ############################
            for dict_player in match["player_stats"][type_team]:

                lineup_players = "_".join(["lineup", type_team])

                if lineup_players in match:
                    holder = (
                        1
                        if dict_player["player"]
                        in match[lineup_players].split("Bench")[0]
                        else 0
                    )
                else:
                    holder = None

                dict_player_match = {
                    "team_h": match["team_h"],
                    "team_a": match["team_a"],
                    "date_match": datetime.strptime(
                        match["date_match"], "%d-%m-%Y"
                    ).strftime("%Y-%m-%d"),
                    "player_team": (
                        match["team_h"] if type_team == "home" else match["team_a"]
                    ),
                    "holder": holder,
                }

                dict_player.update(dict_player_match)

                insert_query_players = (
                    f"INSERT INTO player_stats ({', '.join(dict_player.keys())})"
                    f"VALUES ({', '.join(['%s'] * len(dict_player))});"
                )

                cur.execute(insert_query_players, list(dict_player.values()))

            ################################
            ### INSERT GOALKEEPERS STATS ###
            ################################
            if "goalkeeper_stats" in match:
                for dict_player_goalkeeper in match["goalkeeper_stats"][type_team]:

                    dict_player_match = {
                        "team_h": match["team_h"],
                        "team_a": match["team_a"],
                        "date_match": datetime.strptime(
                            match["date_match"], "%d-%m-%Y"
                        ).strftime("%Y-%m-%d"),
                        "player_team": (
                            match["team_h"] if type_team == "home" else match["team_a"]
                        ),
                    }

                    dict_player_goalkeeper.update(dict_player_match)

                    insert_query_goalkeeper = (
                        f"INSERT INTO goalkeeper_stats ({', '.join(dict_player_goalkeeper.keys())})"
                        f"VALUES ({', '.join(['%s'] * len(dict_player_goalkeeper))});"
                    )

                    cur.execute(
                        insert_query_goalkeeper, list(dict_player_goalkeeper.values())
                    )

        ##########################
        ### INSERT SHOTS STATS ###
        ##########################
        if "shots" in match:
            for dict_shot in match["shots"]:

                if all(value is None for value in dict_shot.values()):
                    continue

                dict_player_match = {
                    "team_h": match["team_h"],
                    "team_a": match["team_a"],
                    "date_match": datetime.strptime(
                        match["date_match"], "%d-%m-%Y"
                    ).strftime("%Y-%m-%d"),
                }

                dict_shot.update(dict_player_match)

                insert_query_shots = (
                    f"INSERT INTO shots ({', '.join(dict_shot.keys())})"
                    f"VALUES ({', '.join(['%s'] * len(dict_shot))});"
                )
                cur.execute(insert_query_shots, list(dict_shot.values()))

        ########################
        ### INSERT URL MATCH ###
        ########################
        insert_query_url = "INSERT INTO match_urls (url) VALUES (%s);"
        values = (match["url"],)
        cur.execute(insert_query_url, values)
        conn.commit()

    except IntegrityError as e:
        conn.rollback()
        print(f"\tErreur de contrainte de clé unique: {e}")
        insert_query_url = "INSERT INTO match_urls (url) VALUES (%s);"
        values = (match["url"],)
        cur.execute(insert_query_url, values)
        conn.commit()
        raise
    except Exception as e:
        conn.rollback()
        print("Erreur inattendue:", e)
        raise
