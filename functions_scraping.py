import time
import json
import random
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
import simplejson

from functions_cleaning import (
    clean_attendance_stade_avenue,
    clean_competition,
    clean_general_informations,
    clean_general_statistics,
    clean_general_statistics_2,
    clean_goals,
    clean_lineup_formation,
    clean_manager_captain,
    clean_notes,
    clean_penalties,
    clean_season_history,
    clean_xg,
)


def get_html_content(url: str) -> BeautifulSoup:
    """
    Get the HTML content of the link.
    Args:
        url (string): url of the match
    Return:
        BeautifulSoup: soup element
    """
    response = requests.get(url, timeout=10)
    time.sleep(3)
    html_content = response.content
    soup = BeautifulSoup(html_content, "html.parser")
    return soup


def rename_columns_players_stats(lst_dfs: list) -> list:
    """
    Cleaning columns names from
    dataframes players statistics
    Args:
        lst_dfs: list of dataframes coming from soup element
    Return:
        list: list of new column names
    """
    for i in range(0, len(lst_dfs)):
        new_columns = []
        for col in lst_dfs[i].columns:
            new_col = col[1] if "Unnamed" in col[0] else col[0] + "_" + col[1]
            new_columns.append(new_col)
            new_columns = [
                el_col.replace(" ", "_")
                .replace("-", "_")
                .replace("%", "_percent")
                .replace("/", "_")
                .replace("#", "num")
                .lower()
                for el_col in new_columns
            ]
        lst_dfs[i].columns = new_columns
    return lst_dfs


def get_players_stats(soup: BeautifulSoup, match: dict) -> None:
    """
    Get players statistics
    Args:
        soup (BeautifulSoup): soup element
        match (dict): dict that contains match data
    Return:
        None
    """

    selectors_table = [
        {
            "id": "summary",
            "selector": "table[id*=summary]",
        },
        {
            "id": "passing",
            "selector": "table[id*=passing]",
        },
        {
            "id": "defense",
            "selector": "table[id*=defense]",
        },
        {
            "id": "possession",
            "selector": "table[id*=possession]",
        },
        {
            "id": "miscellaneous",
            "selector": "table[id*=misc]",
        },
        {"id": "goalkeeper", "selector": "table[id*=keeper]"},
        {"id": "shots", "selector": "table[id*=shots_all]"},
    ]

    data_dict = {}

    # Iterate over selector dictionnary
    for selectors in selectors_table:

        # Get tables from selector html
        tables = soup.select(selectors["selector"])

        if tables:
            dataframes = []
            for table in tables:
                df_passing = pd.read_html(str(table))[0]
                dataframes.append(df_passing)

            # Rename columns
            dataframes = rename_columns_players_stats(dataframes)

            # Case selector passing
            if len(dataframes) > 2 and selectors["id"] == "passing":

                if "players_stats" not in data_dict:
                    data_dict["players_stats"] = {"home": {}, "away": {}}

                home_data = pd.merge(dataframes[0], dataframes[1])
                away_data = pd.merge(dataframes[2], dataframes[3])
                home_stats = home_data.set_index("player").to_dict(orient="index")
                away_stats = away_data.set_index("player").to_dict(orient="index")

                for type_team, stats in [("home", home_stats), ("away", away_stats)]:
                    for player, player_stats in stats.items():
                        if " Players" not in player:
                            team_players_stats = data_dict["players_stats"][type_team]
                            team_players_stats.setdefault(player, {}).update(
                                player_stats
                            )

            # Case selector shots
            if selectors["id"] == "shots":

                if "shots" not in data_dict:
                    data_dict["shots"] = {}

                for minute, sub_df in dataframes[0].groupby("minute"):
                    if len(sub_df) > 1:
                        data_dict["shots"][minute] = [
                            sub_df.iloc[i, 1:].to_dict() for i in range(len(sub_df))
                        ]
                    else:
                        data_dict["shots"][minute] = sub_df.iloc[0, 1:].to_dict()

            if selectors["id"] == "goalkeeper":

                if "goalkeeper_stats" not in data_dict:
                    data_dict["goalkeeper_stats"] = {"home": {}, "away": {}}

                home_stats = dataframes[0].set_index("player").to_dict(orient="index")
                away_stats = dataframes[1].set_index("player").to_dict(orient="index")
                for type_team, stats in [("home", home_stats), ("away", away_stats)]:
                    for player, player_stats in stats.items():
                        team_players_stats = data_dict["goalkeeper_stats"][type_team]
                        team_players_stats.setdefault(player, {}).update(player_stats)

            # Other cases
            if (
                selectors["id"] == "summary"
                or selectors["id"] == "defense"
                or selectors["id"] == "possession"
                or selectors["id"] == "miscellaneous"
            ):

                if "players_stats" not in data_dict:
                    data_dict["players_stats"] = {"home": {}, "away": {}}

                home_stats = dataframes[0].set_index("player").to_dict(orient="index")
                away_stats = dataframes[1].set_index("player").to_dict(orient="index")
                for type_team, stats in [("home", home_stats), ("away", away_stats)]:
                    for player, player_stats in stats.items():
                        if " Players" not in player:
                            team_players_stats = data_dict["players_stats"][type_team]
                            team_players_stats.setdefault(player, {}).update(
                                player_stats
                            )

    data_dict = simplejson.loads(simplejson.dumps(data_dict, ignore_nan=True))

    match.update(data_dict)


def get_match_informations(soup: BeautifulSoup) -> None:
    """
    Get general information about the match
    Args:
        soup (BeautifulSoup): soup element
    Return:
        None
    """

    selectors = [
        {
            "id": "home_team",
            "selector": ".scorebox div:nth-of-type(1) div strong a",
        },
        {
            "id": "away_team",
            "selector": ".scorebox div:nth-of-type(2) strong a",
        },
        {"id": "competition", "selector": ".box > div:nth-of-type(1)"},
        {
            "id": "home_season_history",
            "selector": ".scorebox div:nth-of-type(1) div:nth-of-type(3)",
        },
        {
            "id": "away_season_history",
            "selector": ".scorebox div:nth-of-type(2) div:nth-of-type(3)",
        },
        {"id": "goals", "selector": "div.score"},
        {"id": "notes", "selector": ".scorebox_meta div:nth-of-type(4)"},
        {"id": "xg", "selector": "div.score_xg"},
        {"id": "penalties", "selector": "div.score_pen"},
        {
            "id": "home_manager_captain",
            "selector": ".scorebox > div:nth-of-type(1)",
        },
        {
            "id": "away_manager_captain",
            "selector": ".scorebox > div:nth-of-type(2)",
        },
        {"id": "date", "selector": ".scorebox_meta strong a"},
        {"id": "time", "selector": "span.venuetime"},
        {
            "id": "attendance_venue_officials",
            "selector": ".scorebox_meta div:nth-of-type(n+5)",
        },
        {"id": "home_lineup", "selector": "div#a.lineup"},
        {"id": "away_lineup", "selector": "div#b.lineup"},
        {
            "id": "general_statistics",
            "selector": "#team_stats_extra div:nth-of-type(n+4)",
        },
        {
            "id": "general_statistics_2",
            "selector": "#team_stats tr:nth-of-type(n+2) th, td > div > div:nth-of-type(1)",
        },
    ]

    data_dict = {}
    for selector_info in selectors:
        element = soup.select(selector_info["selector"])
        if element:
            if len(element) == 1:
                data_dict[selector_info["id"]] = element[0].get_text()
            else:
                data_dict[selector_info["id"]] = [item.get_text() for item in element]

    return data_dict


def save_match(match: dict, url_match: str, folder_match: str) -> None:
    """
    
    """
    name_json = url_match.split("/")
    name_json = name_json[len(name_json) - 1].replace("-", "_")
    with open(folder_match + "/matchs/" + name_json + ".json", "w") as f:
        json.dump(match, f)


def get_folders_competitions() -> list:
    folder_matchs = [
        os.path.join("data/", folder)
        for folder in os.listdir("data/")
        if os.path.isdir(os.path.join("data/", folder))
    ]
    random.shuffle(folder_matchs)

    return folder_matchs


def get_matchs_urls(folder: str) -> list:
    csv_files = [file for file in os.listdir(folder) if file.endswith(".csv")]

    if csv_files:
        csv_file = os.path.join(folder, csv_files[0])

        # JSON list
        path_folder_json = folder + "/matchs"
        list_json = os.listdir(path_folder_json)
        list_json = [
            file.replace(".json", "").replace("-", "_")
            for file in list_json
            if file.endswith(".json")
        ]

        # Match to scrap
        match_to_scrap = []
        df_links = pd.read_csv(csv_file)
        for _, row in df_links.iterrows():
            if row["Link"].split("/")[6].replace("-", "_") not in list_json:
                match_to_scrap.append([row["Season"], row["Link"]])

        return match_to_scrap


def scrap_match(url: str, folder: str) -> None:
    try:
        soup = get_html_content(url=url[1])
        match = get_match_informations(soup=soup)
        clean_general_informations(match, season=url[0])
        clean_competition(match)
        clean_goals(match)
        clean_notes(match)
        clean_penalties(match)
        clean_xg(match)
        clean_manager_captain(match)
        clean_manager_captain(match)
        clean_season_history(match)
        clean_attendance_stade_avenue(match)
        clean_lineup_formation(match)
        clean_general_statistics(match)
        clean_general_statistics_2(match)
        get_players_stats(soup, match)
        save_match(match, url[1], folder)
        name_file = url[1].split("/")[-1].replace("-", "_")
        print(f"\t\u2705 {name_file}")
    except Exception as e:
        print(f"\t\u274C Error processing {url[1]}: {e}")
