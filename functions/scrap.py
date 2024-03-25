#!/usr/bin/env python3

"""Functions in order to scrap matchs"""

import time
import json
import random
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
import simplejson


def get_html_content(url: str) -> BeautifulSoup:
    """
    Get the HTML content of the link.
    Args:
        url (string): url of the match
    Return:
        BeautifulSoup: soup element
    """
    response = requests.get(url, timeout=10)
    time.sleep(random.randrange(3, 5, 1))
    html_content = response.content
    soup = BeautifulSoup(html_content, "html.parser")
    return soup


def get_match_informations(soup: BeautifulSoup) -> None:
    """
    Get general information about the match
    Args:
        soup (BeautifulSoup): soup element
    Return: None
    """

    selectors = [
        {
            "id": "team_h",
            "selector": ".scorebox div:nth-of-type(1) div strong a",
        },
        {
            "id": "team_a",
            "selector": ".scorebox div:nth-of-type(2) strong a",
        },
        {"id": "competition", "selector": ".box > div:nth-of-type(1)"},
        {
            "id": "season_history_h",
            "selector": ".scorebox div:nth-of-type(1) div:nth-of-type(3)",
        },
        {
            "id": "season_history_a",
            "selector": ".scorebox div:nth-of-type(2) div:nth-of-type(3)",
        },
        {"id": "notes", "selector": ".scorebox_meta div:nth-of-type(4)"},
        {"id": "penalties", "selector": "div.score_pen"},
        {
            "id": "manager_captain_h",
            "selector": ".scorebox > div:nth-of-type(1)",
        },
        {
            "id": "manager_captain_a",
            "selector": ".scorebox > div:nth-of-type(2)",
        },
        {"id": "date", "selector": ".scorebox_meta strong a"},
        {"id": "time", "selector": "span.venuetime"},
        {
            "id": "attendance_venue_officials",
            "selector": ".scorebox_meta div:nth-of-type(n+5)",
        },
        {"id": "lineup_h", "selector": "div#a.lineup"},
        {"id": "lineup_a", "selector": "div#b.lineup"},
        {
            "id": "possession",
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


def rename_columns_players_stats(lst_dfs: list) -> list:
    """
    Cleaning column names from dataframes players statistics
    Args:
        lst_dfs: list of dataframes coming from soup element
    Return:
        list: list of dataframes with updated column names
    """
    for df in lst_dfs:
        new_columns = []
        for col in df.columns:
            new_col = col[1] if "Unnamed" in col[0] else col[0] + "_" + col[1]
            new_columns.append(new_col)
        new_columns = [
            el_col.replace(" ", "_")
            .replace("-", "_")
            .replace("%", "_percent")
            .replace("/", "_")
            .replace("#", "num")
            .replace("(", "")
            .replace(")", "")
            .replace("1_3", "final_third")
            .replace("+", "_plus_")
            .lower()
            for el_col in new_columns
        ]
        df.columns = new_columns
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
    ]

    for selectors in selectors_table:
        tables = soup.select(selectors["selector"])

        if tables:
            dataframes = []
            for table in tables:
                df_passing = pd.read_html(str(table))[0]
                dataframes.append(df_passing)

            dataframes = rename_columns_players_stats(dataframes)

            if len(dataframes) > 2 and selectors["id"] == "passing":
                dataframes[0] = dataframes[0].drop_duplicates()
                dataframes[1] = dataframes[1].drop_duplicates()
                dataframes[2] = dataframes[2].drop_duplicates()
                dataframes[3] = dataframes[3].drop_duplicates()
                home_stats = (
                    pd.merge(dataframes[0], dataframes[1])
                    .set_index("player")
                    .to_dict(orient="index")
                )
                away_stats = (
                    pd.merge(dataframes[2], dataframes[3])
                    .set_index("player")
                    .to_dict(orient="index")
                )
            else:
                dataframes[0] = dataframes[0].drop_duplicates()
                dataframes[1] = dataframes[1].drop_duplicates()
                home_stats = dataframes[0].set_index("player").to_dict(orient="index")
                away_stats = dataframes[1].set_index("player").to_dict(orient="index")

            if "players_stats" not in match:
                match["players_stats"] = {"home": {}, "away": {}}
            for type_team, stats in [("home", home_stats), ("away", away_stats)]:
                for player, player_stats in stats.items():
                    if " Players" not in player:
                        team_players_stats = match["players_stats"][type_team]
                        team_players_stats.setdefault(player, {}).update(player_stats)

    match["players_stats"] = simplejson.loads(
        simplejson.dumps(match["players_stats"], ignore_nan=True)
    )


def get_gk_stats(soup: BeautifulSoup, match: dict) -> None:
    """
    Get goalkeepers statistics
    Args:
        soup (BeautifulSoup): soup element
        match (dict): dict that contains match data
    Return:
        None
    """
    tables = soup.select("table[id*=keeper]")
    if tables:
        dataframes = []
        for table in tables:
            df_passing = pd.read_html(str(table))[0]
            dataframes.append(df_passing)
        dataframes = rename_columns_players_stats(dataframes)
        if len(dataframes) == 2:
            match["goalkeeper_stats"] = {"home": {}, "away": {}}
            home_stats = dataframes[0].set_index("player").to_dict(orient="index")
            away_stats = dataframes[1].set_index("player").to_dict(orient="index")
            for type_team, stats in [("home", home_stats), ("away", away_stats)]:
                for player, player_stats in stats.items():
                    match["goalkeeper_stats"][type_team].setdefault(player, {}).update(
                        player_stats
                    )

            match["goalkeeper_stats"] = simplejson.loads(
                simplejson.dumps(match["goalkeeper_stats"], ignore_nan=True)
            )


def get_shots_stats(soup: BeautifulSoup, match: dict) -> None:
    """
    Get shots statistics
    Args:
        soup (BeautifulSoup): soup element
        match (dict): dict that contains match data
    Return:
        None
    """
    tables = soup.select("table[id*=shots_all]")
    if tables:
        match["shots"] = {}
        dataframes = []
        for table in tables:
            df_passing = pd.read_html(str(table))[0]
            dataframes.append(df_passing)
        dataframes = rename_columns_players_stats(dataframes)
        for minute, sub_df in dataframes[0].groupby("minute"):
            if len(sub_df) > 1:
                match["shots"][minute] = [
                    sub_df.iloc[i, 1:].to_dict() for i in range(len(sub_df))
                ]
            else:
                match["shots"][minute] = sub_df.iloc[0, 1:].to_dict()

        match["shots"] = simplejson.loads(
            simplejson.dumps(match["shots"], ignore_nan=True)
        )


def save_match(match: dict, url_match: str, folder_match: str) -> None:
    """
    Save dict match as json
    Args:
        match (dict): dict data
        url_match (str): match url
        folder_match (str): path to folder
    Return: None
    """

    name_json = url_match.split("/")
    name_json = name_json[len(name_json) - 1].replace("-", "_")
    with open(
        folder_match + "/matchs/" + name_json + ".json", "w", encoding="utf-8"
    ) as f:
        json.dump(match, f)


def get_folders_competitions() -> list:
    """
    Get folder generate from generate_matchs_urls.py
    Args: None
    Return: None
    """

    df = pd.read_csv("competitions/urls_seasons_competitions.csv")
    df = df.sort_values(by=["rank"])
    competitions = df["name"].tolist()
    folder_matchs = [
        os.path.join("data/", competition)
        for competition in competitions
        if os.path.isdir(os.path.join("data/", competition))
    ]

    return folder_matchs


def get_matchs_urls(folder: str) -> list:
    """
    Get matchs urls from CSV files
    Args:
        folder (str): path to folder
    Return:
        list of match urls
    """

    csv_files = [file for file in os.listdir(folder) if file.endswith(".csv")]

    if csv_files:
        csv_file = os.path.join(folder, csv_files[0])

        path_folder_json = folder + "/matchs"
        list_json = os.listdir(path_folder_json)
        list_json = [
            file.replace(".json", "").replace("-", "_")
            for file in list_json
            if file.endswith(".json")
        ]

        match_to_scrap = []
        df_links = pd.read_csv(csv_file)
        for _, row in df_links.iterrows():
            if row["Link"].split("/")[6].replace("-", "_") not in list_json:
                match_to_scrap.append([row["Season"], row["Link"]])

        return match_to_scrap

    return []
