#!/usr/bin/env python3

"""Functions in order to scrap matchs"""

import os
import random
import time
from io import StringIO

import duckdb
import pandas as pd
import requests
import simplejson
from bs4 import BeautifulSoup


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


def get_match_informations(soup: BeautifulSoup, url: str) -> None:
    """
    Get general information about the match
    Args:
        soup (BeautifulSoup): soup element
        url (str): url of the match in order
            to put it in the result dict
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
        {"id": "date_match", "selector": ".scorebox_meta strong a"},
        {"id": "time_match", "selector": "span.venuetime"},
        {
            "id": "attendance_venue_officials",
            "selector": ".scorebox_meta div:nth-of-type(n+5)",
        },
        {"id": "lineup_home", "selector": "div#a.lineup"},
        {"id": "lineup_away", "selector": "div#b.lineup"},
    ]

    data_dict = {}
    data_dict["url"] = url
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


def convert_tables_to_dfs(tables) -> list:
    """
    Convert tables from soup element to dfs
    Args:
        tables (BeautifulSoup): soup element
    Return:
        list: list of dataframes
    """
    dataframes = []
    for table in tables:
        with StringIO(str(table)) as html_buffer:
            df = pd.read_html(html_buffer)[0]
            df = df.drop_duplicates()
            dataframes.append(df)
    return dataframes


def merge_dataframes(liste_dataframes):
    """
    Merge table from players statistics
    Args:
        tables (BeautifulSoup): soup element
    Return:
        list: list of dataframes
    """
    merged_df = liste_dataframes[0]
    for df in liste_dataframes[1:]:
        new_cols = [col for col in df.columns if col not in merged_df.columns]
        merged_df = pd.merge(
            merged_df, df[new_cols + ["player"]], on="player", how="outer"
        )
    q = """SELECT * FROM merged_df WHERE player NOT LIKE '% Players%'"""
    merged_df = duckdb.sql(q).df()
    return merged_df


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

    home_stats, away_stats = [], []
    match["player_stats"] = {"home": [], "away": []}
    for selectors in selectors_table:
        tables = soup.select(selectors["selector"])
        if tables:
            dataframes = convert_tables_to_dfs(tables)
            dataframes = rename_columns_players_stats(dataframes)
            if len(dataframes) > 2 and selectors["id"] == "passing":
                home_stats.append(pd.merge(dataframes[0], dataframes[1]))
                away_stats.append(pd.merge(dataframes[2], dataframes[3]))
            else:
                dataframes[0] = dataframes[0].drop_duplicates()
                dataframes[1] = dataframes[1].drop_duplicates()
                home_stats.append(dataframes[0])
                away_stats.append(dataframes[1])

    home_stats = merge_dataframes(home_stats)
    away_stats = merge_dataframes(away_stats)
    match["player_stats"]["home"] = home_stats.to_dict(orient="records")
    match["player_stats"]["away"] = away_stats.to_dict(orient="records")
    match["player_stats"] = simplejson.loads(
        simplejson.dumps(match["player_stats"], ignore_nan=True)
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
        match["goalkeeper_stats"] = {"home": [], "away": []}
        gk_home, gk_away = [], []
        dataframes = convert_tables_to_dfs(tables)
        dataframes = rename_columns_players_stats(dataframes)
        if len(dataframes) == 2:
            gk_home.append(dataframes[0].drop_duplicates())
            gk_away.append(dataframes[1].drop_duplicates())
            gk_home = merge_dataframes(gk_home)
            gk_away = merge_dataframes(gk_away)
            match["goalkeeper_stats"]["home"] = gk_home.to_dict(orient="records")
            match["goalkeeper_stats"]["away"] = gk_away.to_dict(orient="records")
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
        match["shots"] = []
        dataframes = convert_tables_to_dfs(tables)
        dataframes = rename_columns_players_stats(dataframes)
        match["shots"] = dataframes[0].to_dict(orient="records")
        match["shots"] = simplejson.loads(
            simplejson.dumps(match["shots"], ignore_nan=True)
        )


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


def get_matchs_urls(cur, folder):
    """
    Get matchs urls from CSV files
    Args:
        folder (str): path to folder
    Return:
        list of match urls
    """
    csv_files = [file for file in os.listdir(folder) if file.endswith(".csv")]
    if not csv_files:
        return []
    query = "SELECT * FROM match_urls"
    cur.execute(query)
    results = cur.fetchall()
    df_psql = pd.DataFrame(results, columns=["Link"])
    df_league = pd.read_csv("/".join([folder, "match_urls.csv"]))
    df_league = df_league[~df_league["Link"].isin(df_psql["Link"])]
    match_to_scrap = []
    for _, row in df_league.iterrows():
        match_to_scrap.append([row["Season"], row["Link"]])

    return match_to_scrap
