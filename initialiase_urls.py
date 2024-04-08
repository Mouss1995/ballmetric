#!/usr/bin/env python3

""" 
The aim of this script is to generate all the match links for several 
competitions, from the year 2000 to the present day. Folders with the name 
of each competition will be created, containing :
    - a csv file with the match links for each season, 
    - as well as a matches folder, which will contain the json files 
    for each scraped match.
"""

import datetime
import os
import random
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup


def generate_seasons_links(row: pd.Series) -> list:
    """
    Generate all the season links for
    the specific competition
    """

    print(f"\t\U0001F504 Retrieving match links for {row['name']}")

    type_league = row["type"]

    start_year = 2000
    end_year = datetime.datetime.now().year

    links = (
        [
            row["part1"] + str(i) + row["part2"] + str(i) + row["part3"]
            for i in range(start_year, end_year + 1)
        ]
        if type_league == "summer"
        else [
            f"{row['part1']}{i}-{i+1}{row['part2']}{i}-{i+1}{row['part3']}"
            for i in range(start_year, end_year + 1)
        ]
    )

    return links


def get_matchs_urls(links: list, row: pd.Series) -> None:
    """
    Get match urls for each season of
    the competition
    """

    type_league = row["type"]
    competition_name = row["name"]

    type_regex = r"(\d{4})" if type_league == "summer" else r"(\d{4}-\d{4})"

    df_final = pd.DataFrame(columns=["Season", "Link"])
    for link in links:
        urls = []
        while True:
            try:
                response = requests.get(link, timeout=10)
                time.sleep(random.randrange(3, 7, 1))
                soup = BeautifulSoup(response.content, "html.parser")
                season_element = soup.select("h1")[0].text
                season_element = re.search(type_regex, season_element)
                if season_element:
                    season_element = season_element.group(1)
                season_match = re.search(r"/" + type_regex + "/", link)
                if season_match:
                    season_match = season_match.group(1)
                if season_element == season_match:
                    elements_url = soup.select(".center a")
                    urls = [
                        "https://fbref.com" + element.get("href")
                        for element in elements_url
                        if element.get("href")
                    ]

                if not urls:
                    print(f"\t\u274C No matches for the {season_match} season")
                    break

                df_season = pd.DataFrame({"Season": season_match, "Link": urls})
                df_final = pd.concat([df_final, df_season])
                print(
                    f"\t\u2705 {len(list(set(urls)))} match links for {season_match} season"
                )
                break

            except requests.exceptions.Timeout:
                print(f"\tTimeout occurred {link}")

    df_final = df_final.drop_duplicates(subset=["Link"], keep="last")
    df_final.reset_index(drop=True, inplace=True)
    os.makedirs("data/" + competition_name)
    os.makedirs("data/" + competition_name + "/matchs")
    print(f"\t\U0001F5C2 Folder {competition_name} created")
    df_final.to_csv("data/" + competition_name + "/match_urls.csv", index=False)
    print(f"\t=======> {len(df_final)} match links retrieved\n")


if __name__ == "__main__":
    df_competitions = pd.read_csv("competitions/init_competitions.csv")
    for _, line in df_competitions.iterrows():
        if not os.path.exists(os.path.join(os.getcwd(), "data", line["name"])):
            links_season = generate_seasons_links(line)
            get_matchs_urls(links_season, line)
