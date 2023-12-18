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


def link_initialization(type_league: str, df_league: pd.DataFrame) -> None:
    """
    Function in order to generate links for each seasons
    and create folder and csv files.
    """
    df_league = df_league[df_league["type"] == type_league]
    df_league = df_league.sample(frac=1)

    for _, row in df_league.iterrows():
        competition_name = row["name"]
        if not os.path.exists("data/" + competition_name):
            print(f"#-------- Link initialization for {competition_name} --------#")
            start_year = 2000
            end_year = datetime.datetime.now().year
            if type_league == "summer":
                type_regex = r"(\d{4})"
                links = [
                    row["part1"] + str(i) + row["part2"] + str(i) + row["part3"]
                    for i in range(start_year, end_year + 1)
                ]
            if type_league == "winter":
                type_regex = r"(\d{4}-\d{4})"
                links = [
                    f"{row['part1']}{i}-{i+1}{row['part2']}{i}-{i+1}{row['part3']}"
                    for i in range(start_year, end_year + 1)
                ]

            # Create a dataframe with all matchs links for each competition
            df_final = pd.DataFrame(columns=["Season", "Link"])
            for link in links:
                urls = []
                while True:
                    try:
                        response = requests.get(link, timeout=10)
                        time.sleep(random.randrange(7, 15, 1))
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
                            for element in elements_url:
                                href_value = element.get("href")
                                if href_value:
                                    urls.append("https://fbref.com" + href_value)
                        if not urls:
                            print(
                                f"\t\u274C No matches for the {season_match} season \u274C"
                            )
                            break

                        df_season = pd.DataFrame({"Season": season_match, "Link": urls})
                        df_season = df_season.drop_duplicates(
                            subset=["Link"], keep="last"
                        )
                        df_season.reset_index(drop=True, inplace=True)
                        df_final = pd.concat([df_final, df_season])
                        print(
                            f"\t\u2705 {len(df_season)} match links "
                            f"for {season_match} season \u2705"
                        )
                        break

                    except requests.exceptions.Timeout:
                        print(f"Timeout occurred {link}")

            # Create directory and save dataframe
            os.makedirs("data/" + competition_name)
            os.makedirs("data/" + competition_name + "/matchs")
            print(f"\t\U0001F5C2 Folder {competition_name} created")
            df_final = df_final.drop_duplicates(subset=["Link"], keep="last")
            df_final.reset_index(drop=True, inplace=True)
            df_final.to_csv("data/" + competition_name + "/match_urls.csv", index=False)
            print(f"\t=======> {len(df_final)} match links retrieved\n")


if __name__ == "__main__":
    league = ["winter", "summer"]
    df_competitions = pd.read_csv("competitions/init_competitions.csv")

    for el in league:
        link_initialization(el, df_competitions)
