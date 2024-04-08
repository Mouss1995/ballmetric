#!/usr/bin/env python3

"""Module in order to update matchs links for each competition"""

import os
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup


def update_matchs_urls() -> None:
    """Function to update the list of match links"""

    df_competitions = pd.read_csv("competitions/urls_seasons_competitions.csv")
    df_competitions = df_competitions.sort_values(by=["rank"])

    for _, row in df_competitions.iterrows():
        competition_name = row["name"]
        while True:
            try:

                print(f"\t\U0001F504 Update matchs links for {competition_name}")

                folder_path = os.path.join("data", competition_name)
                if not os.path.exists(folder_path):
                    print(
                        f"\t\u274C \U0001F5C2 Folder {competition_name} does not exist"
                    )
                    break

                urls = []

                csv_path = os.path.join("data", competition_name, "match_urls.csv")
                if not os.path.exists(csv_path):
                    print(
                        f"\t\u274C \U0001F5D2 CSV file for {competition_name} does not exist"
                    )
                    break

                response = requests.get(row["update_link"], timeout=10)
                time.sleep(7)
                soup = BeautifulSoup(response.content, "html.parser")
                season_element = soup.select("h1")[0].get_text()
                season_element = re.findall(r"\b\d{4}\b", season_element)
                season_element = (
                    season_element[0]
                    if len(season_element) == 1
                    else "-".join(season_element)
                )

                if season_element:
                    elements_url = soup.select(".center a")
                    urls = [
                        "https://fbref.com" + element.get("href")
                        for element in elements_url
                        if element.get("href")
                    ]

                if not urls:
                    print("\t\u274C No matches to add")
                    break

                df_season = pd.DataFrame({"Season": season_element, "Link": urls})
                df_matchs = pd.read_csv(csv_path)
                len_before = len(df_matchs)
                df_matchs = pd.concat([df_matchs, df_season])
                df_matchs = df_matchs.drop_duplicates(subset=["Link"], keep="last")
                df_matchs.reset_index(drop=True, inplace=True)
                len_after = len(df_matchs)

                if len_after - len_before == 0:
                    print("\t\u274C No matches to add")
                    break

                df_matchs.to_csv(csv_path, index=False)
                nbre_matchs = len_after - len_before
                print(f"\t\u2705 {nbre_matchs} match urls added")
                break

            except requests.exceptions.Timeout:
                print(f"\t\u274C Timeout occurred {competition_name}")
