#!/usr/bin/env python3

"""Module in order to update matchs links for each competition"""

import os
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup


def update_matchs_urls():
    """Function to update the list of match links"""

    df_competitions = pd.read_csv("competitions/update_competitions.csv")

    for index, row in df_competitions.iterrows():
        competition_name = row["name"]
        print(f"#-------- Update matchs links for {competition_name} --------#")

        folder_path = os.path.join("data", competition_name)
        if not os.path.exists(folder_path):
            print(f"\t❌ 🗂️ Folder {competition_name} does not exist")
            continue

        urls = []

        # Create a dataframe for new links
        csv_path = os.path.join(f"data {competition_name} match_urls.csv")
        if not os.path.exists(csv_path):
            print(f"\t❌ 🗒️ CSV file for {competition_name} does not exist")
            continue

        response = requests.get(row["link"], timeout=10)
        time.sleep(7)
        soup = BeautifulSoup(response.content, "html.parser")
        season_element = soup.select("h1")[0].get_text()
        season_element = re.findall(r"\b\d{4}\b", season_element)
        season_element = (
            season_element[0] if len(season_element) == 1 else "-".join(season_element)
        )

        if season_element:
            elements_url = soup.select(".center a")
            for element in elements_url:
                href_value = element.get("href")
                if href_value:
                    urls.append("https://fbref.com" + href_value)
        if not urls:
            print("\t❌ No matches to add ❌")
            continue

        df_season = pd.DataFrame({"Season": season_element, "Link": urls})
        df_matchs = pd.read_csv(csv_path)
        len_before = len(df_matchs)
        df_matchs = pd.concat([df_matchs, df_season])
        df_matchs = df_matchs.drop_duplicates(subset=["Link"], keep="last")
        df_matchs.reset_index(drop=True, inplace=True)
        len_after = len(df_matchs)

        if len_after - len_before == 0:
            print("\t❌ No matches to add ❌")
            continue

        df_matchs.to_csv(csv_path, index=False)
        nbre_matchs = len_after - len_before
        print(f"\t✅ {nbre_matchs} match urls added ✅")


if __name__ == "__main__":
    update_matchs_urls()
