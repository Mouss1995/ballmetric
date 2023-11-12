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


def initialization_summer_url_links():
    """
    This function will retrieve the match links for each summer competition.
    """

    df_competitions = pd.read_csv("competitions/init_competitions.csv")
    df_competitions = df_competitions[df_competitions["type"] == "summer"]

    for index, row in df_competitions.iterrows():
        competition_name = row["name"]
        print(f"#-------- Link initialization for {competition_name} --------#")

        # Create folder if not exist
        if not os.path.exists("data/" + competition_name):
            # Generate links for seasons and add to list
            links = []
            for i in range(2000, (datetime.datetime.now().year) + 1):
                links.append(
                    row["part1"] + str(i) + row["part2"] + str(i) + row["part3"]
                )

            # Create a dataframe with all matchs links for each competition
            df_final = pd.DataFrame(columns=["Season", "Link"])
            for link in links:
                urls = []
                response = requests.get(link, timeout=10)
                time.sleep(random.randrange(7, 15, 1))
                soup = BeautifulSoup(response.content, "html.parser")
                season_element = soup.select("h1")[0].get_text()
                season_element = re.search(r"(\d{4})", season_element)
                if season_element:
                    season_element = season_element.group(1)
                season_match = re.search(r"/(\d{4})/", link)
                if season_match:
                    season_match = season_match.group(1)
                if season_element == season_match:
                    elements_url = soup.select(".center a")
                    for element in elements_url:
                        href_value = element.get("href")
                        if href_value:
                            urls.append("https://fbref.com" + href_value)
                if not urls:
                    print(f"\t❌ No matchs for {season_match} season ❌")
                    continue

                df_season = pd.DataFrame({"Season": season_match, "Link": urls})
                df_season = df_season.drop_duplicates(subset=["Link"], keep="last")
                df_season.reset_index(drop=True, inplace=True)
                df_final = pd.concat([df_final, df_season])
                print(f"\t✅ {len(df_season)} match links for {season_match} season ✅")

            # Create directory and save dataframe
            os.makedirs("data/" + competition_name)
            os.makedirs("data/" + competition_name + "/matchs")
            print(f"\t🗂️ Folder {competition_name} created")
            df_final = df_final.drop_duplicates(subset=["Link"], keep="last")
            df_final.reset_index(drop=True, inplace=True)
            df_final.to_csv("data/" + competition_name + "/match_urls.csv", index=False)
            print(f"\t=======> {len(df_final)} match links retrieved\n")


def initialization_winter_url_links():
    """
    This function will retrieve the match links for each winter competition.
    """

    df_competitions = pd.read_csv("competitions/init_competitions.csv")
    df_competitions = df_competitions[df_competitions["type"] == "winter"]

    for index, row in df_competitions.iterrows():
        competition_name = row["name"]
        print(f"#-------- Link initialization for {competition_name} --------#")

        # Create folder if not exist
        if not os.path.exists("data/" + competition_name):
            # Generate links for seasons and add to list
            links = []
            for i in range(2000, (datetime.datetime.now().year) + 1):
                links.append(
                    row["part1"]
                    + str(i)
                    + "-"
                    + str(i + 1)
                    + row["part2"]
                    + str(i)
                    + "-"
                    + str(i + 1)
                    + row["part3"]
                )

            # Create a dataframe with all matchs links for each competition
            df_final = pd.DataFrame(columns=["Season", "Link"])
            for link in links:
                urls = []
                response = requests.get(link, timeout=10)
                time.sleep(random.randrange(7, 15, 1))
                soup = BeautifulSoup(response.content, "html.parser")
                season_element = soup.select("h1")[0].text
                season_element = re.search(r"(\d{4}-\d{4})", season_element)
                if season_element:
                    season_element = season_element.group(1)
                season_match = re.search(r"/(\d{4}-\d{4})/", link)
                if season_match:
                    season_match = season_match.group(1)
                if season_element == season_match:
                    elements_url = soup.select(".center a")
                    for element in elements_url:
                        href_value = element.get("href")
                        if href_value:
                            urls.append("https://fbref.com" + href_value)
                if not urls:
                    print(f"\t❌ No matchs for {season_match} season ❌")
                    continue

                df_season = pd.DataFrame({"Season": season_match, "Link": urls})
                df_season = df_season.drop_duplicates(subset=["Link"], keep="last")
                df_season.reset_index(drop=True, inplace=True)
                df_final = pd.concat([df_final, df_season])
                print(f"\t✅ {len(df_season)} match links for {season_match} season ✅")

            # Create directory and save dataframe
            os.makedirs("data/" + competition_name)
            os.makedirs("data/" + competition_name + "/matchs")
            print(f"\t🗂️ Folder {competition_name} created")
            df_final = df_final.drop_duplicates(subset=["Link"], keep="last")
            df_final.reset_index(drop=True, inplace=True)
            df_final.to_csv("data/" + competition_name + "/match_urls.csv", index=False)
            print(f"\t=======> {len(df_final)} match links retrieved\n")


if __name__ == "__main__":
    initialization_winter_url_links()
    initialization_summer_url_links()
