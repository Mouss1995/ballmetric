#!/usr/bin/env python3

"""Module in order to update matchs links for each competition"""

import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup


list_leagues = [
    [
        "https://fbref.com/en/comps/17/schedule/Segunda-Division-Scores-and-Fixtures",
        "La_Liga_2",
    ],
    [
        "https://fbref.com/en/comps/14/schedule/Copa-Libertadores-Scores-and-Fixtures",
        "Copa_Libertadores",
    ],
    [
        "https://fbref.com/en/comps/205/schedule/Copa-Sudamericana-Scores-and-Fixtures",
        "Copa_Sudamericana",
    ],
    [
        "https://fbref.com/en/comps/8/schedule/Champions-League-Scores-and-Fixtures",
        "UEFA_Champions_League",
    ],
    [
        "https://fbref.com/en/comps/19/schedule/Europa-League-Scores-and-Fixtures",
        "UEFA_Europa_League",
    ],
    [
        "https://fbref.com/en/comps/882/schedule/Europa-Conference-League-Scores-and-Fixtures",
        "UEFA_Europa_Conference_League",
    ],
    [
        "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
        "Premier_League",
    ],
    ["https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures", "La_Liga"],
    ["https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures", "Ligue_1"],
    [
        "https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures",
        "Bundesliga",
    ],
    ["https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures", "Serie_A"],
    [
        "https://fbref.com/en/comps/21/schedule/Primera-Division-Scores-and-Fixtures",
        "Argentina_Primera_Division",
    ],
    [
        "https://fbref.com/en/comps/65/schedule/A-League-Men-Scores-and-Fixtures",
        "A-League",
    ],
    [
        "https://fbref.com/en/comps/56/schedule/Austrian-Bundesliga-Scores-and-Fixtures",
        "Austrian_Bundesliga",
    ],
    [
        "https://fbref.com/en/comps/37/schedule/Belgian-Pro-League-Scores-and-Fixtures",
        "Belgian_Pro_League",
    ],
    [
        "https://fbref.com/en/comps/74/schedule/Primera-Division-Scores-and-Fixtures",
        "Division_de_Futbol_Profesional_Bolivia",
    ],
    [
        "https://fbref.com/en/comps/24/schedule/Serie-A-Scores-and-Fixtures",
        "Campeonato_Brasileiro_Serie_A",
    ],
    [
        "https://fbref.com/en/comps/67/schedule/Bulgarian-First-League-Scores-and-Fixtures",
        "Bulgarian_First",
    ],
    [
        "https://fbref.com/en/comps/211/schedule/Canadian-Premier-League-Scores-and-Fixtures",
        "Canadian_Premier_League",
    ],
    [
        "https://fbref.com/en/comps/35/schedule/Primera-Division-Scores-and-Fixtures",
        "Chilean_Primera_Division",
    ],
    [
        "https://fbref.com/en/comps/62/schedule/Chinese-Super-League-Scores-and-Fixtures",
        "Chinese_Football_Association_Super_League",
    ],
    [
        "https://fbref.com/en/comps/41/schedule/Primera-A-Scores-and-Fixtures",
        "Categoria_Primera_A",
    ],
    [
        "https://fbref.com/en/comps/63/schedule/Hrvatska-NL-Scores-and-Fixtures",
        "Croatian_HNL",
    ],
    [
        "https://fbref.com/en/comps/66/schedule/Czech-First-League-Scores-and-Fixtures",
        "Czech_First",
    ],
    [
        "https://fbref.com/en/comps/50/schedule/Superliga-Scores-and-Fixtures",
        "Denmark_Superliga",
    ],
    [
        "https://fbref.com/en/comps/58/schedule/Serie-A-Scores-and-Fixtures",
        "Liga_Profesional_Ecuador",
    ],
    [
        "https://fbref.com/en/comps/43/schedule/Veikkausliiga-Scores-and-Fixtures",
        "Veikkausliiga",
    ],
    [
        "https://fbref.com/en/comps/27/schedule/Super-League-Greece-Scores-and-Fixtures",
        "Super_League_Greece",
    ],
    ["https://fbref.com/en/comps/46/schedule/NB-I-Scores-and-Fixtures", "NB_Hungary"],
    [
        "https://fbref.com/en/comps/82/schedule/Indian-Super-League-Scores-and-Fixtures",
        "India_Super_League",
    ],
    [
        "https://fbref.com/en/comps/64/schedule/Persian-Gulf-Pro-League-Scores-and-Fixtures",
        "Persian_Gulf_Pro_League",
    ],
    [
        "https://fbref.com/en/comps/25/schedule/J1-League-Scores-and-Fixtures",
        "J1_League",
    ],
    [
        "https://fbref.com/en/comps/55/schedule/K-League-1-Scores-and-Fixtures",
        "K_League_1",
    ],
    [
        "https://fbref.com/en/comps/70/schedule/Saudi-Professional-League-Scores-and-Fixtures",
        "Saudia_Professional_League",
    ],
    [
        "https://fbref.com/en/comps/31/schedule/Liga-MX-Scores-and-Fixtures",
        "Liga_MX_Mexico",
    ],
    [
        "https://fbref.com/en/comps/23/schedule/Eredivisie-Scores-and-Fixtures",
        "Eredivisie",
    ],
    [
        "https://fbref.com/en/comps/28/schedule/Eliteserien-Scores-and-Fixtures",
        "Eliteserien",
    ],
    [
        "https://fbref.com/en/comps/61/schedule/Primera-Division-Scores-and-Fixtures",
        "Paraguayan_Primera_Division",
    ],
    [
        "https://fbref.com/en/comps/44/schedule/Liga-1-Scores-and-Fixtures",
        "Liga_1_de_Futbol_Profesional",
    ],
    [
        "https://fbref.com/en/comps/36/schedule/Ekstraklasa-Scores-and-Fixtures",
        "Ekstraklasa_Poland",
    ],
    [
        "https://fbref.com/en/comps/32/schedule/Primeira-Liga-Scores-and-Fixtures",
        "Primeira_Liga",
    ],
    [
        "https://fbref.com/en/comps/47/schedule/Liga-I-Scores-and-Fixtures",
        "Liga_Romania",
    ],
    [
        "https://fbref.com/en/comps/52/schedule/Premier-Division-Scores-and-Fixtures",
        "South_African_Premier_Division",
    ],
    [
        "https://fbref.com/en/comps/30/schedule/Russian-Premier-League-Scores-and-Fixtures",
        "Russian_Premier_League",
    ],
    [
        "https://fbref.com/en/comps/40/schedule/Scottish-Premiership-Scores-and-Fixtures",
        "Scottish_Premiership",
    ],
    [
        "https://fbref.com/en/comps/54/schedule/Serbian-SuperLiga-Scores-and-Fixtures",
        "Serbian_Superliga",
    ],
    [
        "https://fbref.com/en/comps/57/schedule/Swiss-Super-League-Scores-and-Fixtures",
        "Swiss_Super_League",
    ],
    [
        "https://fbref.com/en/comps/29/schedule/Allsvenskan-Scores-and-Fixtures",
        "Allsvenskan",
    ],
    [
        "https://fbref.com/en/comps/26/schedule/Super-Lig-Scores-and-Fixtures",
        "Süper_Lig_Turkey",
    ],
    [
        "https://fbref.com/en/comps/39/schedule/Ukrainian-Premier-League-Scores-and-Fixtures",
        "Ukrainian_Premier_League",
    ],
    [
        "https://fbref.com/en/comps/45/schedule/Primera-Division-Scores-and-Fixtures",
        "Uruguayan_Primera_Division",
    ],
    [
        "https://fbref.com/en/comps/22/schedule/Major-League-Soccer-Scores-and-Fixtures",
        "Major_League_Soccer",
    ],
    [
        "https://fbref.com/en/comps/105/schedule/Liga-FUTVE-Scores-and-Fixtures",
        "Venezuelan_Primera_Division",
    ],
    [
        "https://fbref.com/en/comps/69/schedule/Challenger-Pro-League-Scores-and-Fixtures",
        "Challenger_Pro_League",
    ],
    [
        "https://fbref.com/en/comps/38/schedule/Serie-B-Scores-and-Fixtures",
        "Campeonato_Brasileiro_Serie_B",
    ],
    [
        "https://fbref.com/en/comps/10/schedule/Championship-Scores-and-Fixtures",
        "Championship",
    ],
    ["https://fbref.com/en/comps/60/schedule/Ligue-2-Scores-and-Fixtures", "Ligue_2"],
    [
        "https://fbref.com/en/comps/33/schedule/2-Bundesliga-Scores-and-Fixtures",
        "2_Bundesliga",
    ],
    [
        "https://fbref.com/en/comps/378/schedule/I-League-Scores-and-Fixtures",
        "I-League",
    ],
    ["https://fbref.com/en/comps/18/schedule/Serie-B-Scores-and-Fixtures", "Serie_B"],
    [
        "https://fbref.com/en/comps/49/schedule/J2-League-Scores-and-Fixtures",
        "J2_League",
    ],
    [
        "https://fbref.com/en/comps/51/schedule/Eerste-Divisie-Scores-and-Fixtures",
        "Eerste_Divisie",
    ],
    [
        "https://fbref.com/en/comps/72/schedule/Scottish-Championship-Scores-and-Fixtures",
        "Scottish_Championship",
    ],
    [
        "https://fbref.com/en/comps/48/schedule/Superettan-Scores-and-Fixtures",
        "Superettan",
    ],
]

def update_matchs_urls(leagues):
    """Function to update the list of match links"""

    for league in leagues:
        print("#-------- Update matchs links for", league[1], "--------#")

        folder_path = os.path.join("data", league[1])
        if not os.path.exists(folder_path):
            print("\t❌ 🗂️ Folder", league[1], "does not exist")
            continue

        urls = []

        # Create a dataframe for new links
        csv_path = os.path.join("data", league[1], "match_urls.csv")
        if not os.path.exists(csv_path):
            print("\t❌ 🗒️ CSV file for", league[1], "does not exist")
            continue

        response = requests.get(league[0], timeout=10)
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
        print("\t✅", len_after - len_before, "match urls added ✅")


if __name__ == "__main__":
    update_matchs_urls(list_leagues)
