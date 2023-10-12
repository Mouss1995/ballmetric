""" 
The aim of this script is to generate all the match links for several 
competitions, from the year 2000 to the present day. Folders with the name 
of each competition will be created, containing :
    - a csv file with the match links for each season, 
    - as well as a matches folder, which will contain the json files 
    for each scraped match.
"""
import os
import re
import time
import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup

# ------ Wintry league seasons ------ #
europa_conference_league = [
    "https://fbref.com/en/comps/882/",
    "/schedule/",
    "-Europa-Conference-League-Scores-and-Fixtures",
    "UEFA_Europa_Conference_League",
]
saudi_arabia = [
    "https://fbref.com/en/comps/70/",
    "/schedule/",
    "-Saudi-Professional-League-Scores-and-Fixtures",
    "Saudia_Professional_League",
]
belgian_second = [
    "https://fbref.com/en/comps/69/",
    "/schedule/",
    "-Belgian-First-Division-B-Scores-and-Fixtures",
    "Challenger_Pro_League",
]
south_africa = [
    "https://fbref.com/en/comps/52/",
    "/schedule/",
    "-Premier-Division-Scores-and-Fixtures",
    "South_African_Premier_Division",
]
ukraine = [
    "https://fbref.com/en/comps/39/",
    "/schedule/",
    "-Ukrainian-Premier-League-Scores-and-Fixtures",
    "Ukrainian_Premier_League",
]
belgian_first = [
    "https://fbref.com/en/comps/37/",
    "/schedule/",
    "-Belgian-First-Division-A-Scores-and-Fixtures",
    "Belgian_Pro_League",
]
scottish_2 = [
    "https://fbref.com/en/comps/72/",
    "/schedule/",
    "-Scottish-Championship-Scores-and-Fixtures",
    "Scottish_Championship",
]
champions_league = [
    "https://fbref.com/en/comps/8/",
    "/schedule/",
    "-Champions-League-Scores-and-Fixtures",
    "UEFA_Champions_League",
]
russia = [
    "https://fbref.com/en/comps/30/",
    "/schedule/",
    "-Russian-Premier-League-Scores-and-Fixtures",
    "Russian_Premier_League",
]
scottish_1 = [
    "https://fbref.com/en/comps/40/",
    "/schedule/",
    "-Scottish-Premiership-Scores-and-Fixtures",
    "Scottish_Premiership",
]
iran = [
    "https://fbref.com/en/comps/64/",
    "/schedule/",
    "-Persian-Gulf-Pro-League-Scores-and-Fixtures",
    "Persian_Gulf_Pro_League",
]
switzerland = [
    "https://fbref.com/en/comps/57/",
    "/schedule/",
    "-Swiss-Super-League-Scores-and-Fixtures",
    "Swiss_Super_League",
]
bulgarian = [
    "https://fbref.com/en/comps/67/",
    "/schedule/",
    "-Bulgarian-First-League-Scores-and-Fixtures",
    "Bulgarian_First",
]
austrian = [
    "https://fbref.com/en/comps/56/",
    "/schedule/",
    "-Austrian-Bundesliga-Scores-and-Fixtures",
    "Austrian_Bundesliga",
]
europa_league = [
    "https://fbref.com/en/comps/19/",
    "/schedule/",
    "-Europa-League-Scores-and-Fixtures",
    "UEFA_Europa_League",
]
greece = [
    "https://fbref.com/en/comps/27/",
    "/schedule/",
    "-Super-League-Greece-Scores-and-Fixtures",
    "Super_League_Greece",
]
india = [
    "https://fbref.com/en/comps/82/",
    "/schedule/",
    "-Indian-Super-League-Scores-and-Fixtures",
    "India_Super_League",
]
premier_league = [
    "https://fbref.com/en/comps/9/",
    "/schedule/",
    "-Premier-League-Scores-and-Fixtures",
    "Premier_League",
]
serbia = [
    "https://fbref.com/en/comps/54/",
    "/schedule/",
    "-Serbian-SuperLiga-Scores-and-Fixtures",
    "Serbian_Superliga",
]
eredivisie_2 = [
    "https://fbref.com/en/comps/51/",
    "/schedule/",
    "-Eerste-Divisie-Scores-and-Fixtures",
    "Eerste_Divisie",
]
championship = [
    "https://fbref.com/en/comps/10/",
    "/schedule/",
    "-Championship-Scores-and-Fixtures",
    "Championship",
]
bundesliga_2 = [
    "https://fbref.com/en/comps/33/",
    "/schedule/",
    "-2-Bundesliga-Scores-and-Fixtures",
    "2_Bundesliga",
]
poland = [
    "https://fbref.com/en/comps/36/",
    "/schedule/",
    "-Ekstraklasa-Scores-and-Fixtures",
    "Ekstraklasa_Poland",
]
portugal = [
    "https://fbref.com/en/comps/32/",
    "/schedule/",
    "-Primeira-Liga-Scores-and-Fixtures",
    "Primeira_Liga",
]
la_liga_2 = [
    "https://fbref.com/en/comps/17/",
    "/schedule/",
    "-Segunda-Division-Scores-and-Fixtures",
    "La_Liga_2",
]
czech = [
    "https://fbref.com/en/comps/66/",
    "/schedule/",
    "-Czech-First-League-Scores-and-Fixtures",
    "Czech_First",
]
denmark = [
    "https://fbref.com/en/comps/50/",
    "/schedule/",
    "-Superliga-Scores-and-Fixtures",
    "Denmark_Superliga",
]
turkey = [
    "https://fbref.com/en/comps/26/",
    "/schedule/",
    "-Super-Lig-Scores-and-Fixtures",
    "Süper_Lig_Turkey",
]
bundesliga = [
    "https://fbref.com/en/comps/20/",
    "/schedule/",
    "-Bundesliga-Scores-and-Fixtures",
    "Bundesliga",
]
eredivisie = [
    "https://fbref.com/en/comps/23/",
    "/schedule/",
    "-Eredivisie-Scores-and-Fixtures",
    "Eredivisie",
]
australia = [
    "https://fbref.com/en/comps/65/",
    "/schedule/",
    "-A-League-Men-Scores-and-Fixtures",
    "A-League",
]
india_second = [
    "https://fbref.com/en/comps/378/",
    "/schedule/",
    "-I-League-Scores-and-Fixtures",
    "I-League",
]
mexico = [
    "https://fbref.com/en/comps/31/",
    "/schedule/",
    "-Liga-MX-Scores-and-Fixtures",
    "Liga_MX_Mexico",
]
romania = [
    "https://fbref.com/en/comps/47/",
    "/schedule/",
    "-Liga-I-Scores-and-Fixtures",
    "Liga_Romania",
]
croatian = [
    "https://fbref.com/en/comps/63/",
    "/schedule/",
    "-1-HNL-Scores-and-Fixtures",
    "Croatian_HNL",
]
hungary = [
    "https://fbref.com/en/comps/46/",
    "/schedule/",
    "-NB-I-Scores-and-Fixtures",
    "NB_Hungary",
]
la_liga = [
    "https://fbref.com/en/comps/12/",
    "/schedule/",
    "-La-Liga-Scores-and-Fixtures",
    "La_Liga",
]
serie_a = [
    "https://fbref.com/en/comps/11/",
    "/schedule/",
    "-Serie-A-Scores-and-Fixtures",
    "Serie_A",
]
serie_b = [
    "https://fbref.com/en/comps/18/",
    "/schedule/",
    "-Serie-B-Scores-and-Fixtures",
    "Serie_B",
]
ligue_1 = [
    "https://fbref.com/en/comps/13/",
    "/schedule/",
    "-Ligue-1-Scores-and-Fixtures",
    "Ligue_1",
]
ligue_2 = [
    "https://fbref.com/en/comps/60/",
    "/schedule/",
    "-Ligue-2-Scores-and-Fixtures",
    "Ligue_2",
]

winter_links = [
    la_liga,
    la_liga_2,
    premier_league,
    championship,
    serie_a,
    serie_b,
    ligue_1,
    ligue_2,
    bundesliga,
    bundesliga_2,
    belgian_first,
    belgian_second,
    austrian,
    bulgarian,
    croatian,
    czech,
    denmark,
    greece,
    hungary,
    eredivisie,
    eredivisie_2,
    poland,
    portugal,
    romania,
    russia,
    scottish_1,
    scottish_2,
    serbia,
    switzerland,
    turkey,
    mexico,
    australia,
    india,
    india_second,
    iran,
    saudi_arabia,
    south_africa,
    ukraine,
    champions_league,
    europa_conference_league,
    europa_league,
]

# ------ Summer league seasons ------ #
argentina = [
    "https://fbref.com/en/comps/21/",
    "/schedule/",
    "-Primera-Division-Scores-and-Fixtures",
    "Argentina_Primera_Division",
]
bolivia = [
    "https://fbref.com/en/comps/74/",
    "/schedule/",
    "-Primera-Division-Scores-and-Fixtures",
    "Division_de_Futbol_Profesional_Bolivia",
]
brazil = [
    "https://fbref.com/en/comps/24/",
    "/schedule/",
    "-Serie-A-Scores-and-Fixtures",
    "Campeonato_Brasileiro_Serie_A",
]
canada = [
    "https://fbref.com/en/comps/211/",
    "/schedule/",
    "-Canadian-Premier-League-Scores-and-Fixtures",
    "Canadian_Premier_League",
]
chile = [
    "https://fbref.com/en/comps/35/",
    "/schedule/",
    "-Primera-Division-Scores-and-Fixtures",
    "Chilean_Primera_Division",
]
china = [
    "https://fbref.com/en/comps/62/",
    "/schedule/",
    "-Chinese-Super-League-Scores-and-Fixtures",
    "Chinese_Football_Association_Super_League",
]
colombia = [
    "https://fbref.com/en/comps/41/",
    "/schedule/",
    "-Primera-A-Scores-and-Fixtures",
    "Categoria_Primera_A",
]
ecuador = [
    "https://fbref.com/en/comps/58/",
    "/schedule/",
    "-Serie-A-Scores-and-Fixtures",
    "Liga_Profesional_Ecuador",
]
finland = [
    "https://fbref.com/en/comps/43/",
    "/schedule/",
    "-Veikkausliiga-Scores-and-Fixtures",
    "Veikkausliiga",
]
japan = [
    "https://fbref.com/en/comps/25/",
    "/schedule/",
    "-J1-League-Scores-and-Fixtures",
    "J2_League",
]
japan_second = [
    "https://fbref.com/en/comps/49/",
    "/schedule/",
    "-J2-League-Scores-and-Fixtures",
    "J1_League",
]
korea = [
    "https://fbref.com/en/comps/55/",
    "/schedule/",
    "-K-League-1-Scores-and-Fixtures",
    "K_League_1",
]
norway = [
    "https://fbref.com/en/comps/28/",
    "/schedule/",
    "-Eliteserien-Scores-and-Fixtures",
    "Eliteserien",
]
paraguay = [
    "https://fbref.com/en/comps/61/",
    "/schedule/",
    "-Primera-Division-Scores-and-Fixtures",
    "Paraguayan_Primera_Division",
]
peru = [
    "https://fbref.com/en/comps/44/",
    "/schedule/",
    "-Liga-1-Scores-and-Fixtures",
    "Liga_1_de_Futbol_Profesional",
]
sweden = [
    "https://fbref.com/en/comps/29/",
    "/schedule/",
    "-Allsvenskan-Scores-and-Fixtures",
    "Allsvenskan",
]
sweden_second = [
    "https://fbref.com/en/comps/48/",
    "/schedule/",
    "-Superettan-Scores-and-Fixtures",
    "Superettan",
]
uruguay = [
    "https://fbref.com/en/comps/45/",
    "/schedule/",
    "-Primera-Division-Scores-and-Fixtures",
    "Uruguayan_Primera_Division",
]
united_states = [
    "https://fbref.com/en/comps/22/",
    "/schedule/",
    "-Major-League-Soccer-Scores-and-Fixtures",
    "Major_League_Soccer",
]
venezuela = [
    "https://fbref.com/en/comps/105/",
    "/schedule/",
    "-Liga-FUTVE-Scores-and-Fixtures",
    "Venezuelan_Primera_Division",
]
brazil_second = [
    "https://fbref.com/en/comps/38/",
    "/schedule/",
    "-Serie-B-Scores-and-Fixtures",
    "Campeonato_Brasileiro_Serie_B",
]
uefa_super_cup = [
    "https://fbref.com/en/comps/122/",
    "/schedule/",
    "-league-Scores-and-Fixtures",
    "UEFA_Super_Cup",
]
copa_sudamericana = [
    "https://fbref.com/en/comps/205/",
    "/schedule/",
    "-Copa-Sudamericana-Scores-and-Fixtures",
    "Copa_Sudamericana",
]
copa_libertadores = [
    "https://fbref.com/en/comps/14/",
    "/schedule/",
    "-Copa-Libertadores-Scores-and-Fixtures",
    "Copa_Libertadores",
]

summer_links = [
    argentina,
    bolivia,
    brazil,
    canada,
    chile,
    china,
    colombia,
    ecuador,
    finland,
    japan,
    japan_second,
    korea,
    norway,
    paraguay,
    peru,
    sweden,
    sweden_second,
    uruguay,
    united_states,
    venezuela,
    brazil_second,
    uefa_super_cup,
    copa_libertadores,
    copa_sudamericana,
]


def initialization_summer_url_links(leagues):
    """
    This function will retrieve the match links for each summer competition.
    """

    for league in leagues:
        print("#-------- Link initialization for", league[3], "--------#")

        # Create folder if not exist
        if not os.path.exists("data/" + league[3]):
            os.makedirs("data/" + league[3])
            os.makedirs("data/" + league[3] + "/matchs")
            print("\t=======> 🗂️ Folder", league[3].replace("_", " "), "created")

        # Generate links for seasons and add to list
        links = []
        for i in range(2000, (datetime.datetime.now().year) + 1):
            links.append(league[0] + str(i) + league[1] + str(i) + league[2])

        # Create a dataframe with all matchs links for each competition
        df_final = pd.DataFrame(columns=["Season", "Link"])
        for link in links:
            urls = []
            response = requests.get(link, timeout=10)
            time.sleep(7)
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
                print("\t❌ No matchs for", season_match, "season ❌")
            else:
                df_season = pd.DataFrame({"Season": season_match, "Link": urls})
                df_season = df_season.drop_duplicates(subset=["Link"], keep="last")
                df_season.reset_index(drop=True, inplace=True)
                df_final = pd.concat([df_final, df_season])
                print(
                    "\t✅", len(df_season), "match links for", season_match, "season ✅"
                )

        # Save dataframe
        df_final = df_final.drop_duplicates(subset=["Link"], keep="last")
        df_final.reset_index(drop=True, inplace=True)
        df_final.to_csv("data/" + league[3] + "/match_urls.csv")
        print("\t=======>", len(df_final), "match links retrieved\n")


def initialization_winter_url_links(leagues):
    """
    This function will retrieve the match links for each winter competition.
    """

    for league in leagues:
        print("#-------- Link initialization for", league[3], "--------#")

        # Create folder if not exist
        if not os.path.exists("data/" + league[3]):
            os.makedirs("data/" + league[3])
            os.makedirs("data/" + league[3] + "/matchs")
            print("\t=======> 🗂️ Folder", league[3].replace("_", " "), "created")

        # Generate links for seasons and add to list
        links = []
        for i in range(2000, (datetime.datetime.now().year) + 1):
            links.append(
                league[0]
                + str(i)
                + "-"
                + str(i + 1)
                + league[1]
                + str(i)
                + "-"
                + str(i + 1)
                + league[2]
            )

        # Create a dataframe with all matchs links for each competition
        df_final = pd.DataFrame(columns=["Season", "Link"])
        for link in links:
            urls = []
            response = requests.get(link, timeout=10)
            time.sleep(7)
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
                print("\t❌ No matchs for", season_match, "season ❌")
            else:
                df_season = pd.DataFrame({"Season": season_match, "Link": urls})
                df_season = df_season.drop_duplicates(subset=["Link"], keep="last")
                df_season.reset_index(drop=True, inplace=True)
                df_final = pd.concat([df_final, df_season])
                print(
                    "\t✅", len(df_season), "match links for", season_match, "season ✅"
                )

        # Save dataframe
        df_final = df_final.drop_duplicates(subset=["Link"], keep="last")
        df_final.reset_index(drop=True, inplace=True)
        df_final.to_csv("data/" + league[3] + "/match_urls.csv")
        print("\t=======>", len(df_final), "match links retrieved\n")


if __name__ == "__main__":
    initialization_winter_url_links(winter_links)
    initialization_summer_url_links(summer_links)
