# pylint: disable=missing-module-docstring
# pylint: disable=missing-function-docstring

import os

import pandas as pd

from functions_cleaning import (
    clean_attendance_stade_avenue,
    clean_captain,
    clean_competition,
    clean_events,
    clean_general_informations,
    clean_general_statistics,
    clean_goals,
    clean_lineup_formation,
    clean_manager,
    clean_penalties,
    clean_players_statistics,
    clean_season_history,
    clean_xg,
    remove_empty_dicts,
    save_match,
)
from functions_scraping import scraping_match

selectors = [
    {
        "id": "Home_Team",
        "selector": ".scorebox div:nth-of-type(1) div strong a",
    },
    {
        "id": "Away_Team",
        "selector": ".scorebox div:nth-of-type(2) strong a",
    },
    {"id": "Competition", "selector": ".box > div:nth-of-type(1)"},
    {
        "id": "Home_Season_History",
        "selector": ".scorebox div:nth-of-type(1) div:nth-of-type(3)",
    },
    {
        "id": "Away_Season_History",
        "selector": ".scorebox div:nth-of-type(2) div:nth-of-type(3)",
    },
    {"id": "Home_Goals", "selector": "div:nth-of-type(1) div.score"},
    {
        "id": "Away_Goals",
        "selector": ".scorebox div:nth-of-type(2) .scores div.score",
    },
    {"id": "Home_xG", "selector": "div:nth-of-type(1) div.score_xg"},
    {
        "id": "Away_xG",
        "selector": ".scorebox div:nth-of-type(2) .scores div.score_xg",
    },
    {
        "id": "Home_Penalties",
        "selector": "div:nth-of-type(1) div.score_pen",
    },
    {
        "id": "Away_Penalties",
        "selector": ".scorebox div:nth-of-type(2) .scores div.score_pen",
    },
    {
        "id": "Home_Manager",
        "selector": ".scorebox > div:nth-of-type(1)",
    },
    {
        "id": "Away_Manager",
        "selector": ".scorebox > div:nth-of-type(2)",
    },
    {
        "id": "Home_Captain",
        "selector": ".scorebox > div:nth-of-type(1)",
    },
    {
        "id": "Away_Captain",
        "selector": ".scorebox > div:nth-of-type(2)",
    },
    {"id": "Date", "selector": ".scorebox_meta strong a"},
    {"id": "Time", "selector": "span.venuetime"},
    {
        "id": "Attendance-Venue-Officials",
        "selector": ".scorebox_meta div:nth-of-type(n+5)",
    },
    {"id": "Home_Lineup", "selector": "div#a.lineup"},
    {"id": "Away_Lineup", "selector": "div#b.lineup"},
    {
        "id": "General_Statistics",
        "selector": "#team_stats_extra div:nth-of-type(n+4)",
    },
    {
        "id": "General_Statistics_2",
        "selector": "#team_stats tr:nth-of-type(n+2) th, td > div > div:nth-of-type(1)",
    },
    {
        "id": "Home_Events",
        "selector": "div.a:nth-of-type(n+3) > div",
    },
    {
        "id": "Away_Events",
        "selector": "div.b:nth-of-type(n+3) > div",
    },
    {
        "id": "Home_Events_Penalties",
        "selector": "div.a:nth-of-type(n+34)",
    },
    {
        "id": "Away_Events_Penalties",
        "selector": "div.b:nth-of-type(n+33)",
    },
]

selectors_table = [
    {
        "id": "Tables_Stats_Summary",
        "selector": "table[id*=summary]",
    },
    {
        "id": "Tables_Stats_Passing",
        "selector": "table[id*=passing]",
    },
    {
        "id": "Tables_Stats_Passing_Types",
        "selector": "table[id*=passing_types]",
    },
    {
        "id": "Tables_Stats_Defense",
        "selector": "table[id*=defense]",
    },
    {
        "id": "Tables_Stats_Possession",
        "selector": "table[id*=possession]",
    },
    {
        "id": "Tables_Stats_Misc",
        "selector": "table[id*=misc]",
    },
    {"id": "Tables_Stats_Keeper", "selector": "table[id*=keeper]"},
    {"id": "Tables_Stats_Shot", "selector": "table[id*=shots_all]"},
]

folder_matchs = [
    os.path.join("data/", folder)
    for folder in os.listdir("data/")
    if os.path.isdir(os.path.join("data/", folder))
]

for folder in folder_matchs:
    csv_files = [file for file in os.listdir(folder) if file.endswith(".csv")]
    if not csv_files:
        print(f"\tNo CSV file containing match links for {folder}\n")
    else:
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
            for index, row in df_links.iterrows():
                if row["Link"].split("/")[6].replace("-", "_") not in list_json:
                    match_to_scrap.append([row["Season"], row["Link"]])

            print(
                "\t#--------",
                folder.replace("data/", "").replace("_", " "),
                ":",
                len(match_to_scrap),
                "matchs to recover -------#",
            )

            for url in match_to_scrap:
                try:
                    scrap_dict_clean = {}
                    scrap_dict = scraping_match(url[1], selectors, selectors_table)
                    clean_general_informations(scrap_dict, scrap_dict_clean, url[0])
                    clean_competition(scrap_dict, scrap_dict_clean)
                    clean_goals(scrap_dict, scrap_dict_clean)
                    clean_penalties(scrap_dict, scrap_dict_clean)
                    clean_xg(scrap_dict, scrap_dict_clean)
                    clean_manager(scrap_dict, scrap_dict_clean)
                    clean_captain(scrap_dict, scrap_dict_clean)
                    clean_season_history(scrap_dict, scrap_dict_clean)
                    clean_attendance_stade_avenue(scrap_dict, scrap_dict_clean)
                    clean_lineup_formation(scrap_dict, scrap_dict_clean)
                    clean_general_statistics(scrap_dict, scrap_dict_clean)
                    clean_events(scrap_dict, scrap_dict_clean)
                    clean_players_statistics(scrap_dict, scrap_dict_clean)
                    remove_empty_dicts(scrap_dict_clean)
                    save_match(scrap_dict_clean, url[1], folder)
                    name_file = url[1].split("/")[-1].replace("-", "_")
                    print(f"\t✅ {name_file}")
                except Exception as e:
                    print(f"Error processing {url[1]}: {e}")
