import json
import math
import os
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup


def scrap_match(url):
    # Get url content
    response = requests.get(url)
    time.sleep(7)
    html_content = response.content
    soup = BeautifulSoup(html_content, "html.parser")

    # Define selectors for each elements
    selectors = [
        {
            "id": "Home_Team",
            "selector": ".scorebox div:nth-of-type(1) div strong a",
        },  # Team Home
        {
            "id": "Away_Team",
            "selector": ".scorebox div:nth-of-type(2) strong a",
        },  # Team Away
        {"id": "Competition", "selector": ".box > div:nth-of-type(1)"},  # Competition
        {
            "id": "Home_Season_History",
            "selector": ".scorebox div:nth-of-type(1) div:nth-of-type(3)",
        },  # Season History Home
        {
            "id": "Away_Season_History",
            "selector": ".scorebox div:nth-of-type(2) div:nth-of-type(3)",
        },  # Season History Away
        {"id": "Home_Goals", "selector": "div:nth-of-type(1) div.score"},  # Goals Home
        {
            "id": "Away_Goals",
            "selector": ".scorebox div:nth-of-type(2) .scores div.score",
        },  # Goals Away
        {"id": "Home_xG", "selector": "div:nth-of-type(1) div.score_xg"},  # xG Home
        {
            "id": "Away_xG",
            "selector": ".scorebox div:nth-of-type(2) .scores div.score_xg",
        },  # xG Away
        {
            "id": "Home_Penalties",
            "selector": "div:nth-of-type(1) div.score_pen",
        },  # Penalties Home
        {
            "id": "Away_Penalties",
            "selector": ".scorebox div:nth-of-type(2) .scores div.score_pen",
        },  # Penalties Away
        {
            "id": "Home_Manager",
            "selector": ".scorebox > div:nth-of-type(1)",
        },  # Manager Home
        {
            "id": "Away_Manager",
            "selector": ".scorebox > div:nth-of-type(2)",
        },  # Manager Away
        {
            "id": "Home_Captain",
            "selector": ".scorebox > div:nth-of-type(1)",
        },  # Captain Home
        {
            "id": "Away_Captain",
            "selector": ".scorebox > div:nth-of-type(2)",
        },  # Captain Away
        {"id": "Date", "selector": ".scorebox_meta strong a"},  # Date
        {"id": "Time", "selector": "span.venuetime"},  # Time
        {
            "id": "Attendance-Venue-Officials",
            "selector": ".scorebox_meta div:nth-of-type(n+5)",
        },  # Attendance / Venue / Officials
        {"id": "Home_Lineup", "selector": "div#a.lineup"},  # Home lineup
        {"id": "Away_Lineup", "selector": "div#b.lineup"},  # Away lineup
        {
            "id": "General_Statistics",
            "selector": "#team_stats_extra div:nth-of-type(n+4)",
        },  # Generals statistics
        {
            "id": "General_Statistics_2",
            "selector": "#team_stats tr:nth-of-type(n+2) th, td > div > div:nth-of-type(1)",
        },  # Generals statistics 2
        {
            "id": "Home_Events",
            "selector": "div.a:nth-of-type(n+3) > div",
        },  # Home summary
        {
            "id": "Away_Events",
            "selector": "div.b:nth-of-type(n+3) > div",
        },  # Away summary
        {
            "id": "Home_Events_Penalties",
            "selector": "div.a:nth-of-type(n+34)",
        },  # Home penalties events
        {
            "id": "Away_Events_Penalties",
            "selector": "div.b:nth-of-type(n+33)",
        },  # Away penalties events
    ]

    # Define selectors for each tables
    selectors_table = [
        {
            "id": "Tables_Stats_Summary",
            "selector": "table[id*=summary]",
        },  # Table Summary
        {
            "id": "Tables_Stats_Passing",
            "selector": "table[id*=passing]",
        },  # Table Passing
        {
            "id": "Tables_Stats_Passing_Types",
            "selector": "table[id*=passing_types]",
        },  # Tables Passing Types
        {
            "id": "Tables_Stats_Defense",
            "selector": "table[id*=defense]",
        },  # Table Statistics Defense
        {
            "id": "Tables_Stats_Possession",
            "selector": "table[id*=possession]",
        },  # Table Possession
        {
            "id": "Tables_Stats_Misc",
            "selector": "table[id*=misc]",
        },  # Table Generals Statistics
        {"id": "Tables_Stats_Keeper", "selector": "table[id*=keeper]"},  # Table Goals
        {"id": "Tables_Stats_Shot", "selector": "table[id*=shots_all]"},  # Table Shots
    ]

    # Get generals statistics with selectors
    data_dict = {}
    for selector_info in selectors:
        element = soup.select(selector_info["selector"])
        if element:
            if len(element) == 1:
                data_dict[selector_info["id"]] = element[0].get_text()
            else:
                data_dict[selector_info["id"]] = [item.get_text() for item in element]

    # Get tables with selectors
    data_table = {}
    for selector_info in selectors_table:
        elements = soup.select(selector_info["selector"])
        if elements:
            i = 0
            for element in elements:
                table_df = pd.read_html(str(element))[0]
                if selector_info["id"] != "Tables_Stats_Shot":
                    if i == 0:
                        data_table["Home_" + selector_info["id"]] = table_df
                    else:
                        data_table["Away_" + selector_info["id"]] = table_df
                else:
                    data_table[selector_info["id"]] = table_df
                i += 1

    # Rename columns dataframe
    for el in data_table:
        data_table[el].columns = data_table[el].columns.map("_".join)
        data_table[el] = data_table[el].reset_index(drop=True)
        table_headers = data_table[el].columns.tolist()
        new_headers = [
            element.split("Unnamed")[1].split("_", 3)[-1]
            if "Unnamed" in element
            else element
            for element in table_headers
        ]
        data_table[el].columns = new_headers

    # Separate table before merge
    Home_Players_Stats = [
        value
        for key, value in data_table.items()
        if "Home" in key and "Keeper" not in key
    ]
    Away_Players_Stats = [
        value
        for key, value in data_table.items()
        if "Away" in key and "Keeper" not in key
    ]
    Home_Goalkeeper_Stats = [
        value for key, value in data_table.items() if "Home_Tables_Stats_Keeper" in key
    ]
    Away_Goalkeeper_Stats = [
        value for key, value in data_table.items() if "Away_Tables_Stats_Keeper" in key
    ]
    Shots_Stats = [value for key, value in data_table.items() if "Shot" in key]
    if Home_Goalkeeper_Stats:
        Home_Goalkeeper_Stats = Home_Goalkeeper_Stats[0]
    if Away_Goalkeeper_Stats:
        Away_Goalkeeper_Stats = Away_Goalkeeper_Stats[0]
    if Shots_Stats:
        Shots_Stats = Shots_Stats[0]

    # Merge for data_table_home list
    if Home_Players_Stats:
        df = Home_Players_Stats[0]
        for i in range(1, len(Home_Players_Stats)):
            common_cols = set(df.columns).intersection(Home_Players_Stats[i].columns)
            df = df.merge(
                Home_Players_Stats[i],
                how="inner",
                left_on=list(common_cols),
                right_on=list(common_cols),
            )
        Home_Players_Stats = df.copy()

    # Merge for data_table_away list
    if Away_Players_Stats:
        df = Away_Players_Stats[0]
        for i in range(1, len(Away_Players_Stats)):
            common_cols = set(df.columns).intersection(Away_Players_Stats[i].columns)
            df = df.merge(
                Away_Players_Stats[i],
                how="inner",
                left_on=list(common_cols),
                right_on=list(common_cols),
            )
        Away_Players_Stats = df.copy()

    # Transform statistics dataframes in dictionnary
    lst_df = [
        Home_Players_Stats,
        Away_Players_Stats,
        Home_Goalkeeper_Stats,
        Away_Goalkeeper_Stats,
        Shots_Stats,
    ]

    for df in lst_df:
        result_dict = {}

        if isinstance(df, pd.DataFrame):
            for index, row in df.iterrows():
                key = row["Player"]
                values_dict = row.drop("Player").to_dict()
                result_dict[key] = values_dict

            df_name = [name for name, var in locals().items() if var is df][0]

            if df_name in data_dict:
                data_dict[df_name].update(result_dict)
            else:
                data_dict[df_name] = result_dict

    return data_dict


def clean_match(match, season):
    
    cleaned_match = {}  # Our new dictionnary with cleaned data

    # ------- Teams ------- #
    cleaned_match["Teams"] = {"Home": match["Home_Team"], "Away": match["Away_Team"]}

    # ------- Competition ------- #
    competition = re.split(r"\s+\(", match["Competition"])
    competition = [partie.strip("()") for partie in competition]
    cleaned_match["Competition"] = competition[0]
    match["Matchweek"] = competition[1]
    if "Matchweek" in match["Matchweek"]:
        cleaned_match["Matchweek"] = int(match["Matchweek"].replace("Matchweek ", ""))
    else:
        cleaned_match["Round"] = match["Matchweek"].split(")")[0]

    # -------- Season -------- #
    cleaned_match["Season"] = season

    # -------- Time -------- #
    if "Time" in match:
        cleaned_match["Time"] = match["Time"].replace(" (venue time)", "")

    # -------- Date -------- #
    if "Date" in match:
        cleaned_match["Date"] = match["Date"]

    # ------- Goals ------- #
    if len(match["Home_Goals"]) > 1:
        match["Home_Goals"] = int(match["Home_Goals"][0])
    match["Away_Goals"] = int(match["Away_Goals"])
    cleaned_match["Goals"] = {"Home": match["Home_Goals"], "Away": match["Away_Goals"]}

    # -------- Penalties -------- #
    if "Home_Penalties" in match and "Away_Penalties" in match:
        if len(match["Home_Penalties"]) > 1:
            match["Home_Penalties"] = int(match["Home_Penalties"][0])
        match["Away_Penalties"] = int(match["Away_Penalties"])
        cleaned_match["Penalties"] = {
            "Home": match["Home_Penalties"],
            "Away": match["Away_Penalties"],
        }

    # -------- xG -------- #
    if "Home_xG" in match and "Away_xG" in match:
        if len(match["Home_xG"]) > 1:
            match["Home_xG"] = float(match["Home_xG"][0].replace(",", "."))
        match["Away_xG"] = float((match["Away_xG"]).replace(",", "."))
        cleaned_match["xG"] = {"Home": match["Home_xG"], "Away": match["Away_xG"]}

    # -------- Manager -------- #
    lst = ["Home_Manager", "Away_Manager"]
    dict_manager = {}
    for el_lst in lst:
        if el_lst in match:
            match[el_lst] = match[el_lst].split("\n")
            for el in match[el_lst]:
                if "Manager" in el:
                    el = el.replace("\xa0", " ").replace("Manager: ", "")
                    if el_lst == "Home_Manager":
                        dict_manager["Home"] = el
                    else:
                        dict_manager["Away"] = el
    if dict_manager:
        cleaned_match["Manager"] = dict_manager

    # -------- Captain -------- #
    lst = ["Home_Captain", "Away_Captain"]
    dict_captain = {}
    for el_lst in lst:
        if el_lst in match:
            match[el_lst] = match[el_lst].split("\n")
            for el in match[el_lst]:
                if "Captain" in el:
                    el = el.replace("\xa0", " ").replace("Captain: ", "")
                    if el_lst == "Home_Captain":
                        dict_captain["Home"] = el
                    else:
                        dict_captain["Away"] = el
    if dict_captain:
        cleaned_match["Captain"] = dict_captain

    # ------- Season History ------- #
    for team_history in ["Home_Season_History", "Away_Season_History"]:
        if team_history in match:
            if isinstance(match[team_history], list):
                match.pop(team_history)
            elif "Match" in match[team_history]:
                match.pop(team_history)
            else:
                dict_history = {}
                match[team_history] = match[team_history].split("-")
                dict_history = {
                    "Victory": match[team_history][0],
                    "Draw": match[team_history][1],
                    "Defeat": match[team_history][2],
                }
                match[team_history] = dict_history
                if "Season_History" not in cleaned_match:
                    cleaned_match["Season_History"] = {}
                if team_history == "Home_Season_History":
                    cleaned_match["Season_History"]["Home"] = match[team_history]
                else:
                    cleaned_match["Season_History"]["Away"] = match[team_history]

    # -------- Attendance / Stade / Arbitres -------- #
    if match["Attendance-Venue-Officials"]:
        for el in match["Attendance-Venue-Officials"]:
            if "Attendance" in el:
                cleaned_match["Attendance"] = int(
                    el.replace("Attendance: ", "").replace(",", "")
                )
            if "Venue" in el:
                cleaned_match["Venue"] = el.replace("Venue: ", "")
            if "Officials" in el:
                officials = el.replace("Officials: ", "")
                if officials != "":
                    officials = officials.split("\xa0· ")
                    match["Officials"] = [
                        element.replace("\xa0", " ") for element in officials
                    ]
                    dict_arbitres = {}
                    for official in match["Officials"]:
                        official = official.split(" (")
                        official[1] = official[1].replace(")", "")
                        dict_arbitres[official[1]] = official[0]
                    cleaned_match["Officials"] = dict_arbitres

    # -------- Lineup & Formation -------- #
    if "Home_Lineup" in match and "Away_Lineup" in match:
        lst_lineup = ["Home_Lineup", "Away_Lineup"]
        for el in lst_lineup:
            lineup = match[el].split("\n")
            lineup = [el for el in lineup if el != ""]
            formation = re.split(r"\s+\(", lineup[0])[1].replace(")", "")
            starting_list = re.findall(r"(\d+)([^\d]+)", lineup[1])
            substitute_list = re.findall(r"(\d+)([^\d]+)", lineup[3])
            starting = {}
            substitute = {}
            for num, name in starting_list:
                starting[num] = name
            for num, name in substitute_list:
                substitute[num] = name
            lineup = {
                "Formation": formation,
                "Starting": starting,
                "Substitute": substitute,
            }
            match[el] = lineup
        cleaned_match["Lineup"] = {
            "Home": match["Home_Lineup"],
            "Away": match["Away_Lineup"],
        }

    # -------- General Statistics -------- #
    if "General_Statistics" in match:
        sublists = []
        for i in range(0, len(match["General_Statistics"]), 3):
            sublist = match["General_Statistics"][i : i + 3]
            sublists.append(sublist)
        for lst in sublists:
            cleaned_match[lst[1]] = {"Home": int(lst[0]), "Away": int(lst[2])}

    # -------- General Statistics 2 -------- #
    if "General_Statistics_2" in match:
        sublists = []
        general_stats_2 = {}
        for i in range(0, len(match["General_Statistics_2"]), 3):
            sublist = match["General_Statistics_2"][i : i + 3]
            if "Cards" not in sublist:
                sublists.append(sublist)
        for lst in sublists:
            general_stats_2[lst[0]] = {"Home": lst[1], "Away": lst[2]}
        for key, value in general_stats_2.items():
            teams = ["Home", "Away"]
            for team in teams:
                if "\xa0" in value[team]:
                    new_value = value[team].replace("\xa0", " ")
                    new_value = new_value.replace("%", "")
                    new_value = new_value.split(" — ")
                    general_stats_2[key][team] = {
                        "Success": "",
                        "Failed": "",
                        "Percentage": "",
                    }
                    for el in new_value:
                        if "of" in el:
                            el = el.split(" of ")
                            if el[0] == "":
                                general_stats_2[key][team]["Success"] = 0
                            else:
                                general_stats_2[key][team]["Success"] = int(el[0])
                            if el[1] == "":
                                general_stats_2[key][team]["Failed"] = 0
                            else:
                                general_stats_2[key][team]["Failed"] = int(el[1])
                        else:
                            if el == "":
                                general_stats_2[key][team]["Percentage"] = 0
                            else:
                                general_stats_2[key][team]["Percentage"] = int(el)
                else:
                    new_value = int(value[team].replace("%", ""))
                    general_stats_2[key][team] = new_value
        cleaned_match.update(general_stats_2)

    # -------- Events -------- #
    if "Home_Events" in match and "Away_Events" in match:
        cleaned_match["Events"] = {}
        dict_events = {}
        for events in ["Home_Events", "Away_Events"]:
            list_events = match[events]
            list_events = [
                event.replace("\n", "").replace("\t", "").replace("\xa0", "")
                for event in list_events
            ]
            sublists = [list_events[i : i + 2] for i in range(0, len(list_events), 2)]
            for el in sublists:
                if "’" in el[0]:
                    temp_events = {}
                    type_team = (
                        cleaned_match["Teams"]["Home"]
                        if events == "Home_Events"
                        else cleaned_match["Teams"]["Away"]
                    )
                    minute = el[0].split("’")[0]
                    score = el[0].split("’")[1]

                    if "Own Goal" in el[1]:
                        player = el[1].split("Own Goal")[0]
                        temp_events = {
                            "Team": type_team,
                            "Event": "Goal",
                            "Type": "Own Goal",
                            "Player": player,
                            "Score": score,
                        }
                        if minute in dict_events:
                            if not isinstance(dict_events[minute], list):
                                dict_events[minute] = [dict_events[minute]]
                            dict_events[minute].append(temp_events)
                        else:
                            dict_events[minute] = temp_events
                        continue

                    if "Penalty saved" in el[1]:
                        player = el[1].split("Penalty saved by ")[0]
                        player_saved_by = (
                            el[1].split("Penalty saved by ")[1].replace("—", "")
                        )
                        temp_events = {
                            "Team": type_team,
                            "Event": "Penalty Saved",
                            "Player": player,
                            "Saved By": player_saved_by,
                            "Score": score,
                        }
                        if minute in dict_events:
                            if not isinstance(dict_events[minute], list):
                                dict_events[minute] = [dict_events[minute]]
                            dict_events[minute].append(temp_events)
                        else:
                            dict_events[minute] = temp_events
                        continue

                    if "Penalty Miss" in el[1]:
                        player = (
                            el[1]
                            .split("—")[0]
                            .replace("Penalty Miss", "")
                            .split("—")[0]
                        )
                        temp_events = {
                            "Team": type_team,
                            "Event": "Penalty Miss",
                            "Player": player,
                            "Score": score,
                        }
                        if minute in dict_events:
                            if not isinstance(dict_events[minute], list):
                                dict_events[minute] = [dict_events[minute]]
                            dict_events[minute].append(temp_events)
                        else:
                            dict_events[minute] = temp_events
                        continue

                    if "Penalty Kick" in el[1]:
                        scorer = el[1].split("—")[0].replace("Penalty Kick", "")
                        temp_events = {
                            "Team": type_team,
                            "Event": "Goal",
                            "Type": "Penalty Kick",
                            "Score": score,
                            "Scorer": scorer,
                        }
                        if minute in dict_events:
                            if not isinstance(dict_events[minute], list):
                                dict_events[minute] = [dict_events[minute]]
                            dict_events[minute].append(temp_events)
                        else:
                            dict_events[minute] = temp_events
                        continue

                    if "Goal" in el[1]:
                        scorer = el[1].split("—")[0]
                        if "Assist:" in scorer:
                            assister = scorer.split("Assist:")[1]
                            scorer = scorer.split("Assist:")[0]
                            temp_events = {
                                "Team": type_team,
                                "Event": "Goal",
                                "Score": score,
                                "Scorer": scorer,
                                "Assister": assister,
                            }
                            if minute in dict_events:
                                if not isinstance(dict_events[minute], list):
                                    dict_events[minute] = [dict_events[minute]]
                                dict_events[minute].append(temp_events)
                            else:
                                dict_events[minute] = temp_events
                            continue
                        else:
                            temp_events = {
                                "Team": type_team,
                                "Event": "Goal",
                                "Score": score,
                                "Scorer": scorer,
                            }
                            if minute in dict_events:
                                if not isinstance(dict_events[minute], list):
                                    dict_events[minute] = [dict_events[minute]]
                                dict_events[minute].append(temp_events)
                            else:
                                dict_events[minute] = temp_events
                            continue

                    if "for " in el[1] and "Substitute" in el[1]:
                        incoming = el[1].split("for ")[0]
                        outcoming = el[1].split("for ")[1].split("—")[0]
                        temp_events = {
                            "Team": type_team,
                            "Event": "Substitute",
                            "Score": score,
                            "Incoming": incoming,
                            "Outcoming": outcoming,
                        }
                        if minute in dict_events:
                            if not isinstance(dict_events[minute], list):
                                dict_events[minute] = [dict_events[minute]]
                            dict_events[minute].append(temp_events)
                        else:
                            dict_events[minute] = temp_events
                        continue

                    if "Yellow Card" in el[1] or "Red Card" in el[1]:
                        player = el[1].split("—")[0]
                        type_card = el[1].split("—")[1]
                        temp_events = {
                            "Team": type_team,
                            "Event": type_card,
                            "Score": score,
                            "Player": player,
                        }
                        if minute in dict_events:
                            if not isinstance(dict_events[minute], list):
                                dict_events[minute] = [dict_events[minute]]
                            dict_events[minute].append(temp_events)
                        else:
                            dict_events[minute] = temp_events
                        continue

            # Update events final dict
            cleaned_match["Events"].update(dict_events)

        # Sorting events by minute and conserved the type of key (string)
        tuple_list = [
            (
                int(key.split("+")[0]),
                int(key.split("+")[1]) if "+" in key else 0,
                key,
                value,
            )
            for key, value in cleaned_match["Events"].items()
        ]
        sorted_tuple_list = sorted(tuple_list, key=lambda x: (x[0], x[1]))
        sorted_data = {key: value for _, _, key, value in sorted_tuple_list}
        cleaned_match["Events"] = {
            key: sorted_data[key] for _, _, key, _ in sorted_tuple_list
        }

    # -------- Players statistics -------- #
    players_stats = ["Home_Players_Stats", "Away_Players_Stats"]
    cleaned_match["Players Statistics"] = {}
    for stats in players_stats:
        if stats in match:
            if stats == "Home_Players_Stats":
                cleaned_match["Players Statistics"]["Home"] = match[
                    "Home_Players_Stats"
                ]
            else:
                cleaned_match["Players Statistics"]["Away"] = match[
                    "Away_Players_Stats"
                ]

    # -------- Goalkeeper statistics -------- #
    goals_stats = ["Home_Goalkeeper_Stats", "Away_Goalkeeper_Stats"]
    cleaned_match["Goalkeepers Statistics"] = {}
    for stats in goals_stats:
        if stats in match:
            if stats == "Home_Goalkeeper_Stats":
                cleaned_match["Goalkeepers Statistics"]["Home"] = match[
                    "Home_Goalkeeper_Stats"
                ]
            else:
                cleaned_match["Goalkeepers Statistics"]["Away"] = match[
                    "Away_Goalkeeper_Stats"
                ]

    # Remove NaN values from Players and goalkeepers Statistics and get key total players
    elements_to_remove = []
    fields = ["Players Statistics", "Goalkeepers Statistics"]
    fields_home_away = ["Home", "Away"]

    for field in fields:
        if field in cleaned_match:
            for field_home_away in fields_home_away:
                if field_home_away in cleaned_match[field]:
                    for subfield in cleaned_match[field][field_home_away]:
                        if " Players" in subfield:
                            elements_to_remove.append(
                                (field, field_home_away, subfield)
                            )
                        else:
                            cleaned_match[field][field_home_away][subfield] = {
                                key: value
                                for key, value in cleaned_match[field][field_home_away][
                                    subfield
                                ].items()
                                if not (
                                    isinstance(value, (float, int))
                                    and math.isnan(value)
                                )
                            }

    # Remove total players
    for element in elements_to_remove:
        field, field_home_away, subfield = element
        cleaned_match[field][field_home_away].pop(subfield)

    return cleaned_match


def remove_empty_dicts(data):
    if isinstance(data, dict):
        # Check if 'Home' and 'Away' keys have empty dictionaries, and remove them if empty.
        if "Home" in data and "Away" in data and not data["Home"] and not data["Away"]:
            del data["Home"]
            del data["Away"]

        # Recursively process sub-dictionaries.
        for key, value in list(data.items()):
            if isinstance(value, dict):
                remove_empty_dicts(value)
                if (
                    not value
                ):  # If the value is an empty dictionary after recursion, remove the key.
                    del data[key]
            elif isinstance(value, list):
                for item in value:
                    remove_empty_dicts(item)
                    if not item:
                        value.remove(item)

    return data


def save_match(match, url, folder):
    name_json = url.split("/")
    name_json = name_json[len(name_json) - 1].replace("-", "_")
    with open(folder + "/matchs/" + name_json + ".json", "w") as f:
        json.dump(match, f)


def launch_scraping():
    folder_matchs = [
        os.path.join("data/", folder)
        for folder in os.listdir("data/")
        if os.path.isdir(os.path.join("data/", folder))
    ]

    for folder in folder_matchs:
        csv_files = [file for file in os.listdir(folder) if file.endswith(".csv")]
        if not csv_files:
            print("\tNo CSV file containing match links for", folder, "\n")
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
                    match = scrap_match(url=url[1])
                    match_cleaned = clean_match(match=match, season=url[0])
                    match_cleaned = remove_empty_dicts(match_cleaned)
                    save_match(match_cleaned, url[1], folder)
                    print("\t✅", url[1].split("/")[-1].replace("-", "_"), "✅")


if __name__ == "__main__":
    launch_scraping()
