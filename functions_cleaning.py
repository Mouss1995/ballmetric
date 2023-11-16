# pylint: disable=missing-module-docstring
# pylint: disable=missing-function-docstring

import json
import math
import re
from datetime import datetime


def clean_general_informations(match, cleaned_match, season):
    # ------- Teams ------- #
    cleaned_match["Teams"] = {"Home": match["Home_Team"], "Away": match["Away_Team"]}

    # -------- Season -------- #
    cleaned_match["Season"] = season

    # -------- Time -------- #
    if "Time" in match:
        cleaned_match["Time"] = match["Time"].replace(" (venue time)", "")

    # -------- Date -------- #
    if "Date" in match:
        cleaned_match["Date"] = match["Date"]

        date_object = datetime.strptime(match["Date"], "%A %B %d, %Y")

        cleaned_match["Date"] = date_object.strftime("%d-%m-%Y")


def clean_competition(match, cleaned_match):
    competition = re.split(r"\s+\(", match["Competition"])
    competition = [partie.strip("()") for partie in competition]
    cleaned_match["Competition"] = competition[0]
    match["Matchweek"] = competition[1]
    if "Matchweek" in match["Matchweek"]:
        cleaned_match["Matchweek"] = int(match["Matchweek"].replace("Matchweek ", ""))
    else:
        cleaned_match["Round"] = match["Matchweek"].split(")")[0]


def clean_goals(match, cleaned_match):
    if len(match["Home_Goals"]) > 1:
        match["Home_Goals"] = int(match["Home_Goals"][0])
    match["Away_Goals"] = int(match["Away_Goals"])
    cleaned_match["Goals"] = {"Home": match["Home_Goals"], "Away": match["Away_Goals"]}


def clean_penalties(match, cleaned_match):
    if "Home_Penalties" in match and "Away_Penalties" in match:
        if len(match["Home_Penalties"]) > 1:
            match["Home_Penalties"] = int(match["Home_Penalties"][0])
        match["Away_Penalties"] = int(match["Away_Penalties"])
        cleaned_match["Penalties"] = {
            "Home": match["Home_Penalties"],
            "Away": match["Away_Penalties"],
        }


def clean_xg(match, cleaned_match):
    if "Home_xG" in match and "Away_xG" in match:
        if len(match["Home_xG"]) > 1:
            match["Home_xG"] = float(match["Home_xG"][0].replace(",", "."))
        match["Away_xG"] = float((match["Away_xG"]).replace(",", "."))
        cleaned_match["xG"] = {"Home": match["Home_xG"], "Away": match["Away_xG"]}


def clean_manager(match, cleaned_match):
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


def clean_captain(match, cleaned_match):
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


def clean_season_history(match, cleaned_match):
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
                    "Victory": int(match[team_history][0]),
                    "Draw": int(match[team_history][1]),
                    "Defeat": int(match[team_history][2]),
                }
                match[team_history] = dict_history
                if "Season_History" not in cleaned_match:
                    cleaned_match["Season_History"] = {}
                if team_history == "Home_Season_History":
                    cleaned_match["Season_History"]["Home"] = match[team_history]
                else:
                    cleaned_match["Season_History"]["Away"] = match[team_history]


def clean_attendance_stade_avenue(match, cleaned_match):
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


def clean_lineup_formation(match, cleaned_match):
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


def clean_general_statistics(match, cleaned_match):
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
                        team_stats = general_stats_2[key][team]
                        if "of" in el:
                            el = el.split(" of ")
                            if el[0] == "":
                                team_stats["Success"] = 0
                            else:
                                team_stats["Success"] = int(el[0])
                            if el[1] == "":
                                team_stats["Failed"] = 0
                            else:
                                team_stats["Failed"] = int(el[1])
                        else:
                            if el == "":
                                team_stats["Percentage"] = 0
                            else:
                                team_stats["Percentage"] = int(el)
                else:
                    new_value = int(value[team].replace("%", ""))
                    general_stats_2[key][team] = new_value
        cleaned_match.update(general_stats_2)


def clean_events(match, cleaned_match):
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


def clean_players_statistics(match, cleaned_match):
    # -------- Players statistics -------- #
    players_stats = ["home_players_stats", "away_players_stats"]
    cleaned_match["Players Statistics"] = {}
    for stats in players_stats:
        if stats in match:
            if stats == "home_players_stats":
                cleaned_match["Players Statistics"]["Home"] = match[
                    "home_players_stats"
                ]
            else:
                cleaned_match["Players Statistics"]["Away"] = match[
                    "away_players_stats"
                ]

    # -------- Goalkeeper statistics -------- #
    goals_stats = ["home_goalkeeper_stats", "away_goalkeeper_stats"]
    cleaned_match["Goalkeepers Statistics"] = {}
    for stats in goals_stats:
        if stats in match:
            if stats == "home_goalkeeper_stats":
                cleaned_match["Goalkeepers Statistics"]["Home"] = match[
                    "home_goalkeeper_stats"
                ]
            else:
                cleaned_match["Goalkeepers Statistics"]["Away"] = match[
                    "away_goalkeeper_stats"
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


def remove_empty_dicts(cleaned_match):
    if isinstance(cleaned_match, dict):
        # Check if 'Home' and 'Away' keys have empty dictionaries, and remove them if empty.
        if (
            "Home" in cleaned_match
            and "Away" in cleaned_match
            and not cleaned_match["Home"]
            and not cleaned_match["Away"]
        ):
            del cleaned_match["Home"]
            del cleaned_match["Away"]

        # Recursively process sub-dictionaries.
        for key, value in list(cleaned_match.items()):
            if isinstance(value, dict):
                remove_empty_dicts(value)
                if (
                    not value
                ):  # If the value is an empty dictionary after recursion, remove the key.
                    del cleaned_match[key]
            elif isinstance(value, list):
                for item in value:
                    remove_empty_dicts(item)
                    if not item:
                        value.remove(item)
    return cleaned_match


def save_match(match_dict, url_match, folder_match):
    name_json = url_match.split("/")
    name_json = name_json[len(name_json) - 1].replace("-", "_")
    with open(folder_match + "/matchs/" + name_json + ".json", "w") as f:
        json.dump(match_dict, f)
