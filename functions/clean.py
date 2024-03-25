"""
Function script in order to clean data
"""

import re
from datetime import datetime


def clean_general_informations(match: dict, season: str) -> None:
    """
    Cleaning general informations of the match
    Args:
        match (dict): dict that contains match data
        season (str): season of the match from csv links file
    Return: None
    """
    match["season"] = season
    if "time" in match:
        match["time"] = match["time"].replace(" (venue time)", "")
    if "date" in match:
        date_object = datetime.strptime(match["date"], "%A %B %d, %Y")
        match["date"] = date_object.strftime("%d-%m-%Y")


def clean_competition(match: dict) -> None:
    """
    Cleaning competition of the match
    Args:
        match (dict): dict that contains match data
    Return: None
    """
    if match["competition"]:
        competition = match["competition"].split(",")[0]
        match["competition"] = competition.split("(")[0].strip()
        matchweek_round = re.findall(r"\((.*?)\)", competition)
        if matchweek_round:
            if "Matchweek" in matchweek_round[0]:
                match["matchweek"] = int(matchweek_round[0].replace("Matchweek ", ""))
            else:
                match["round"] = matchweek_round[0]


def clean_notes(match: dict) -> None:
    """
    Cleaning notes of the match
    Args:
        match (dict): dict that contains match data
    Return: None
    """
    match["notes"] = match["notes"].replace("\xa0", "")
    if "*" in match["notes"]:
        match["notes"] = match["notes"].replace("*", "")
    if match["notes"] == "":
        match.pop("notes", None)


def clean_penalties(match: dict) -> None:
    """
    Cleaning penalties of the match
    Args:
        match (dict): dict that contains match data
    Return: None
    """
    if "penalties" in match:
        match["penalties_h"] = int(match["penalties"][0])
        match["penalties_a"] = int(match["penalties"][1])
        match.pop("penalties", None)


def clean_manager_captain(match: dict) -> None:
    """
    Cleaning manager/captain of the match
    Args:
        match (dict): dict that contains match data
    Return: None
    """
    for type_team in ["h", "a"]:
        if "".join(["manager_captain_", type_team]) in match:
            home_captain_manager = match[
                "".join(["manager_captain_", type_team])
            ].split("\n")
            for el_list in home_captain_manager:
                if "Manager" in el_list:
                    match["".join(["manager_", type_team])] = el_list.replace(
                        "Manager: ", ""
                    ).replace("\xa0", " ")
                if "Captain" in el_list:
                    match["".join(["captain_", type_team])] = el_list.replace(
                        "Captain: ", ""
                    ).replace("\xa0", " ")
            match.pop("".join(["manager_captain_", type_team]), None)


def clean_season_history(match: dict) -> None:
    """
    Cleaning season history of the match
    Args:
        match (dict): dict that contains match data
    Return: None
    """
    for type_team in ["season_history_h", "season_history_a"]:
        if type_team in match:
            if not isinstance(match[type_team], list):
                pattern_history = r"\b\d{1,2}-\d{1,2}-\d{1,2}\b"
                result = re.search(pattern_history, match[type_team])
                if result:
                    history_season = match[type_team].split("-")
                    match[type_team] = match[type_team].split("-")
                    match[type_team] = {
                        "victory": int(history_season[0]),
                        "draw": int(history_season[1]),
                        "defeat": int(history_season[2]),
                    }
                else:
                    match.pop(type_team, None)
            else:
                match.pop(type_team, None)


def clean_attendance_stade_avenue(match: dict) -> None:
    """
    Cleaning attendance/stade/avenue of the match
    Args:
        match (dict): dict that contains match data
    Return: None
    """
    if match["attendance_venue_officials"]:
        for el in match["attendance_venue_officials"]:
            if "Attendance" in el:
                match["attendance"] = int(
                    el.replace("Attendance: ", "").replace(",", "")
                )
            if "Venue" in el:
                match["venue"] = el.replace("Venue: ", "")
            if "Officials" in el:
                officials = el.replace("Officials: ", "")
                if officials != "":
                    officials = officials.split("\xa0· ")
                    match["officials"] = [
                        element.replace("\xa0", " ") for element in officials
                    ]
                    dict_arbitres = {}
                    for official in match["officials"]:
                        official = official.split(" (")
                        official[1] = (
                            official[1]
                            .replace(")", "")
                            .lower()
                            .replace("4th", "fourth")
                        )
                        dict_arbitres[official[1]] = official[0]
                    match["officials"] = dict_arbitres
        match.pop("attendance_venue_officials", None)


def clean_lineup_formation(match: dict) -> None:
    """
    Cleaning lineup and formation of the match
    Args:
        match (dict): dict that contains match data
    Return: None
    """

    for type_team in ["h", "a"]:
        if "".join(["lineup_", type_team]) in match:
            el_lineup = "".join(["lineup_", type_team])
            lineup = match[el_lineup].split("\n")
            lineup = [el for el in lineup if el != ""]
            try:
                formation = re.split(r"\s+\(", lineup[0])[1].replace(")", "")
                match["".join(["formation_", type_team])] = formation
            except IndexError:
                match["".join(["formation_", type_team])] = None


def clean_possession(match: dict) -> None:
    """
    Cleaning possession data
    Args:
        match (dict): dict that contains match data
    Return: None
    """
    if "possession" in match:
        if isinstance(match["possession"], list):
            if "Possession" in match["possession"]:
                for index, item in enumerate(match["possession"]):
                    if item == "Possession":
                        dict_possession = {}
                        possession_data = match["possession"][index + 1 : index + 3]
                        possession_data = [
                            int(el.replace("%", "")) for el in possession_data
                        ]
                        (
                            dict_possession["possession_h"],
                            dict_possession["possession_a"],
                        ) = possession_data
                        match.pop("possession", None)
                        break
            else:
                match.pop("possession", None)
