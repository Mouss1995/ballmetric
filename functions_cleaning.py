import re
from datetime import datetime


def clean_general_informations(match: dict, season: str) -> None:
    match["season"] = season
    if "time" in match:
        match["time"] = match["time"].replace(" (venue time)", "")
    if "date" in match:
        date_object = datetime.strptime(match["date"], "%A %B %d, %Y")
        match["date"] = date_object.strftime("%d-%m-%Y")


def clean_competition(match: dict) -> None:
    if match["competition"]:
        competition = match["competition"].split(",")[0]
        match["competition"] = competition.split("(")[0].strip()
        matchweek_round = re.findall(r"\((.*?)\)", competition)
        if matchweek_round:
            if "Matchweek" in matchweek_round[0]:
                match["matchweek"] = int(matchweek_round[0].replace("Matchweek ", ""))
            else:
                match["round"] = matchweek_round[0]


def clean_goals(match: dict) -> None:
    if "--" in match["goals"]:
        match["home_goals"] = None
        match["away_goals"] = None
    else:
        match["home_goals"] = int(match["goals"][0].replace("*", ""))
        match["away_goals"] = int(match["goals"][1].replace("*", ""))
    match.pop("goals", None)


def clean_notes(match):
    match["notes"] = match["notes"].replace("\xa0", "")
    if "*" in match["notes"]:
        match["notes"] = match["notes"].replace("*", "")
    if match["notes"] == "":
        match.pop("notes", None)


def clean_penalties(match: dict) -> None:
    if "penalties" in match:
        match["home_penalties"] = int(match["penalties"][0])
        match["away_penalties"] = int(match["penalties"][1])
        match.pop("penalties", None)


def clean_xg(match: dict) -> None:
    if "xg" in match:
        match["home_xg"] = float(match["xg"][0].replace(",", "."))
        match["away_xg"] = float(match["xg"][1].replace(",", "."))
        match.pop("xg", None)


def clean_manager_captain(match: dict) -> None:
    for type_team in ["home", "away"]:
        if "".join([type_team, "_manager_captain"]) in match:
            home_captain_manager = match[
                "".join([type_team, "_manager_captain"])
            ].split("\n")
            for el_list in home_captain_manager:
                if "Manager" in el_list:
                    match["".join([type_team, "_manager"])] = el_list.replace(
                        "Manager: ", ""
                    ).replace("\xa0", " ")
                if "Captain" in el_list:
                    match["".join([type_team, "_captain"])] = el_list.replace(
                        "Captain: ", ""
                    ).replace("\xa0", " ")
            match.pop("".join([type_team, "_manager_captain"]), None)


def clean_season_history(match: dict) -> None:
    for type_team in ["home_season_history", "away_season_history"]:
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
                        official[1] = official[1].replace(")", "").lower()
                        dict_arbitres[official[1]] = official[0]
                    match["officials"] = dict_arbitres
        match.pop("attendance_venue_officials", None)


def clean_lineup_formation(match: dict) -> None:
    for type_team in ["home", "away"]:
        if "".join([type_team, "_lineup"]) in match:
            el_lineup = "".join([type_team, "_lineup"])
            lineup = match[el_lineup].split("\n")
            lineup = [el for el in lineup if el != ""]
            try:
                formation = re.split(r"\s+\(", lineup[0])[1].replace(")", "")
            except IndexError:
                formation = None
            starting_list = re.findall(r"(\d+)([^\d]+)", lineup[1])
            substitute_list = re.findall(r"(\d+)([^\d]+)", lineup[3])
            starting = {}
            substitute = {}
            for num, name in starting_list:
                starting[num] = name
            for num, name in substitute_list:
                substitute[num] = name
            lineup = {
                "formation": formation,
                "starting": starting,
                "substitute": substitute,
            }
            match[el_lineup] = lineup


def clean_general_statistics(match: dict) -> None:

    if "general_statistics" in match:
        stats = match["general_statistics"]
        cleaned_stats = {}
        for i in range(0, len(stats), 3):
            home_stat = int(stats[i])
            label = stats[i + 1].lower().replace(" ", "_")
            away_stat = int(stats[i + 2])
            cleaned_stats[label] = {"home": home_stat, "away": away_stat}
        match["general_statistics"] = cleaned_stats


def clean_general_statistics_2(match: dict) -> None:

    if "general_statistics_2" in match:
        sublists = []
        general_stats_2 = {}
        for i in range(0, len(match["general_statistics_2"]), 3):
            sublist = match["general_statistics_2"][i : i + 3]
            if "Cards" not in sublist:
                sublists.append(sublist)
        for lst in sublists:
            lst[0] = lst[0].replace(" ", "_").lower()
            general_stats_2[lst[0]] = {"home": lst[1], "away": lst[2]}
        for key, value in general_stats_2.items():
            key = key.replace(" ", "_").lower()
            teams = ["home", "away"]
            for team in teams:
                if "\xa0" in value[team]:
                    new_value = value[team].replace("\xa0", " ")
                    new_value = new_value.replace("%", "")
                    new_value = new_value.split(" — ")
                    general_stats_2[key][team] = {
                        "success": "",
                        "failed": "",
                        "percentage": "",
                    }
                    for el in new_value:
                        team_stats = general_stats_2[key][team]
                        if "of" in el:
                            el = el.split(" of ")
                            if el[0] == "":
                                team_stats["success"] = 0
                            else:
                                team_stats["success"] = int(el[0])
                            if el[1] == "":
                                team_stats["failed"] = 0
                            else:
                                team_stats["failed"] = int(el[1])
                        else:
                            if el == "":
                                team_stats["percentage"] = 0
                            else:
                                team_stats["percentage"] = int(el)
                else:
                    new_value = int(value[team].replace("%", ""))
                    general_stats_2[key][team] = new_value
        if "general_statistics" in match:
            match["general_statistics"].update(general_stats_2)
        match.pop("general_statistics_2", None)
