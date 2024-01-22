# pylint: disable=missing-module-docstring
# pylint: disable=missing-function-docstring

import time
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup


def scraping_match(url_match, list_selectors, list_selectors_table):
    response = requests.get(url_match, timeout=10)
    time.sleep(7)
    html_content = response.content
    soup = BeautifulSoup(html_content, "html.parser")

    # Get generals statistics with selectors
    data_dict = {}
    for selector_info in list_selectors:
        element = soup.select(selector_info["selector"])
        if element:
            if len(element) == 1:
                data_dict[selector_info["id"]] = element[0].get_text()
            else:
                data_dict[selector_info["id"]] = [item.get_text() for item in element]

    # Get tables with selectors
    data_table = {}
    for selector_info in list_selectors_table:
        elements = soup.select(selector_info["selector"])
        if elements:
            i = 0
            for element in elements:
                html_file_like = StringIO(str(element))
                table_df = pd.read_html(html_file_like)[0]

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
    home_players_stats = [
        value
        for key, value in data_table.items()
        if "Home" in key and "Keeper" not in key
    ]
    away_players_stats = [
        value
        for key, value in data_table.items()
        if "Away" in key and "Keeper" not in key
    ]
    home_goalkeeper_stats = [
        value for key, value in data_table.items() if "Home_Tables_Stats_Keeper" in key
    ]
    away_goalkeeper_stats = [
        value for key, value in data_table.items() if "Away_Tables_Stats_Keeper" in key
    ]
    shots_stats = [value for key, value in data_table.items() if "Shot" in key]
    if home_goalkeeper_stats:
        home_goalkeeper_stats = home_goalkeeper_stats[0]
    if away_goalkeeper_stats:
        away_goalkeeper_stats = away_goalkeeper_stats[0]
    if shots_stats:
        shots_stats = shots_stats[0]

    # Merge for data_table_home list
    if home_players_stats:
        df = home_players_stats[0]
        for i in range(1, len(home_players_stats)):
            common_cols = set(df.columns).intersection(home_players_stats[i].columns)
            df = df.merge(
                home_players_stats[i],
                how="inner",
                left_on=list(common_cols),
                right_on=list(common_cols),
            )
        home_players_stats = df.copy()

    # Merge for data_table_away list
    if away_players_stats:
        df = away_players_stats[0]
        for i in range(1, len(away_players_stats)):
            common_cols = set(df.columns).intersection(away_players_stats[i].columns)
            df = df.merge(
                away_players_stats[i],
                how="inner",
                left_on=list(common_cols),
                right_on=list(common_cols),
            )
        away_players_stats = df.copy()

    # Transform statistics dataframes in dictionnary
    lst_df = [
        home_players_stats,
        away_players_stats,
        home_goalkeeper_stats,
        away_goalkeeper_stats,
        shots_stats,
    ]

    for df in lst_df:
        result_dict = {}

        if isinstance(df, pd.DataFrame):
            for _, line in df.iterrows():
                key = line["Player"]
                values_dict = line.drop("Player").to_dict()
                result_dict[key] = values_dict

            df_name = [name for name, var in locals().items() if var is df][0]

            if df_name in data_dict:
                data_dict[df_name].update(result_dict)
            else:
                data_dict[df_name] = result_dict

    return data_dict
