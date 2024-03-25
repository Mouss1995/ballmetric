#!/usr/bin/env python3
# pylint: disable=broad-exception-caught

"""Module in order to launch scraping"""

import logging

logging.basicConfig(filename="errors.log", level=logging.ERROR)

from functions.update import update_matchs_urls

from functions.scrap import (
    get_folders_competitions,
    get_matchs_urls,
    save_match,
    get_html_content,
    get_match_informations,
    get_players_stats,
    get_gk_stats,
    get_shots_stats,
)

from functions.clean import (
    clean_attendance_stade_avenue,
    clean_competition,
    clean_general_informations,
    clean_possession,
    clean_lineup_formation,
    clean_manager_captain,
    clean_notes,
    clean_penalties,
    clean_season_history,
)

from functions.insert_postgres import (
    open_connection_postgresql,
    insert_matchs_psql,
    insert_players_psql,
    insert_goalkeeper_psql,
    insert_shots_psql,
)

if __name__ == "__main__":
    # update_matchs_urls()
    connection = open_connection_postgresql()
    folders = get_folders_competitions()
    for folder in folders:
        list_urls = get_matchs_urls(folder)
        name_competition = folder.split("/")[1]
        print(f"\n\t\U0001f3c6 {name_competition} : {len(list_urls)} matchs to recover")
        for url in list_urls:
            try:
                soup = get_html_content(url=url[1])
                match = get_match_informations(soup=soup)
                clean_general_informations(match, season=url[0])
                clean_competition(match)
                clean_notes(match)
                clean_penalties(match)
                clean_possession(match)
                clean_manager_captain(match)
                clean_season_history(match)
                clean_attendance_stade_avenue(match)
                clean_lineup_formation(match)
                get_players_stats(soup, match)
                get_gk_stats(soup, match)
                get_shots_stats(soup, match)
                insert_matchs_psql(connection, match)
                insert_players_psql(connection, match)
                insert_goalkeeper_psql(connection, match)
                insert_shots_psql(connection, match)
                save_match(match, url[1], folder)
                name_file = url[1].split("/")[-1].replace("-", "_")
                print(f"\t\u2705 {name_file}")
            except Exception as e:
                print(f"\t\u274c Error processing {url[1]}: {e}")
                logging.error(f"\t\u274c Error processing {url[1]}: {e}")
    connection.close()
