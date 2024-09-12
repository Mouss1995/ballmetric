#!/usr/bin/env python3
# pylint: disable=broad-exception-caught

"""Module in order to launch scraping"""

import os

from functions.clean import (
    clean_attendance_stade_avenue,
    clean_competition,
    clean_general_informations,
    clean_lineup_formation,
    clean_manager_captain,
    clean_notes,
    clean_penalties,
)
from functions.initialiase_urls import initialisation
from functions.insert_postgres import (
    insert_data,
    open_connection_postgresql,
    test_connection_db,
)
from functions.scrap import (
    get_folders_competitions,
    get_gk_stats,
    get_html_content,
    get_match_informations,
    get_matchs_urls,
    get_players_stats,
    get_shots_stats,
)
from functions.update import update_matchs_urls


def main():
    """
    Main function for scraping matchs
    and insert to postgresql
    """
    print(
        """
    ██████╗  █████╗ ██╗     ██╗     ███╗   ███╗███████╗████████╗██████╗ ██╗ ██████╗
    ██╔══██╗██╔══██╗██║     ██║     ████╗ ████║██╔════╝╚══██╔══╝██╔══██╗██║██╔════╝
    ██████╔╝███████║██║     ██║     ██╔████╔██║█████╗     ██║   ██████╔╝██║██║     
    ██╔══██╗██╔══██║██║     ██║     ██║╚██╔╝██║██╔══╝     ██║   ██╔══██╗██║██║     
    ██████╔╝██║  ██║███████╗███████╗██║ ╚═╝ ██║███████╗   ██║   ██║  ██║██║╚██████╗
    ╚═════╝ ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝ ╚═════╝
                            Scrap matchs from Fbref and add to postgresql database!
    """
    )
    # Test connection db
    test_connection_db()
    # Link initialisation (if data folder not exist)
    if not os.path.exists("data/"):
        initialisation()
    else:
        # Update links matchs
        update_matchs_urls()
    # Open connection
    connection = open_connection_postgresql()
    cur = connection.cursor()
    # Iterate over competitions
    folders = get_folders_competitions()
    for folder in folders:
        list_urls = get_matchs_urls(cur, folder)
        name_competition = folder.split("/")[1]
        print(f"\n\t\U0001f3c6 {name_competition} : {len(list_urls)} matchs to recover")
        # Scrap all new matchs
        for url in list_urls:
            try:
                # Scrap match
                soup = get_html_content(url=url[1])
                match = get_match_informations(soup=soup, url=url[1])
                get_players_stats(soup, match)
                get_gk_stats(soup, match)
                get_shots_stats(soup, match)
                # Clean data
                clean_general_informations(match, season=url[0])
                clean_competition(match)
                clean_notes(match)
                clean_penalties(match)
                clean_manager_captain(match)
                clean_attendance_stade_avenue(match)
                clean_lineup_formation(match)
                # Insert data to postgresql
                insert_data(match, cur, connection)
                name_file = url[1].split("/")[-1].replace("-", "_")
                print(f"\t\u2705 {name_file}")
            except Exception as e:
                print(f"\t\u274c Error processing {url[1]}: {e}")
    cur.close()
    connection.close()


if __name__ == "__main__":
    main()
