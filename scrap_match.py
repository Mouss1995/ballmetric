#!/usr/bin/env python3

"""Module in order to launch scraping"""

from functions_scraping import get_folders_competitions, get_matchs_urls, scrap_match

from functions_update import update_matchs_urls

if __name__ == "__main__":
    update_matchs_urls()
    folders = get_folders_competitions()
    for folder in folders:
        list_urls = get_matchs_urls(folder)
        name_competition = folder.split("/")[1]
        print(f"\n\t\U0001F3C6 {name_competition} : {len(list_urls)} matchs to recover")
        for url in list_urls:
            scrap_match(url, folder)
