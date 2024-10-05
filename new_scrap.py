import requests
import duckdb
import pandas as pd
from bs4 import BeautifulSoup
import io

pd.set_option('display.max_columns', None)

def get_elements_url(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    return soup


def get_matchs_overview(content, selectors):
    data = {}
    for key, value in selectors.items():
        if key == 'attendance_venue_official':
            element = content.select(value)
            if element:
                l = []
                for el in element:
                    l.append(el.text)
                l = ' | '.join(l)
                data[key] = l
        else:
            element = content.select_one(value)
            if element:
                data[key] = element.text
    df = pd.DataFrame([data])
    return df


def get_player_summary(content, overview):
    elements = content.select('table[id*=summary]')    
    if not elements:
        return pd.DataFrame() 
    dfs = []
    for i, element in enumerate(elements):
        df = pd.read_html(io.StringIO(str(element)))[0]        
        df.columns = ['_'.join(col).strip() for col in df.columns.values]        
        df.insert(0, 'team_home', overview['team_home'][0])
        df.insert(1, 'team_away', overview['team_away'][0])
        df.insert(2, 'date', overview['date'][0])
        df.insert(3, 'team_player', overview['team_home'][0] if i == 0 else overview['team_away'][0])
        dfs.append(df)
    result_df = pd.concat(dfs, ignore_index=True)
    return result_df


def get_player_passing(content, overview):
    elements = content.select('table[id*=passing]')
    if not elements:
        return pd.DataFrame()
    
    dfs = pd.read_html(io.StringIO(str(elements)))
    for df in dfs:
        df.columns = ['_'.join(col).strip() for col in df.columns.values]
    
    common_columns = list(set(dfs[0].columns).intersection(set(dfs[1].columns)))
    
    df_home = pd.merge(dfs[0], dfs[1], on=common_columns, how='inner')
    df_away = pd.merge(dfs[2], dfs[3], on=common_columns, how='inner')
    
    for df, team in zip([df_home, df_away], ['team_home', 'team_away']):
        df.insert(0, 'team_home', overview['team_home'][0])
        df.insert(1, 'team_away', overview['team_away'][0])
        df.insert(2, 'date', overview['date'][0])
        df.insert(3, 'team_player', overview[team][0])
    
    result_df = pd.concat([df_home, df_away], ignore_index=True)
    return result_df


def get_player_defense(content, overview):
    elements = content.select('table[id*=defense]')    
    if not elements:
        return pd.DataFrame() 
    dfs = []
    for i, element in enumerate(elements):
        df = pd.read_html(io.StringIO(str(element)))[0]        
        df.columns = ['_'.join(col).strip() for col in df.columns.values]        
        df.insert(0, 'team_home', overview['team_home'][0])
        df.insert(1, 'team_away', overview['team_away'][0])
        df.insert(2, 'date', overview['date'][0])
        df.insert(3, 'team_player', overview['team_home'][0] if i == 0 else overview['team_away'][0])
        dfs.append(df)
    result_df = pd.concat(dfs, ignore_index=True)
    return result_df

def get_player_possession(content, overview):
    elements = content.select('table[id*=possession]')    
    if not elements:
        return pd.DataFrame() 
    dfs = []
    for i, element in enumerate(elements):
        df = pd.read_html(io.StringIO(str(element)))[0]        
        df.columns = ['_'.join(col).strip() for col in df.columns.values]        
        df.insert(0, 'team_home', overview['team_home'][0])
        df.insert(1, 'team_away', overview['team_away'][0])
        df.insert(2, 'date', overview['date'][0])
        df.insert(3, 'team_player', overview['team_home'][0] if i == 0 else overview['team_away'][0])
        dfs.append(df)
    result_df = pd.concat(dfs, ignore_index=True)
    return result_df

def get_player_miscellaneous(content, overview):
    elements = content.select('table[id*=misc]')    
    if not elements:
        return pd.DataFrame() 
    dfs = []
    for i, element in enumerate(elements):
        df = pd.read_html(io.StringIO(str(element)))[0]        
        df.columns = ['_'.join(col).strip() for col in df.columns.values]        
        df.insert(0, 'team_home', overview['team_home'][0])
        df.insert(1, 'team_away', overview['team_away'][0])
        df.insert(2, 'date', overview['date'][0])
        df.insert(3, 'team_player', overview['team_home'][0] if i == 0 else overview['team_away'][0])
        dfs.append(df)
    result_df = pd.concat(dfs, ignore_index=True)
    return result_df

def get_player_shots(content, overview):
    element = content.select_one('table[id*=shots_all]')
    if element:
        df = pd.read_html(io.StringIO(str(element)))[0]
        df.columns = ['_'.join(col).strip() for col in df.columns.values]
        df.insert(0, 'team_home', overview['team_home'][0])
        df.insert(1, 'team_away', overview['team_away'][0])
        df.insert(2, 'date', overview['date'][0])
        return df
    
def get_goalkeeper(content, overview):
    elements = content.select('table[id*=keeper]')    
    if not elements:
        return pd.DataFrame() 
    dfs = []
    for i, element in enumerate(elements):
        df = pd.read_html(io.StringIO(str(element)))[0]        
        df.columns = ['_'.join(col).strip() for col in df.columns.values]        
        df.insert(0, 'team_home', overview['team_home'][0])
        df.insert(1, 'team_away', overview['team_away'][0])
        df.insert(2, 'date', overview['date'][0])
        df.insert(3, 'team_player', overview['team_home'][0] if i == 0 else overview['team_away'][0])
        dfs.append(df)
    result_df = pd.concat(dfs, ignore_index=True)
    return result_df

selectors_match = {'team_home':'.scorebox div:nth-of-type(1) div strong a',
            'team_away':'.scorebox div:nth-of-type(2) strong a',
            'competition':'.scorebox_meta div:nth-of-type(2)',
            'date':'.scorebox_meta strong a',
            'manager_home':'div:nth-of-type(1) div.datapoint',
            'manager_away':'.scorebox div:nth-of-type(2) div.datapoint',
            'captain_home':'div:nth-of-type(1) .datapoint a',
            'captain_away':'.scorebox div:nth-of-type(2) .datapoint a',
            'attendance_venue_official':'.scorebox_meta div:nth-of-type(n+5)'
        }

url = "https://fbref.com/en/matches/1cb5e939/Villarreal-Osasuna-September-8-2001-La-Liga"
url = "https://fbref.com/en/matches/bc36a5ed/Eibar-Castellon-August-17-2024-Segunda-Division"
url = "https://fbref.com/en/matches/f0d1a7a8/Napoli-Barcelona-February-21-2024-Champions-League"
url = "https://fbref.com/en/matches/1cb5e939/Villarreal-Osasuna-September-8-2001-La-Liga"


content = get_elements_url(url)
overview = get_matchs_overview(content, selectors_match)
summary = get_player_summary(content, overview)
passing = get_player_passing(content, overview)
defense = get_player_defense(content, overview)
possession = get_player_possession(content, overview)
miscellaneous = get_player_miscellaneous(content, overview)
goalkeeper = get_goalkeeper(content, overview)
shots = get_player_shots(content, overview)