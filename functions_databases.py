"""Function script in order to interact with database"""

from datetime import datetime

import psycopg2


def open_connection_postgresql():
    """Open connexion Postgresql"""
    # Paramètres de connexion à la base de données
    db_params = {
        "dbname": "ballmetric",
        "user": "alancuzon",
        "password": "bilbao1995",
        # 'host': 'votre_host',
        # 'port': 'votre_port'
    }

    # Connexion à la base de données
    connection = psycopg2.connect(**db_params)

    return connection


# connection = open_connection_postgresql()


def insert_postgresql(connection, match):
    """Insert data in postgresql database"""
    try:
        # Création d'un curseur
        with connection.cursor() as cursor:
            req = """
                INSERT INTO match_info
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            date_formatted = match.get("Date", None)
            if date_formatted is not None:
                date_formatted = datetime.strptime(date_formatted, "%d-%m-%Y").strftime(
                    "%Y-%m-%d"
                )

            # Utiliser la méthode de formatage pour insérer les valeurs
            valeurs = (
                match.get("Teams", {}).get("Home", None),
                match.get("Teams", {}).get("Away", None),
                match.get("Season", None),
                match.get("Goals", {}).get("Home", None),
                match.get("Goals", {}).get("Away", None),
                match.get("Competition", None),
                date_formatted,
                match.get("Time", None),
                match.get("Matchweek", None),
                match.get("Round", None),
                match.get("Manager", {}).get("Home", None),
                match.get("Manager", {}).get("Away", None),
                match.get("Captain", {}).get("Home", None),
                match.get("Captain", {}).get("Away", None),
                match.get("Attendance", None),
                match.get("Venue", None),
                match.get("Season_History", {}).get("Home", {}).get("Victory", None),
                match.get("Season_History", {}).get("Home", {}).get("Defeat", None),
                match.get("Season_History", {}).get("Home", {}).get("Draw", None),
                match.get("Season_History", {}).get("Away", {}).get("Victory", None),
                match.get("Season_History", {}).get("Away", {}).get("Defeat", None),
                match.get("Season_History", {}).get("Away", {}).get("Draw", None),
                match.get("Officials", {}).get("Referee", None),
                match.get("Officials", {}).get("AR1", None),
                match.get("Officials", {}).get("AR2", None),
                match.get("Officials", {}).get("4th", None),
                match.get("Officials", {}).get("VAR", None),
                match.get("Lineup", {}).get("Home", {}).get("Formation", None),
                match.get("Lineup", {}).get("Away", {}).get("Formation", None),
                match.get("xG", {}).get("Home", None),
                match.get("xG", {}).get("Away", None),
                match.get("Fouls", {}).get("Home", None),
                match.get("Fouls", {}).get("Away", None),
                match.get("Corners", {}).get("Home", None),
                match.get("Corners", {}).get("Away", None),
                match.get("Crosses", {}).get("Home", None),
                match.get("Crosses", {}).get("Away", None),
                match.get("Touches", {}).get("Home", None),
                match.get("Touches", {}).get("Away", None),
                match.get("Tackles", {}).get("Home", None),
                match.get("Tackles", {}).get("Away", None),
                match.get("Interceptions", {}).get("Home", None),
                match.get("Interceptions", {}).get("Away", None),
                match.get("Aerials Won", {}).get("Home", None),
                match.get("Aerials Won", {}).get("Away", None),
                match.get("Clearances", {}).get("Home", None),
                match.get("Clearances", {}).get("Away", None),
                match.get("Offsides", {}).get("Home", None),
                match.get("Offsides", {}).get("Home", None),
                match.get("Goal Kicks", {}).get("Home", None),
                match.get("Goal Kicks", {}).get("Away", None),
                match.get("Throw Ins", {}).get("Home", None),
                match.get("Throw Ins", {}).get("Away", None),
                match.get("Long Balls", {}).get("Home", None),
                match.get("Long Balls", {}).get("Away", None),
                match.get("Possession", {}).get("Home", None),
                match.get("Possession", {}).get("Away", None),
                match.get("Passing Accuracy", {}).get("Home", {}).get("Success", None),
                match.get("Passing Accuracy", {}).get("Home", {}).get("Failed", None),
                match.get("Passing Accuracy", {}).get("Away", {}).get("Success", None),
                match.get("Passing Accuracy", {}).get("Away", {}).get("Failed", None),
                match.get("Shots on Target", {}).get("Home", {}).get("Success", None),
                match.get("Shots on Target", {}).get("Home", {}).get("Failed", None),
                match.get("Shots on Target", {}).get("Away", {}).get("Success", None),
                match.get("Shots on Target", {}).get("Away", {}).get("Failed", None),
                match.get("Saves", {}).get("Home", {}).get("Success", None),
                match.get("Saves", {}).get("Home", {}).get("Failed", None),
                match.get("Saves", {}).get("Away", {}).get("Success", None),
                match.get("Saves", {}).get("Away", {}).get("Failed", None),
                match.get("Yellow Cards", {}).get("Home", None),
                match.get("Yellow Cards", {}).get("Away", None),
                match.get("Red Cards", {}).get("Home", None),
                match.get("Red Cards", {}).get("Away", None),
                match.get("Notes", None),
            )

            cursor.execute(req, valeurs)

            # Validation des changements
            connection.commit()

    except Exception as e:
        print(f"Erreur: {e}")
        # Annulation des changements en cas d'erreur
        connection.rollback()


def insert_shots_psql(connection, match):
    """Insert data into match table postgresql database"""
    try:
        if "Shots" in match:
            for el, data_shots in match["Shots"].items():
                if el != "NaN":
                    with connection.cursor() as cursor:
                        req = """
                            INSERT INTO shots(team_h, team_a, date_match, player, minute_shot, team, xg, outcome, 
                                            distance, penalty, body_part, sca_1_player, sca_1_event, 
                                            sca_2_player, sca_2_event)
                            VALUES (%(team_h)s, %(team_a)s, %(date_match)s, %(player)s, %(minute_shot)s, %(team)s, %(xg)s, %(outcome)s, 
                                    %(distance)s, %(penalty)s, %(body_part)s, %(sca_1_player)s, %(sca_1_event)s, 
                                    %(sca_2_player)s, %(sca_2_event)s);
                        """

                        values = {
                            "team_h": match.get("Teams", {}).get("Home", None),
                            "team_a": match.get("Teams", {}).get("Away", None),
                            "date_match": (
                                datetime.strptime(
                                    match.get("Date", ""), "%d-%m-%Y"
                                ).strftime("%Y-%m-%d")
                                if match.get("Date")
                                else None
                            ),
                            "player": (
                                el.replace(" (pen)", "") if " (pen)" in el else el
                            ),
                            "minute_shot": data_shots.get("Minute", None),
                            "team": data_shots.get("Squad", None),
                            "xg": data_shots.get("xG", None),
                            "outcome": data_shots.get("Outcome", None),
                            "distance": data_shots.get("Distance", None),
                            "penalty": "Yes" if " (pen)" in el else "No",
                            "body_part": data_shots.get("Body Part", None),
                            "sca_1_player": data_shots.get("SCA 1_Player", None),
                            "sca_1_event": data_shots.get("SCA 1_Event", None),
                            "sca_2_player": data_shots.get("SCA 2_Player", None),
                            "sca_2_event": data_shots.get("SCA 2_Event", None),
                        }

                        cursor.execute(req, values)

                        # Validation des changements
                        connection.commit()

    except Exception as e:
        print(f"Erreur: {e}")
        # Annulation des changements en cas d'erreur
        connection.rollback()


def insert_events_psql(connection, match):
    """Insert data into event table postgresql database"""
    try:
        if "Events" in match:
            lst_data_events = []
            for el, data_events in match["Events"].items():
                if isinstance(data_events, list):
                    for i in range(0, len(data_events)):
                        data_events[i]["Min"] = el
                        lst_data_events.append(data_events[i])
                else:
                    data_events["Min"] = el
                    lst_data_events.append(data_events)

            for data_event in lst_data_events:
                with connection.cursor() as cursor:
                    req = """
                        INSERT INTO events(team_h, team_a, date_match, min_event, type_event, team, player, 
                        score, scorer, assister, incoming, outcoming, own_goal, penalty_saved_by)
                        VALUES (%(team_h)s, %(team_a)s, %(date_match)s, %(min_event)s, %(type_event)s, %(team)s, %(player)s, 
                        %(score)s, %(scorer)s, %(assister)s, %(incoming)s, %(outcoming)s, %(own_goal)s, %(penalty_saved_by)s);
                    """

                    values = {
                        "team_h": match.get("Teams", {}).get("Home", None),
                        "team_a": match.get("Teams", {}).get("Away", None),
                        "date_match": (
                            datetime.strptime(
                                match.get("Date", ""), "%d-%m-%Y"
                            ).strftime("%Y-%m-%d")
                            if match.get("Date")
                            else None
                        ),
                        "min_event": data_event.get("Min", None),
                        "type_event": data_event.get("Event", None),
                        "team": data_event.get("Team", None),
                        "player": data_event.get("Player", None),
                        "score": data_event.get("Score", None),
                        "scorer": data_event.get("Scorer", None),
                        "assister": data_event.get("Assister", None),
                        "incoming": data_event.get("Incoming", None),
                        "outcoming": data_event.get("Outcoming", None),
                        "own_goal": (
                            "Yes"
                            if data_event.get("Type", None) == "Own Goal"
                            else "No"
                        ),
                        "penalty_saved_by": data_event.get("Saved By", None),
                    }

                    cursor.execute(req, values)

                    # Validation des changements
                    connection.commit()

    except Exception as e:
        print(f"Erreur: {e}")
        # Annulation des changements en cas d'erreur
        connection.rollback()


def insert_players_psql(connection, match):
    """Insert data into match table postgresql database"""
    try:
        if "Players Statistics" in match:
            for type_team in ["Home", "Away"]:
                for el, data_players in match["Players Statistics"][type_team].items():
                    with connection.cursor() as cursor:
                        req = """
                            INSERT INTO players(team_h, team_a, date_match, name_player, team_player, num, nation, pos, age_player,
                            holder, min_played, performance_gls, performance_ast, performance_pk, performance_pkatt,
                            performance_sh, performance_sot, performance_crdy, performance_crdr, performance_touches, performance_tkl,
                            performance_int, performance_blocks, expected_xg, expected_npxg, expected_xag, sca_sca,
                            sca_gca, passes_cmp, passes_att, passes_cmp_percent, passes_prgp, carries_carries,
                            carries_prgc, take_ons_att, take_ons_succ, total_cmp, total_att, total_cmp_percent,
                            total_totdist, total_prgdist, short_cmp, short_att, short_cmp_percent, medium_cmp,
                            medium_att, medium_cmp_percent, long_cmp, long_att, long_cmp_percent, ast,
                            xag, xa, kp, final_1_3, ppa, crspa,
                            prgp, att, pass_types_live, pass_types_dead, pass_types_fk, pass_types_tb,
                            pass_types_sw, pass_types_crs, pass_types_ti, pass_types_ck, corner_kicks_in, corner_kicks_out,
                            corner_kicks_str, outcomes_cmp, outcomes_off, outcomes_blocks, tackles_tkl, tackles_tklw,
                            tackles_def_3rd, tackles_mid_3rd, tackles_att_3rd, challenges_tkl, challenges_att, challenges_tkl_percent,
                            challenges_lost, blocks_blocks, blocks_sh, blocks_pass, interceptions, tkl_plus_int,
                            clr, err, touches_touches, touches_def_pen, touches_def_3rd, touches_mid_3rd,
                            touches_att_3rd, touches_att_pen, touches_live, take_ons_succ_percent, take_ons_tkld, take_ons_tkld_percent,
                            carries_totdist, carries_prgdist, carries_1_3, carries_cpa, carries_mis, carries_dis,
                            receiving_rec, receiving_prgr, performance_2crdy, performance_fls, performance_fld, performance_off,
                            performance_crs, performance_tklw, performance_pkwon, performance_pkcon, performance_og, performance_recov,
                            aerial_duels_won, aerial_duels_lost, aerial_duels_won_percent)
                            VALUES (%(team_h)s, %(team_a)s, %(date_match)s, %(name_player)s, %(team_player)s, %(num)s, %(nation)s, %(pos)s, %(age_player)s,
                            %(holder)s, %(min_played)s, %(performance_gls)s, %(performance_ast)s, %(performance_pk)s, %(performance_pkatt)s,
                            %(performance_sh)s, %(performance_sot)s, %(performance_crdy)s, %(performance_crdr)s, %(performance_touches)s, %(performance_tkl)s,
                            %(performance_int)s, %(performance_blocks)s, %(expected_xg)s, %(expected_npxg)s, %(expected_xag)s, %(sca_sca)s,
                            %(sca_gca)s, %(passes_cmp)s, %(passes_att)s, %(passes_cmp_percent)s, %(passes_prgp)s, %(carries_carries)s,
                            %(carries_prgc)s, %(take_ons_att)s, %(take_ons_succ)s, %(total_cmp)s, %(total_att)s, %(total_cmp_percent)s,
                            %(total_totdist)s, %(total_prgdist)s, %(short_cmp)s, %(short_att)s, %(short_cmp_percent)s, %(medium_cmp)s,
                            %(medium_att)s, %(medium_cmp_percent)s, %(long_cmp)s, %(long_att)s, %(long_cmp_percent)s, %(ast)s,
                            %(xag)s, %(xa)s, %(kp)s, %(final_1_3)s, %(ppa)s, %(crspa)s,
                            %(prgp)s, %(att)s, %(pass_types_live)s, %(pass_types_dead)s, %(pass_types_fk)s, %(pass_types_tb)s,
                            %(pass_types_sw)s, %(pass_types_crs)s, %(pass_types_ti)s, %(pass_types_ck)s, %(corner_kicks_in)s, %(corner_kicks_out)s,
                            %(corner_kicks_str)s, %(outcomes_cmp)s, %(outcomes_off)s, %(outcomes_blocks)s, %(tackles_tkl)s, %(tackles_tklw)s,
                            %(tackles_def_3rd)s, %(tackles_mid_3rd)s, %(tackles_att_3rd)s, %(challenges_tkl)s, %(challenges_att)s, %(challenges_tkl_percent)s,
                            %(challenges_lost)s, %(blocks_blocks)s, %(blocks_sh)s, %(blocks_pass)s, %(interceptions)s, %(tkl_plus_int)s,
                            %(clr)s, %(err)s, %(touches_touches)s, %(touches_def_pen)s, %(touches_def_3rd)s, %(touches_mid_3rd)s,
                            %(touches_att_3rd)s, %(touches_att_pen)s, %(touches_live)s, %(take_ons_succ_percent)s, %(take_ons_tkld)s, %(take_ons_tkld_percent)s,
                            %(carries_totdist)s, %(carries_prgdist)s, %(carries_1_3)s, %(carries_cpa)s, %(carries_mis)s, %(carries_dis)s,
                            %(receiving_rec)s, %(receiving_prgr)s, %(performance_2crdy)s, %(performance_fls)s, %(performance_fld)s, %(performance_off)s,
                            %(performance_crs)s, %(performance_tklw)s, %(performance_pkwon)s, %(performance_pkcon)s, %(performance_og)s, %(performance_recov)s,
                            %(aerial_duels_won)s, %(aerial_duels_lost)s, %(aerial_duels_won_percent)s);
                        """

                        if (
                            "Home" in match.get("Lineup", {})
                            and "Starting" in match["Lineup"][type_team]
                            and match["Lineup"][type_team]["Starting"]
                        ):
                            holder = (
                                1
                                if el in match["Lineup"][type_team]["Starting"].values()
                                else 0
                            )
                        else:
                            holder = None

                        values = {
                            "team_h": match.get("Teams", {}).get("Home", None),
                            "team_a": match.get("Teams", {}).get("Away", None),
                            "date_match": (
                                datetime.strptime(
                                    match.get("Date", ""), "%d-%m-%Y"
                                ).strftime("%Y-%m-%d")
                                if match.get("Date")
                                else None
                            ),
                            "name_player": el,
                            "team_player": (
                                match.get("Teams", {}).get("Home", None)
                                if type_team == "Home"
                                else match.get("Teams", {}).get("Away", None)
                            ),
                            "num": data_players.get("#", None),
                            "nation": data_players.get("Nation", None),
                            "pos": data_players.get("Pos", None),
                            "age_player": data_players.get("Age", None),
                            "holder": holder,
                            "min_played": data_players.get("Min", None),
                            "performance_gls": data_players.get(
                                "Performance_Gls", None
                            ),
                            "performance_ast": data_players.get(
                                "Performance_Ast", None
                            ),
                            "performance_pk": data_players.get("Performance_PK", None),
                            "performance_pkatt": data_players.get(
                                "Performance_PKatt", None
                            ),
                            "performance_sh": data_players.get("Performance_Sh", None),
                            "performance_sot": data_players.get(
                                "Performance_SoT", None
                            ),
                            "performance_crdy": data_players.get(
                                "Performance_CrdY", None
                            ),
                            "performance_crdr": data_players.get(
                                "Performance_CrdR", None
                            ),
                            "performance_touches": data_players.get(
                                "Performance_Touches", None
                            ),
                            "performance_tkl": data_players.get(
                                "Performance_Tkl", None
                            ),
                            "performance_int": data_players.get(
                                "Performance_Int", None
                            ),
                            "performance_blocks": data_players.get(
                                "Performance_Blocks", None
                            ),
                            "expected_xg": data_players.get("Expected_xG", None),
                            "expected_npxg": data_players.get("Expected_npxG", None),
                            "expected_xag": data_players.get("Expected_xAG", None),
                            "sca_sca": data_players.get("SCA_SCA", None),
                            "sca_gca": data_players.get("SCA_GCA", None),
                            "passes_cmp": data_players.get("Passes_Cmp", None),
                            "passes_att": data_players.get("Passes_Att", None),
                            "passes_cmp_percent": data_players.get("Passes_Cmp%", None),
                            "passes_prgp": data_players.get("Passes_PrgP", None),
                            "carries_carries": data_players.get(
                                "Carries_Carries", None
                            ),
                            "carries_prgc": data_players.get("Carries_PrgC", None),
                            "take_ons_att": data_players.get("Take-Ons_Att", None),
                            "take_ons_succ": data_players.get("Take-Ons_Succ", None),
                            "total_cmp": data_players.get("Total_Cmp", None),
                            "total_att": data_players.get("Total_Att", None),
                            "total_cmp_percent": data_players.get("Total_Cmp%", None),
                            "total_totdist": data_players.get("Total_TotDist", None),
                            "total_prgdist": data_players.get("Total_PrgDist", None),
                            "short_cmp": data_players.get("Short_Cmp", None),
                            "short_att": data_players.get("Short_Att", None),
                            "short_cmp_percent": data_players.get("Short_Cmp%", None),
                            "medium_cmp": data_players.get("Medium_Cmp", None),
                            "medium_att": data_players.get("Medium_Att", None),
                            "medium_cmp_percent": data_players.get("Medium_Cmp%", None),
                            "long_cmp": data_players.get("Long_Cmp", None),
                            "long_att": data_players.get("Long_Att", None),
                            "long_cmp_percent": data_players.get("Long_Cmp%", None),
                            "ast": data_players.get("Ast", None),
                            "xag": data_players.get("xAG", None),
                            "xa": data_players.get("xA", None),
                            "kp": data_players.get("KP", None),
                            "final_1_3": data_players.get("1/3", None),
                            "ppa": data_players.get("PPA", None),
                            "crspa": data_players.get("CrsPA", None),
                            "prgp": data_players.get("PrgP", None),
                            "att": data_players.get("Att", None),
                            "pass_types_live": data_players.get(
                                "Pass Types_Live", None
                            ),
                            "pass_types_dead": data_players.get(
                                "Pass Types_Dead", None
                            ),
                            "pass_types_fk": data_players.get("Pass Types_FK", None),
                            "pass_types_tb": data_players.get("Pass Types_TB", None),
                            "pass_types_sw": data_players.get("Pass Types_Sw", None),
                            "pass_types_crs": data_players.get("Pass Types_Crs", None),
                            "pass_types_ti": data_players.get("Pass Types_TI", None),
                            "pass_types_ck": data_players.get("Pass Types_CK", None),
                            "corner_kicks_in": data_players.get(
                                "Corner Kicks_In", None
                            ),
                            "corner_kicks_out": data_players.get(
                                "Corner Kicks_Out", None
                            ),
                            "corner_kicks_str": data_players.get(
                                "Corner Kicks_Str", None
                            ),
                            "outcomes_cmp": data_players.get("Outcomes_Cmp", None),
                            "outcomes_off": data_players.get("Outcomes_Off", None),
                            "outcomes_blocks": data_players.get(
                                "Outcomes_Blocks", None
                            ),
                            "tackles_tkl": data_players.get("Tackles_Tkl", None),
                            "tackles_tklw": data_players.get("Tackles_TklW", None),
                            "tackles_def_3rd": data_players.get(
                                "Tackles_Def 3rd", None
                            ),
                            "tackles_mid_3rd": data_players.get(
                                "Tackles_Mid 3rd", None
                            ),
                            "tackles_att_3rd": data_players.get(
                                "Tackles_Att 3rd", None
                            ),
                            "challenges_tkl": data_players.get("Challenges_Tkl", None),
                            "challenges_att": data_players.get("Challenges_Att", None),
                            "challenges_tkl_percent": data_players.get(
                                "Challenges_Tkl%", None
                            ),
                            "challenges_lost": data_players.get(
                                "Challenges_Lost", None
                            ),
                            "blocks_blocks": data_players.get("Blocks_Blocks", None),
                            "blocks_sh": data_players.get("Blocks_Sh", None),
                            "blocks_pass": data_players.get("Blocks_Pass", None),
                            "interceptions": data_players.get("Int", None),
                            "tkl_plus_int": data_players.get("Tkl+Int", None),
                            "clr": data_players.get("Clr", None),
                            "err": data_players.get("Err", None),
                            "touches_touches": data_players.get(
                                "Touches_Touches", None
                            ),
                            "touches_def_pen": data_players.get(
                                "Touches_Def Pen", None
                            ),
                            "touches_def_3rd": data_players.get(
                                "Touches_Def 3rd", None
                            ),
                            "touches_mid_3rd": data_players.get(
                                "Touches_Mid 3rd", None
                            ),
                            "touches_att_3rd": data_players.get(
                                "Touches_Att 3rd", None
                            ),
                            "touches_att_pen": data_players.get(
                                "Touches_Att Pen", None
                            ),
                            "touches_live": data_players.get("Touches_Live", None),
                            "take_ons_succ_percent": data_players.get(
                                "Take-Ons_Succ%", None
                            ),
                            "take_ons_tkld": data_players.get("Take-Ons_Tkld", None),
                            "take_ons_tkld_percent": data_players.get(
                                "Take-Ons_Tkld%", None
                            ),
                            "carries_totdist": data_players.get(
                                "Carries_TotDist", None
                            ),
                            "carries_prgdist": data_players.get(
                                "Carries_PrgDist", None
                            ),
                            "carries_1_3": data_players.get("Carries_1/3", None),
                            "carries_cpa": data_players.get("Carries_CPA", None),
                            "carries_mis": data_players.get("Carries_Mis", None),
                            "carries_dis": data_players.get("Carries_Dis", None),
                            "receiving_rec": data_players.get("Receiving_Rec", None),
                            "receiving_prgr": data_players.get("Receiving_PrgR", None),
                            "performance_2crdy": data_players.get(
                                "Performance_2CrdY", None
                            ),
                            "performance_fls": data_players.get(
                                "Performance_Fls", None
                            ),
                            "performance_fld": data_players.get(
                                "Performance_Fld", None
                            ),
                            "performance_off": data_players.get(
                                "Performance_Off", None
                            ),
                            "performance_crs": data_players.get(
                                "Performance_Crs", None
                            ),
                            "performance_tklw": data_players.get(
                                "Performance_TklW", None
                            ),
                            "performance_pkwon": data_players.get(
                                "Performance_PKwon", None
                            ),
                            "performance_pkcon": data_players.get(
                                "Performance_PKcon", None
                            ),
                            "performance_og": data_players.get("Performance_OG", None),
                            "performance_recov": data_players.get(
                                "Performance_Recov", None
                            ),
                            "aerial_duels_won": data_players.get(
                                "Aerial Duels_Won", None
                            ),
                            "aerial_duels_lost": data_players.get(
                                "Aerial Duels_Lost", None
                            ),
                            "aerial_duels_won_percent": data_players.get(
                                "Aerial Duels_Won%", None
                            ),
                        }

                        cursor.execute(req, values)

                        # Validation des changements
                        connection.commit()

    except Exception as e:
        print(f"Erreur: {e}")
        # Annulation des changements en cas d'erreur
        connection.rollback()


def insert_goalkeeper_psql(connection, match):
    """Insert data into goalkeeper table postgresql database"""
    try:
        if "Goalkeepers Statistics" in match:
            for type_team in ["Home", "Away"]:
                if type_team in match["Goalkeepers Statistics"]:
                    for el, data_gk in match["Goalkeepers Statistics"][
                        type_team
                    ].items():
                        with connection.cursor() as cursor:
                            req = """
                                INSERT INTO goalkeepers(team_h, team_a, date_match, name_goalkeeper, team_goalkeeper, min_played, shot_stopping_sota, shot_stopping_ga, 
                                shot_stopping_saves, shot_stopping_save_percent, shot_stopping_psxg, launched_cmp, launched_att, launched_cmp_percent, 
                                passes_att_gk, passes_thr, passes_launch_percent, passes_avg_len, goal_kicks_att, goal_kicks_launch_percent, goal_kicks_avg_len, 
                                crosses_opp, crosses_stp, crosses_stp_percent, sweeper_opa, sweeper_avg_dist)
                                
                                VALUES (%(team_h)s, %(team_a)s, %(date_match)s, %(name_goalkeeper)s, %(team_goalkeeper)s, %(min_played)s, %(shot_stopping_sota)s, %(shot_stopping_ga)s,
                                %(shot_stopping_saves)s, %(shot_stopping_save_percent)s, %(shot_stopping_psxg)s, %(launched_cmp)s, %(launched_att)s, %(launched_cmp_percent)s,
                                %(passes_att_gk)s, %(passes_thr)s, %(passes_launch_percent)s, %(passes_avg_len)s, %(goal_kicks_att)s, %(goal_kicks_launch_percent)s, 
                                %(goal_kicks_avg_len)s, %(crosses_opp)s, %(crosses_stp)s, %(crosses_stp_percent)s, %(sweeper_opa)s, %(sweeper_avg_dist)s
                                );
                            """

                            values = {
                                "team_h": match.get("Teams", {}).get("Home", None),
                                "team_a": match.get("Teams", {}).get("Away", None),
                                "date_match": (
                                    datetime.strptime(
                                        match.get("Date", ""), "%d-%m-%Y"
                                    ).strftime("%Y-%m-%d")
                                    if match.get("Date")
                                    else None
                                ),
                                "name_goalkeeper": el,
                                "team_goalkeeper": (
                                    match.get("Teams", {}).get("Home", None)
                                    if type_team == "Home"
                                    else match.get("Teams", {}).get("Away", None)
                                ),
                                "min_played": data_gk.get("Min", None),
                                "shot_stopping_sota": data_gk.get(
                                    "Shot Stopping_SoTA", None
                                ),
                                "shot_stopping_ga": data_gk.get(
                                    "Shot Stopping_GA", None
                                ),
                                "shot_stopping_saves": data_gk.get(
                                    "Shot Stopping_Saves", None
                                ),
                                "shot_stopping_save_percent": data_gk.get(
                                    "Shot Stopping_Save%", None
                                ),
                                "shot_stopping_psxg": data_gk.get(
                                    "Shot Stopping_PSxG", None
                                ),
                                "launched_cmp": data_gk.get("Launched_Cmp", None),
                                "launched_att": data_gk.get("Launched_Att", None),
                                "launched_cmp_percent": data_gk.get(
                                    "Launched_Cmp%", None
                                ),
                                "passes_att_gk": data_gk.get("Passes_Att (GK)", None),
                                "passes_thr": data_gk.get("Passes_Thr", None),
                                "passes_launch_percent": data_gk.get(
                                    "Passes_Launch%", None
                                ),
                                "passes_avg_len": data_gk.get("Passes_AvgLen", None),
                                "goal_kicks_att": data_gk.get("Goal Kicks_Att", None),
                                "goal_kicks_launch_percent": data_gk.get(
                                    "Goal Kicks_Launch%", None
                                ),
                                "goal_kicks_avg_len": data_gk.get(
                                    "Goal Kicks_AvgLen", None
                                ),
                                "crosses_opp": data_gk.get("Crosses_Opp", None),
                                "crosses_stp": data_gk.get("Crosses_Stp", None),
                                "crosses_stp_percent": data_gk.get(
                                    "Crosses_Stp%", None
                                ),
                                "sweeper_opa": data_gk.get("Sweeper_#OPA", None),
                                "sweeper_avg_dist": data_gk.get(
                                    "Sweeper_AvgDist", None
                                ),
                            }

                            cursor.execute(req, values)

                            # Validation des changements
                            connection.commit()

    except Exception as e:
        print(f"Erreur: {e}")
        # Annulation des changements en cas d'erreur
        connection.rollback()


def close_connection_postgresql(connection):
    """Close postgresql connexion"""
    # Fermer la connexion
    if connection:
        connection.close()
