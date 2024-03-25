"""Function script in order to interact with database"""

from datetime import datetime

import psycopg2


def open_connection_postgresql():
    """Open connexion Postgresql"""
    db_params = {
        "dbname": "ballmetric",
        "user": "alancuzon",
        "password": "bilbao1995",
        # 'host': 'my_host',
        # 'port': 'my_port'
    }

    connection = psycopg2.connect(**db_params)

    return connection


def insert_matchs_psql(connection, match):
    """Insert data in postgresql database"""
    try:
        # Création d'un curseur
        with connection.cursor() as cursor:
            req = """
                INSERT INTO matchs(team_h, team_a, date_match, season, competition, time_match, matchweek, round, notes,
                manager_h, manager_a, captain_h, captain_a, attendance, venue, season_history_victory_h, season_history_draw_h,
                season_history_defeat_h, season_history_victory_a, season_history_draw_a, season_history_defeat_a, formation_h,
                formation_a, referee, ar1, ar2, fourth, var, penalties_h, penalties_a)
                VALUES (%(team_h)s, %(team_a)s, %(date_match)s, %(season)s, %(competition)s, %(time_match)s, %(matchweek)s, %(round)s, %(notes)s,
                %(manager_h)s, %(manager_a)s, %(captain_h)s, %(captain_a)s, %(attendance)s, %(venue)s, %(season_history_victory_h)s, %(season_history_draw_h)s,
                %(season_history_defeat_h)s, %(season_history_victory_a)s, %(season_history_draw_a)s, %(season_history_defeat_a)s, %(formation_h)s,
                %(formation_a)s, %(referee)s, %(ar1)s, %(ar2)s, %(fourth)s, %(var)s, %(penalties_h)s, %(penalties_a)s);
            """

            date_formatted = match.get("date", None)
            if date_formatted is not None:
                date_formatted = datetime.strptime(date_formatted, "%d-%m-%Y").strftime(
                    "%Y-%m-%d"
                )

            values = {
                "team_h": match.get("team_h", None),
                "team_a": match.get("team_a", None),
                "date_match": date_formatted,
                "season": match.get("season", None),
                "competition": match.get("competition", None),
                "time_match": match.get("time", None),
                "matchweek": match.get("matchweek", None),
                "round": match.get("round", None),
                "notes": match.get("notes", None),
                "manager_h": match.get("manager_h", None),
                "manager_a": match.get("manager_a", None),
                "captain_h": match.get("captain_h", None),
                "captain_a": match.get("captain_a", None),
                "attendance": match.get("attendance", None),
                "venue": match.get("venue", None),
                "season_history_victory_h": match.get("season_history_h", {}).get(
                    "victory", None
                ),
                "season_history_draw_h": match.get("season_history_h", {}).get(
                    "draw", None
                ),
                "season_history_defeat_h": match.get("season_history_h", {}).get(
                    "defeat", None
                ),
                "season_history_victory_a": match.get("season_history_a", {}).get(
                    "victory", None
                ),
                "season_history_draw_a": match.get("season_history_a", {}).get(
                    "draw", None
                ),
                "season_history_defeat_a": match.get("season_history_a", {}).get(
                    "defeat", None
                ),
                "formation_h": match.get("formation_h", None),
                "formation_a": match.get("formation_a", None),
                "referee": match.get("officials", {}).get("referee", None),
                "ar1": match.get("officials", {}).get("ar1", None),
                "ar2": match.get("officials", {}).get("ar2", None),
                "fourth": match.get("officials", {}).get("fourth", None),
                "var": match.get("officials", {}).get("var", None),
                "penalties_h": match.get("penalties_h", None),
                "penalties_a": match.get("penalties_a", None),
            }

            cursor.execute(req, values)
            connection.commit()

    except Exception:
        connection.rollback()
        raise


def insert_players_psql(connection, match):
    """Insert data into match table postgresql database"""
    try:
        if "players_stats" in match:
            for type_team in ["home", "away"]:
                for el, data_players in match["players_stats"][type_team].items():
                    with connection.cursor() as cursor:
                        req = """
                            INSERT INTO player_stats(player_name, player_team, date_match, team_h, team_a, num, nation,
                            club, pos, age, min_played, holder, performance_gls, performance_ast, performance_pk, performance_pkatt, performance_sh, performance_sot,
                            performance_crdy, performance_crdr, performance_touches, performance_tkl, performance_int, performance_blocks, expected_xg, expected_npxg,
                            expected_xag, sca_sca, sca_gca, passes_cmp, passes_att, passes_cmp_percent, passes_prgp, carries_carries, carries_prgc, take_ons_att,
                            take_ons_succ, total_cmp, total_att, total_cmp_percent, total_totdist, total_prgdist, short_cmp, short_att, short_cmp_percent, medium_cmp, medium_att,
                            medium_cmp_percent, long_cmp, long_att, long_cmp_percent, ast, xag, xa, kp, final_third, ppa, crspa, prgp, att, pass_types_live,
                            pass_types_dead, pass_types_fk, pass_types_tb, pass_types_sw, pass_types_crs, pass_types_ti, pass_types_ck, corner_kicks_in, corner_kicks_out,
                            corner_kicks_str, outcomes_cmp, outcomes_off, outcomes_blocks, tackles_tkl, tackles_tklw, tackles_def_3rd, tackles_mid_3rd, tackles_att_3rd,
                            challenges_tkl, challenges_att, challenges_tkl_percent, challenges_lost, blocks_blocks, blocks_sh, blocks_pass, interceptions, tkl_plus_int, 
                            clr, err, touches_touches, touches_def_pen, touches_def_3rd, touches_mid_3rd, touches_att_3rd, touches_att_pen, touches_live, take_ons_succ_percent,
                            take_ons_tkld, take_ons_tkld_percent, carries_totdist, carries_prgdist, carries_final_third, carries_cpa, carries_mis, carries_dis, receiving_rec,
                            receiving_prgr, performance_2crdy, performance_fls, performance_fld, performance_off, performance_crs, performance_tklw,
                            performance_pkwon, performance_pkcon, performance_og, performance_recov, aerial_duels_won, aerial_duels_lost, aerial_duels_won_percent)
                            VALUES (%(player_name)s, %(player_team)s, %(date_match)s, %(team_h)s, %(team_a)s, %(num)s, %(nation)s,
                            %(club)s, %(pos)s, %(age)s, %(min_played)s, %(holder)s, %(performance_gls)s, %(performance_ast)s,
                            %(performance_pk)s, %(performance_pkatt)s, %(performance_sh)s, %(performance_sot)s, %(performance_crdy)s,
                            %(performance_crdr)s, %(performance_touches)s, %(performance_tkl)s, %(performance_int)s, %(performance_blocks)s,
                            %(expected_xg)s, %(expected_npxg)s, %(expected_xag)s, %(sca_sca)s, %(sca_gca)s, %(passes_cmp)s, %(passes_att)s, %(passes_cmp_percent)s,
                            %(passes_prgp)s, %(carries_carries)s, %(carries_prgc)s, %(take_ons_att)s, %(take_ons_succ)s, %(total_cmp)s, %(total_att)s, 
                            %(total_cmp_percent)s, %(total_totdist)s, %(total_prgdist)s, %(short_cmp)s, %(short_att)s, %(short_cmp_percent)s, %(medium_cmp)s,
                            %(medium_att)s, %(medium_cmp_percent)s, %(long_cmp)s, %(long_att)s, %(long_cmp_percent)s, %(ast)s, %(xag)s, %(xa)s, %(kp)s,
                            %(final_third)s, %(ppa)s, %(crspa)s, %(prgp)s, %(att)s, %(pass_types_live)s, %(pass_types_dead)s, %(pass_types_fk)s,
                            %(pass_types_tb)s, %(pass_types_sw)s, %(pass_types_crs)s, %(pass_types_ti)s, %(pass_types_ck)s, %(corner_kicks_in)s,
                            %(corner_kicks_out)s, %(corner_kicks_str)s, %(outcomes_cmp)s, %(outcomes_off)s, %(outcomes_blocks)s, %(tackles_tkl)s,
                            %(tackles_tklw)s, %(tackles_def_3rd)s, %(tackles_mid_3rd)s, %(tackles_att_3rd)s, %(challenges_tkl)s, %(challenges_att)s,
                            %(challenges_tkl_percent)s, %(challenges_lost)s, %(blocks_blocks)s, %(blocks_sh)s, %(blocks_pass)s, %(interceptions)s,
                            %(tkl_plus_int)s, %(clr)s, %(err)s, %(touches_touches)s, %(touches_def_pen)s, %(touches_def_3rd)s, %(touches_mid_3rd)s,
                            %(touches_att_3rd)s, %(touches_att_pen)s, %(touches_live)s, %(take_ons_succ_percent)s, %(take_ons_tkld)s, %(take_ons_tkld_percent)s,
                            %(carries_totdist)s, %(carries_prgdist)s, %(carries_final_third)s, %(carries_cpa)s, %(carries_mis)s, %(carries_dis)s, %(receiving_rec)s,
                            %(receiving_prgr)s, %(performance_2crdy)s, %(performance_fls)s, %(performance_fld)s, %(performance_off)s, %(performance_crs)s,
                            %(performance_tklw)s, %(performance_pkwon)s, %(performance_pkcon)s, %(performance_og)s, %(performance_recov)s, %(aerial_duels_won)s,
                            %(aerial_duels_lost)s, %(aerial_duels_won_percent)s);
                        """
                        if type_team == "home":
                            lineup = match.get("lineup_h", None)
                        elif type_team == "away":
                            lineup = match.get("lineup_a", None)
                        else:
                            lineup = None

                        if lineup is not None and "Bench" in lineup:
                            bench = lineup.split("Bench")[1]
                            if el in bench:
                                holder = 0
                            else:
                                holder = 1
                        else:
                            holder = None

                        values = {
                            "team_h": match.get("team_h", None),
                            "team_a": match.get("team_a", None),
                            "date_match": (
                                datetime.strptime(
                                    match.get("date", ""), "%d-%m-%Y"
                                ).strftime("%Y-%m-%d")
                                if match.get("date")
                                else None
                            ),
                            "player_name": el,
                            "player_team": (
                                match.get("team_h", None)
                                if type_team == "home"
                                else match.get("team_a", None)
                            ),
                            "num": data_players.get("num", None),
                            "nation": data_players.get("nation", None),
                            "club": data_players.get("club", None),
                            "pos": data_players.get("pos", None),
                            "age": data_players.get("age", None),
                            "holder": holder,
                            "min_played": data_players.get("min", None),
                            "performance_gls": data_players.get(
                                "performance_gls", None
                            ),
                            "performance_ast": data_players.get(
                                "performance_ast", None
                            ),
                            "performance_pk": data_players.get("performance_pk", None),
                            "performance_pkatt": data_players.get(
                                "performance_pkatt", None
                            ),
                            "performance_sh": data_players.get("performance_sh", None),
                            "performance_sot": data_players.get(
                                "performance_sot", None
                            ),
                            "performance_crdy": data_players.get(
                                "performance_crdy", None
                            ),
                            "performance_crdr": data_players.get(
                                "performance_crdr", None
                            ),
                            "performance_touches": data_players.get(
                                "performance_touches", None
                            ),
                            "performance_tkl": data_players.get(
                                "performance_tkl", None
                            ),
                            "performance_int": data_players.get(
                                "performance_int", None
                            ),
                            "performance_blocks": data_players.get(
                                "performance_blocks", None
                            ),
                            "expected_xg": data_players.get("expected_xg", None),
                            "expected_npxg": data_players.get("expected_npxg", None),
                            "expected_xag": data_players.get("expected_xag", None),
                            "sca_sca": data_players.get("sca_sca", None),
                            "sca_gca": data_players.get("sca_gca", None),
                            "passes_cmp": data_players.get("passes_cmp", None),
                            "passes_att": data_players.get("passes_att", None),
                            "passes_cmp_percent": data_players.get(
                                "passes_cmp_percent", None
                            ),
                            "passes_prgp": data_players.get("passes_prgp", None),
                            "carries_carries": data_players.get(
                                "carries_carries", None
                            ),
                            "carries_prgc": data_players.get("carries_prgc", None),
                            "take_ons_att": data_players.get("take_ons_att", None),
                            "take_ons_succ": data_players.get("take_ons_succ", None),
                            "total_cmp": data_players.get("total_cmp", None),
                            "total_att": data_players.get("total_att", None),
                            "total_cmp_percent": data_players.get(
                                "total_cmp_percent", None
                            ),
                            "total_totdist": data_players.get("total_totdist", None),
                            "total_prgdist": data_players.get("total_prgdist", None),
                            "short_cmp": data_players.get("short_cmp", None),
                            "short_att": data_players.get("short_att", None),
                            "short_cmp_percent": data_players.get(
                                "short_cmp_percent", None
                            ),
                            "medium_cmp": data_players.get("medium_cmp", None),
                            "medium_att": data_players.get("medium_att", None),
                            "medium_cmp_percent": data_players.get(
                                "medium_cmp_percent", None
                            ),
                            "long_cmp": data_players.get("long_cmp", None),
                            "long_att": data_players.get("long_att", None),
                            "long_cmp_percent": data_players.get(
                                "long_cmp_percent", None
                            ),
                            "ast": data_players.get("ast", None),
                            "xag": data_players.get("xag", None),
                            "xa": data_players.get("xa", None),
                            "kp": data_players.get("kp", None),
                            "final_third": data_players.get("final_third", None),
                            "ppa": data_players.get("ppa", None),
                            "crspa": data_players.get("crspa", None),
                            "prgp": data_players.get("prgp", None),
                            "att": data_players.get("att", None),
                            "pass_types_live": data_players.get(
                                "pass_types_live", None
                            ),
                            "pass_types_dead": data_players.get(
                                "pass_types_dead", None
                            ),
                            "pass_types_fk": data_players.get("pass_types_fk", None),
                            "pass_types_tb": data_players.get("pass_types_tb", None),
                            "pass_types_sw": data_players.get("pass_types_sw", None),
                            "pass_types_crs": data_players.get("pass_types_crs", None),
                            "pass_types_ti": data_players.get("pass_types_ti", None),
                            "pass_types_ck": data_players.get("pass_types_ck", None),
                            "corner_kicks_in": data_players.get(
                                "corner_kicks_in", None
                            ),
                            "corner_kicks_out": data_players.get(
                                "corner_kicks_out", None
                            ),
                            "corner_kicks_str": data_players.get(
                                "corner_kicks_str", None
                            ),
                            "outcomes_cmp": data_players.get("outcomes_cmp", None),
                            "outcomes_off": data_players.get("outcomes_off", None),
                            "outcomes_blocks": data_players.get(
                                "outcomes_blocks", None
                            ),
                            "tackles_tkl": data_players.get("tackles_tkl", None),
                            "tackles_tklw": data_players.get("tackles_tklw", None),
                            "tackles_def_3rd": data_players.get(
                                "tackles_def_3rd", None
                            ),
                            "tackles_mid_3rd": data_players.get(
                                "tackles_mid_3rd", None
                            ),
                            "tackles_att_3rd": data_players.get(
                                "tackles_att_3rd", None
                            ),
                            "challenges_tkl": data_players.get("challenges_tkl", None),
                            "challenges_att": data_players.get("challenges_att", None),
                            "challenges_tkl_percent": data_players.get(
                                "challenges_tkl_percent", None
                            ),
                            "challenges_lost": data_players.get(
                                "challenges_lost", None
                            ),
                            "blocks_blocks": data_players.get("blocks_blocks", None),
                            "blocks_sh": data_players.get("blocks_sh", None),
                            "blocks_pass": data_players.get("blocks_pass", None),
                            "interceptions": data_players.get("int", None),
                            "tkl_plus_int": data_players.get("tkl_plus_int", None),
                            "clr": data_players.get("clr", None),
                            "err": data_players.get("err", None),
                            "touches_touches": data_players.get(
                                "touches_touches", None
                            ),
                            "touches_def_pen": data_players.get(
                                "touches_def_pen", None
                            ),
                            "touches_def_3rd": data_players.get(
                                "touches_def_3rd", None
                            ),
                            "touches_mid_3rd": data_players.get(
                                "touches_mid_3rd", None
                            ),
                            "touches_att_3rd": data_players.get(
                                "touches_att_3rd", None
                            ),
                            "touches_att_pen": data_players.get(
                                "touches_att_pen", None
                            ),
                            "touches_live": data_players.get("touches_live", None),
                            "take_ons_succ_percent": data_players.get(
                                "take_ons_succ_percent", None
                            ),
                            "take_ons_tkld": data_players.get("take_ons_tkld", None),
                            "take_ons_tkld_percent": data_players.get(
                                "take_ons_tkld_percent", None
                            ),
                            "carries_totdist": data_players.get(
                                "carries_totdist", None
                            ),
                            "carries_prgdist": data_players.get(
                                "carries_prgdist", None
                            ),
                            "carries_final_third": data_players.get(
                                "carries_final_third", None
                            ),
                            "carries_cpa": data_players.get("carries_cpa", None),
                            "carries_mis": data_players.get("carries_mis", None),
                            "carries_dis": data_players.get("carries_dis", None),
                            "receiving_rec": data_players.get("receiving_rec", None),
                            "receiving_prgr": data_players.get("receiving_prgr", None),
                            "performance_2crdy": data_players.get(
                                "performance_2crdy", None
                            ),
                            "performance_fls": data_players.get(
                                "performance_fls", None
                            ),
                            "performance_fld": data_players.get(
                                "performance_fld", None
                            ),
                            "performance_off": data_players.get(
                                "performance_off", None
                            ),
                            "performance_crs": data_players.get(
                                "performance_crs", None
                            ),
                            "performance_tklw": data_players.get(
                                "performance_tklw", None
                            ),
                            "performance_pkwon": data_players.get(
                                "performance_pkwon", None
                            ),
                            "performance_pkcon": data_players.get(
                                "performance_pkcon", None
                            ),
                            "performance_og": data_players.get("performance_og", None),
                            "performance_recov": data_players.get(
                                "performance_recov", None
                            ),
                            "aerial_duels_won": data_players.get(
                                "aerial_duels_won", None
                            ),
                            "aerial_duels_lost": data_players.get(
                                "aerial_duels_lost", None
                            ),
                            "aerial_duels_won_percent": data_players.get(
                                "aerial_duels_won_percent", None
                            ),
                        }

                        cursor.execute(req, values)
                        connection.commit()

    except Exception:
        connection.rollback()
        raise


def insert_goalkeeper_psql(connection, match):
    """Insert data into goalkeeper table postgresql database"""
    try:
        if "goalkeeper_stats" in match:
            for type_team in ["home", "away"]:
                if type_team in match["goalkeeper_stats"]:
                    for el, data_gk in match["goalkeeper_stats"][type_team].items():
                        with connection.cursor() as cursor:
                            req = """
                                INSERT INTO goalkeeper_stats(team_h, team_a, date_match, name_gk, team_gk, age_gk, nation, 
                                min_played, shot_stopping_sota, shot_stopping_ga, shot_stopping_saves, shot_stopping_save_percent,
                                shot_stopping_psxg, launched_cmp, launched_att, launched_cmp_percent, passes_att_gk, passes_thr, 
                                passes_launch_percent, passes_avglen, goal_kicks_att, goal_kicks_launch_percent, goal_kicks_avglen, 
                                crosses_opp, crosses_stp, crosses_stp_percent, sweeper_numopa, sweeper_avgdist)
                                VALUES (%(team_h)s, %(team_a)s, %(date_match)s, %(name_gk)s, %(team_gk)s, %(age_gk)s, %(nation)s,
                                %(min_played)s, %(shot_stopping_sota)s, %(shot_stopping_ga)s, %(shot_stopping_saves)s, %(shot_stopping_save_percent)s,
                                %(shot_stopping_psxg)s, %(launched_cmp)s, %(launched_att)s, %(launched_cmp_percent)s, %(passes_att_gk)s,
                                %(passes_thr)s, %(passes_launch_percent)s, %(passes_avglen)s, %(goal_kicks_att)s, %(goal_kicks_launch_percent)s,
                                %(goal_kicks_avglen)s, %(crosses_opp)s, %(crosses_stp)s, %(crosses_stp_percent)s, %(sweeper_numopa)s, %(sweeper_avgdist)s);
                            """

                            values = {
                                "team_h": match.get("team_h", None),
                                "team_a": match.get("team_a", None),
                                "date_match": (
                                    datetime.strptime(
                                        match.get("date", ""), "%d-%m-%Y"
                                    ).strftime("%Y-%m-%d")
                                    if match.get("date")
                                    else None
                                ),
                                "name_gk": el,
                                "team_gk": (
                                    match.get("team_h", None)
                                    if type_team == "home"
                                    else match.get("team_a", None)
                                ),
                                "age_gk": data_gk.get("age", None),
                                "min_played": data_gk.get("min", None),
                                "nation": data_gk.get("nation", None),
                                "shot_stopping_sota": data_gk.get(
                                    "shot_stopping_sota", None
                                ),
                                "shot_stopping_ga": data_gk.get(
                                    "shot_stopping_ga", None
                                ),
                                "shot_stopping_saves": data_gk.get(
                                    "shot_stopping_saves", None
                                ),
                                "shot_stopping_save_percent": data_gk.get(
                                    "shot_stopping_save_percent", None
                                ),
                                "shot_stopping_psxg": data_gk.get(
                                    "shot_stopping_psxg", None
                                ),
                                "launched_cmp": data_gk.get("launched_cmp", None),
                                "launched_att": data_gk.get("launched_att", None),
                                "launched_cmp_percent": data_gk.get(
                                    "launched_cmp_percent", None
                                ),
                                "passes_att_gk": data_gk.get("passes_att_gk", None),
                                "passes_thr": data_gk.get("passes_thr", None),
                                "passes_launch_percent": data_gk.get(
                                    "passes_launch_percent", None
                                ),
                                "passes_avglen": data_gk.get("passes_avglen", None),
                                "goal_kicks_att": data_gk.get("goal_kicks_att", None),
                                "goal_kicks_launch_percent": data_gk.get(
                                    "goal_kicks_launch_percent", None
                                ),
                                "goal_kicks_avglen": data_gk.get(
                                    "goal_kicks_avglen", None
                                ),
                                "crosses_opp": data_gk.get("crosses_opp", None),
                                "crosses_stp": data_gk.get("crosses_stp", None),
                                "crosses_stp_percent": data_gk.get(
                                    "crosses_stp_percent", None
                                ),
                                "sweeper_numopa": data_gk.get("sweeper_numopa", None),
                                "sweeper_avgdist": data_gk.get("sweeper_avgdist", None),
                            }

                            cursor.execute(req, values)
                            connection.commit()

    except Exception:
        connection.rollback()
        raise


def insert_shots_psql(connection, match):
    """Insert data into match table postgresql database"""
    try:
        if "shots" in match:
            lst_shots = []
            for el, data_shots in match["shots"].items():
                if isinstance(data_shots, list):
                    for el_list in data_shots:
                        el_list["minute_shot"] = el
                        lst_shots.append(el_list)
                else:
                    data_shots["minute_shot"] = el
                    lst_shots.append(data_shots)

            for el_shot in lst_shots:

                with connection.cursor() as cursor:
                    req = """
                        INSERT INTO shots(team_h, team_a, date_match, player, minute_shot, squad, xg, 
                        psxg, outcome, distance, penalty, body_part, notes, sca_1_player, sca_1_event, 
                        sca_2_player, sca_2_event)
                        VALUES (%(team_h)s, %(team_a)s, %(date_match)s, %(player)s, %(minute_shot)s, %(squad)s, %(xg)s, 
                        %(psxg)s, %(outcome)s, %(distance)s, %(penalty)s, %(body_part)s, %(notes)s, %(sca_1_player)s,
                        %(sca_1_event)s, %(sca_2_player)s, %(sca_2_event)s);
                    """

                    values = {
                        "team_h": match.get("team_h", None),
                        "team_a": match.get("team_a", None),
                        "date_match": (
                            datetime.strptime(
                                match.get("date", ""), "%d-%m-%Y"
                            ).strftime("%Y-%m-%d")
                            if match.get("date")
                            else None
                        ),
                        "player": (el_shot.get("player", None).replace(" (pen)", "")),
                        "minute_shot": el_shot.get("minute_shot", None),
                        "squad": el_shot.get("squad", None),
                        "xg": el_shot.get("xg", None),
                        "psxg": el_shot.get("psxg", None),
                        "outcome": el_shot.get("outcome", None),
                        "distance": el_shot.get("distance", None),
                        "penalty": 1 if " (pen)" in el_shot.get("player", None) else 0,
                        "body_part": el_shot.get("body_part", None),
                        "notes": el_shot.get("notes", None),
                        "sca_1_player": el_shot.get("sca_1_player", None),
                        "sca_1_event": el_shot.get("sca_1_event", None),
                        "sca_2_player": el_shot.get("sca_2_player", None),
                        "sca_2_event": el_shot.get("sca_2_event", None),
                    }
                    cursor.execute(req, values)
                    connection.commit()

    except Exception:
        connection.rollback()
        raise
