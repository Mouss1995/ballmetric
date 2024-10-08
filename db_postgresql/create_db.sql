DROP TABLE IF EXISTS player_stats;
DROP TABLE IF EXISTS goalkeeper_stats;
DROP TABLE IF EXISTS shots;
DROP TABLE IF EXISTS matchs;

CREATE TABLE matchs (
    team_h VARCHAR NOT NULL,
    team_a VARCHAR NOT NULL,
    date_match DATE NOT NULL,
    season VARCHAR,
    competition VARCHAR,
    time_match TIME,
    matchweek INT, 
    round VARCHAR,
    notes VARCHAR,
    manager_h VARCHAR,
    manager_a VARCHAR,
    captain_h VARCHAR,
    captain_a VARCHAR,
    attendance INT,
    venue VARCHAR,
    formation_home VARCHAR,
    formation_away VARCHAR,
    referee VARCHAR,
    ar1 VARCHAR,
    ar2 VARCHAR,
    fourth VARCHAR,
    var VARCHAR,
    penalties_h INT,
    penalties_a INT,
    PRIMARY KEY (team_h, team_a, date_match)
);

CREATE TABLE player_stats (
    id_player INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    player VARCHAR NOT NULL,
    player_team VARCHAR NOT NULL,
    date_match DATE NOT NULL,
    team_h VARCHAR NOT NULL,
    team_a VARCHAR NOT NULL,
    num INT,
    nation VARCHAR,
    club VARCHAR,
    pos VARCHAR,
    age VARCHAR,
    min INT,
    holder INT,
    performance_gls INT,
    performance_ast INT,
    performance_pk INT,
    performance_pkatt INT,
    performance_sh INT,
    performance_sot INT,
    performance_crdy INT,
    performance_crdr INT,
    performance_touches INT,
    performance_tkl INT,
    performance_int INT,
    performance_blocks INT,
    expected_xg FLOAT,
    expected_npxg FLOAT,
    expected_xag FLOAT,
    sca_sca INT,
    sca_gca INT, 
    passes_cmp INT,
    passes_att INT,
    passes_cmp_percent FLOAT,
    passes_prgp INT, 
    carries_carries INT,
    carries_prgc INT,
    take_ons_att INT,
    take_ons_succ INT,
    total_cmp INT,
    total_att INT,
    total_cmp_percent FLOAT,
    total_totdist INT,
    total_prgdist INT,
    short_cmp INT,
    short_att INT,
    short_cmp_percent FLOAT,
    medium_cmp INT,
    medium_att INT,
    medium_cmp_percent FLOAT,
    long_cmp INT,
    long_att INT,
    long_cmp_percent FLOAT,
    ast INT,
    xag FLOAT,
    xa FLOAT,
    kp INT,
    final_third INT,
    ppa INT,
    crspa INT,
    prgp INT,
    att INT,
    pass_types_live INT,
    pass_types_dead INT,
    pass_types_fk INT,
    pass_types_tb INT,
    pass_types_sw INT,
    pass_types_crs INT,
    pass_types_ti INT,
    pass_types_ck INT,
    corner_kicks_in INT,
    corner_kicks_out INT,
    corner_kicks_str INT,
    outcomes_cmp INT,
    outcomes_off INT,
    outcomes_blocks INT,
    tackles_tkl INT,
    tackles_tklw INT,
    tackles_def_3rd INT,
    tackles_mid_3rd INT,
    tackles_att_3rd INT,
    challenges_tkl INT,
    challenges_att INT,
    challenges_tkl_percent FLOAT,
    challenges_lost INT,
    blocks_blocks INT,
    blocks_sh INT,
    blocks_pass INT,
    int INT,
    tkl_plus_int INT,
    clr INT,
    err INT,
    touches_touches INT,
    touches_def_pen INT,
    touches_def_3rd INT,
    touches_mid_3rd INT,
    touches_att_3rd INT,
    touches_att_pen INT,
    touches_live INT,
    take_ons_succ_percent FLOAT,
    take_ons_tkld INT,
    take_ons_tkld_percent FLOAT,
    carries_totdist INT,
    carries_prgdist INT,
    carries_final_third INT,
    carries_cpa INT,
    carries_mis INT,
    carries_dis INT,
    receiving_rec INT,
    receiving_prgr INT,
    performance_2crdy INT,
    performance_fls INT,
    performance_fld INT,
    performance_off INT,
    performance_crs INT,
    performance_tklw INT,
    performance_pkwon INT,
    performance_pkcon INT,
    performance_og INT,
    performance_recov INT,
    aerial_duels_won INT,
    aerial_duels_lost INT,
    aerial_duels_won_percent FLOAT,
    FOREIGN KEY (team_h, team_a, date_match) REFERENCES matchs(team_h, team_a, date_match)
);

CREATE TABLE goalkeeper_stats (
    id_gk INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    team_h VARCHAR NOT NULL,
    team_a VARCHAR NOT NULL,
    date_match DATE NOT NULL,
    player VARCHAR NOT NULL,
    player_team VARCHAR NOT NULL,
    age VARCHAR,
    nation VARCHAR,
    min INT,
    shot_stopping_sota INT,
    shot_stopping_ga INT,
    shot_stopping_saves INT,
    shot_stopping_save_percent FLOAT,
    shot_stopping_psxg FLOAT,
    launched_cmp INT,
    launched_att INT,
    launched_cmp_percent FLOAT,
    passes_att_gk INT,
    passes_thr INT,
    passes_launch_percent FLOAT,
    passes_avglen FLOAT,
    goal_kicks_att INT,
    goal_kicks_launch_percent FLOAT,
    goal_kicks_avglen FLOAT,
    crosses_opp INT,
    crosses_stp INT,
    crosses_stp_percent FLOAT,
    sweeper_numopa INT,
    sweeper_avgdist FLOAT,
    FOREIGN KEY (team_h, team_a, date_match) REFERENCES matchs(team_h, team_a, date_match)
);

CREATE TABLE shots (
    id_shot INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    team_h VARCHAR NOT NULL,
    team_a VARCHAR NOT NULL,
    date_match DATE NOT NULL,
    player VARCHAR,
    minute VARCHAR NOT NULL,
    squad VARCHAR,
    xg FLOAT,
    psxg FLOAT,
    outcome VARCHAR,
    distance FLOAT,
    body_part VARCHAR,
    notes VARCHAR,
    sca_1_player VARCHAR,
    sca_1_event VARCHAR,
    sca_2_player VARCHAR,
    sca_2_event VARCHAR,
    FOREIGN KEY (team_h, team_a, date_match) REFERENCES matchs(team_h, team_a, date_match)
);
