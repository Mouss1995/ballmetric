CREATE TABLE "match_info" (
  "team_h" varchar NOT NULL,
  "team_a" varchar NOT NULL,
  "season" varchar NOT NULL,
  "goal_h" integer NOT NULL,
  "goal_a" integer NOT NULL,
  "competition" varchar NOT NULL,
  "date" date NOT NULL,
  "time" time,
  "round" varchar,
  "manager_h" varchar,
  "manager_a" varchar,
  "captain_h" varchar,
  "captain_a" varchar,
  "attendance" integer,
  "venue" varchar,
  "history_win_h" integer,
  "history_defeat_h" integer,
  "history_draw_h" integer,
  "history_win_a" integer,
  "history_defeat_a" integer,
  "history_draw_a" integer,
  "officials_referee" varchar,
  "ar1" varchar,
  "ar2" varchar,
  "4th" varchar,
  "var" varchar,
  "formation_h" varchar,
  "formation_a" varchar,
  "xg_a" float,
  "xg_h" float,
  "fouls_h" integer,
  "fouls_a" integer,
  "corners_h" integer,
  "corners_a" integer,
  "crosses_h" integer,
  "crosses_a" integer,
  "touches_h" integer,
  "touches_a" integer,
  "tackles_h" integer,
  "tackles_a" integer,
  "interceptions_h" integer,
  "interceptions_a" integer,
  "aerials_won_h" integer,
  "aerials_won_a" integer,
  "clearances_h" integer,
  "clearances_a" integer,
  "offsides_h" integer,
  "offsides_a" integer,
  "goal_kicks_h" integer,
  "goal_kicks_a" integer,
  "throw_ins_h" integer,
  "throw_ins_a" integer,
  "long_balls_h" integer,
  "long_balls_a" integer,
  "possession_h" integer,
  "possession_a" integer,
  "passing_accuracy_success_h" integer,
  "passing_accuracy_fail_h" integer,
  "passing_accuracy_success_a" integer,
  "passing_accuracy_fail_a" integer,
  "shots_target_success_h" integer,
  "shots_target_fail_h" integer,
  "shots_target_success_a" integer,
  "shots_target_fail_a" integer,
  "saves_success_h" integer,
  "saves_fail_h" integer,
  "saves_success_a" integer,
  "saves_fail_a" integer,
  "yellow_card_h" integer,
  "yellow_card_a" integer,
  "red_card_h" integer,
  "red_card_a" integer,
  PRIMARY KEY ("team_h", "team_a", "date")
);
