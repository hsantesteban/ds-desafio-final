create table dataset_a
(
	tour varchar(1),
	venue_location text,
	tournament text,
	match_date date not null,
	series text,
	court text,
	surface text,
	round text,
	best_of text,
	winner varchar(500) not null,
	loser varchar(500) not null,
	winner_rank integer,
	loser_rank integer,
	winner_games_first_set integer,
	loser_games_first_set integer,
	winner_games_second_set integer,
	loser_games_second_set integer,
	winner_games_third_set integer,
	loser_games_third_set integer,
	winner_games_fourth_set integer,
	loser_games_fourth_set integer,
	winner_games_fifth_set integer,
	loser_games_fifth_set integer,
	winner_sets_won integer,
	loser_sets_won integer,
	match_comment text,
	cb_odds_winner double precision,
	cb_odds_loser double precision,
	gb_odds_winner double precision,
	gb_odds_loser double precision,
	iw_odds_winner double precision,
	iw_odds_loser double precision,
	sb_odds_winner double precision,
	sb_odds_loser double precision,
	b3_odds_winner double precision,
	b3_odds_loser double precision,
	ex_odds_winner double precision,
	ex_odds_loser double precision,
	ps_odds_winner double precision,
	ps_odds_loser double precision,
	winner_entry_points integer,
	loser_entry_points integer,
	ub_odds_winner double precision,
	ub_odds_loser double precision,
	lb_odds_winner double precision,
	lb_odds_loser double precision,
	sj_odds_winner double precision,
	sj_odds_loser double precision,
	max_odds_winner double precision,
	max_odds_loser double precision,
	avg_odds_winner double precision,
	avg_odds_loser double precision,
	constraint dataset_a_pk
		unique (match_date, winner, loser)
);

alter table dataset_a owner to postgres;

create table dataset_b
(
	best_of integer,
	draw_size double precision,
	l_first_server_in_percent double precision,
	l_first_server_win_percent double precision,
	l_second_serve_win_percent double precision,
	l_games_served double precision,
	l_aces double precision,
	l_breakpoints_faced double precision,
	l_breakpoints_saved double precision,
	l_double_faults double precision,
	l_serve_percent double precision,
	loser_age double precision,
	loser_entry varchar(200),
	loser_hand varchar(200),
	loser_ht double precision,
	loser_id integer,
	loser_ioc varchar(200),
	loser_name varchar(200),
	loser_rank double precision,
	loser_rank_points double precision,
	loser_seed double precision,
	match_num integer,
	minutes double precision,
	round varchar(200),
	score varchar(200),
	surface varchar(200),
	tournament_date date,
	tournament_id varchar(200),
	tournament_level varchar(200),
	tournament_name varchar(200),
	w_first_serve_in double precision,
	w_first_serve_win double precision,
	w_second_serve_win double precision,
	w_games_served double precision,
	w_aces double precision,
	w_breakpoints_faced double precision,
	w_breakpoints_saved double precision,
	w_double_faults double precision,
	w_serve_percent double precision,
	winner_age double precision,
	winner_entry varchar(200),
	winner_hand varchar(200),
	winner_ht double precision,
	winner_id integer,
	winner_ioc varchar(200),
	winner_name varchar(200),
	winner_rank double precision,
	winner_rank_points double precision,
	winner_seed double precision
);

alter table dataset_b owner to postgres;

create table tournaments_master
(
	tournament_id varchar(200) not null
		constraint tournaments_master_pk
			primary key,
	season integer
);

alter table tournaments_master owner to postgres;

create table matches_master
(
	match_id serial not null
		constraint table_name_pk
			primary key,
	tournament_id varchar(200) not null,
	match_num integer not null,
	winner_games_set_one integer,
	loser_games_set_one integer,
	winner_games_set_two integer,
	loser_games_set_two integer,
	winner_games_set_three integer,
	loser_games_set_three integer,
	winner_games_set_four integer,
	loser_games_set_four integer,
	winner_games_set_five integer,
	loser_games_set_five integer,
	winner_sets_won integer,
	winner_sets_lost integer,
	loser_sets_won integer,
	loser_sets_lost integer
);

alter table matches_master owner to postgres;

create table matches_statistics
(
	tournament_id varchar(200) not null,
	match_num integer not null,
	winner_games_set_one integer,
	loser_games_set_one integer,
	winner_games_set_two integer,
	loser_games_set_two integer,
	winner_games_set_three integer,
	loser_games_set_three integer,
	winner_games_set_four integer,
	loser_games_set_four integer,
	winner_games_set_five integer,
	loser_games_set_five integer,
	winner_games_won integer,
	winner_games_lost integer,
	loser_games_won integer,
	loser_games_lost integer,
	winner_sets_won integer,
	winner_sets_lost integer,
	loser_sets_won integer,
	loser_sets_lost integer,
	constraint matches_statistics_pk
		primary key (tournament_id, match_num)
);

alter table matches_statistics owner to postgres;

create table player_features
(
	player_id integer not null,
	player_age double precision,
	player_hand varchar(1),
	player_rank integer,
	tournament_id varchar(200) not null,
	tournament_match_num integer not null,
	tournament_match_result varchar(1) not null,
	tournament_match_surface varchar(200),
	tournament_sets_won integer,
	tournament_sets_lost integer,
	tournament_sets_played integer,
	tournament_games_won integer,
	tournament_games_lost integer,
	tournament_games_played integer,
	tournament_player_seed integer,
	tournament_minutes_played integer,
	season_sets_won integer,
	season_sets_lost integer,
	season_sets_played integer,
	season_games_won integer,
	season_games_lost integer,
	season_games_played integer,
	season_matches_won integer,
	season_matches_lost integer,
	season_matches_played integer,
	season_tournaments_won integer,
	season_tournaments_lost integer,
	season_tournaments_played integer,
	season_tournaments_finals integer,
	season_tournaments_semifinals integer,
	season_surface_matches_won integer,
	season_surface_matches_lost integer,
	season_surface_matches_played integer,
	career_surface_matches_won integer,
	career_surface_matches_lost integer,
	career_surface_matches_played integer,
	career_surface_tournaments_won integer,
	career_surface_tournaments_lost integer,
	career_surface_tournaments_played integer,
	career_matches_won integer,
	career_matches_lost integer,
	career_matches_played integer,
	career_tournaments_won integer,
	career_tournaments_lost integer,
	career_tournaments_played integer,
	career_right_handed_won integer,
	career_right_handed_lost integer,
	career_left_handed_won integer,
	career_left_handed_lost integer,
	constraint player_features_pk
		primary key (player_id, tournament_id, tournament_match_num)
);

alter table player_features owner to postgres;

