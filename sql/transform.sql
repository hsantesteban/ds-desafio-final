
-- tournament matches master table.
INSERT INTO matches_master (tournament_id, match_num)
     SELECT tournament_id, match_num FROM dataset_b
   ORDER BY tournament_date, tournament_id, match_num;

TRUNCATE TABLE player_features;
INSERT INTO player_features (player_id, tournament_id, tournament_match_num, tournament_match_result,
                             player_hand, player_rank, tournament_match_surface, tournament_player_seed, player_age)
    SELECT winner_id, tournament_id, match_num, 'W', winner_hand, winner_rank, surface, winner_seed, winner_age
      FROM dataset_b
     UNION ALL
    SELECT loser_id, tournament_id, match_num, 'L', loser_hand, loser_rank, surface, loser_seed, loser_age
      FROM dataset_b;

-- tournament statistics.
UPDATE player_features p SET
   tournament_minutes_played = data.tournament_minutes_played,
   tournament_sets_played = data.tournament_sets_played,
   tournament_sets_won = data.tournament_sets_won,
   tournament_sets_lost = data.tournament_sets_lost,
   tournament_games_played = data.tournament_games_played,
   tournament_games_won = data.tournament_games_won,
   tournament_games_lost = data.tournament_games_lost
FROM (
    SELECT b.player_id, b.tournament_id, b.tournament_match_num, match_id, b.tournament_round,
           Sum(coalesce(tournament_minutes_played, 0)) OVER w as tournament_minutes_played,
           -- sets.
           Sum(coalesce(b.match_sets_played, 0)) OVER w as tournament_sets_played,
           Sum(coalesce(b.match_sets_won, 0)) OVER w as tournament_sets_won,
           Sum(coalesce(b.match_sets_lost, 0)) OVER w as tournament_sets_lost,
           -- games
           Sum(coalesce(b.match_games_played, 0)) OVER w as tournament_games_played,
           Sum(coalesce(b.match_games_won, 0)) OVER w as tournament_games_won,
           Sum(coalesce(b.match_games_lost, 0)) OVER w as tournament_games_lost
           FROM (
               -- inner window calls.
                SELECT b.player_id as player_id,
                       b.tournament_id as tournament_id,
                       b.tournament_match_num as tournament_match_num,
                       mm.match_id as match_id,
                       b.tournament_round as tournament_round,
                       lag(match_minutes_played, 1) OVER w as tournament_minutes_played,
                       -- games.
                       lag(b.match_games_played, 1) OVER w as match_games_played,
                       lag(b.match_games_won, 1) OVER w as match_games_won,
                       lag(b.match_games_lost, 1) OVER w as match_games_lost,
                       -- sets.
                       lag(b.match_sets_won, 1) OVER w as match_sets_won,
                       lag(b.match_sets_lost, 1) OVER w as match_sets_lost,
                       lag(b.match_sets_played, 1) OVER w as match_sets_played
                  FROM (
                        SELECT b.winner_id as player_id,
                               b.tournament_id as tournament_id,
                               b.match_num as tournament_match_num,
                               b.round as tournament_round,
                               b.surface as tournament_surface,
                               coalesce(minutes, 0) as match_minutes_played,
                               coalesce(s.winner_sets_won, 0) as match_sets_won,
                               coalesce(s.winner_sets_lost, 0) as match_sets_lost,
                               coalesce(s.winner_sets_won, 0) + coalesce(s.winner_sets_lost, 0) as match_sets_played,
                               coalesce(s.winner_games_won, 0) + coalesce(winner_games_lost, 0) as match_games_played,
                               coalesce(s.winner_games_won, 0) as match_games_won,
                               coalesce(s.winner_games_lost, 0) as match_games_lost
                          FROM dataset_b AS b
                         INNER JOIN matches_statistics s
                            ON b.tournament_id = s.tournament_id
                           AND b.match_num = s.match_num
                         UNION ALL
                        SELECT b.loser_id as player_id,
                               b.tournament_id as tournament_id,
                               b.match_num as tournament_match_num,
                               b.round as tournament_round,
                               b.surface as tournament_surface,
                               coalesce(minutes, 0) as match_minutes_played,
                               coalesce(s.loser_sets_won, 0) as match_sets_won,
                               coalesce(s.loser_sets_lost, 0) as match_sets_lost,
                               coalesce(s.loser_sets_lost, 0) + coalesce(s.loser_sets_won, 0) as match_sets_played,
                               coalesce(s.loser_games_won, 0) + coalesce(loser_games_lost, 0) as match_games_played,
                               coalesce(s.loser_games_won, 0) as match_games_won,
                               coalesce(s.loser_games_lost, 0) as match_games_lost
                          FROM dataset_b AS b
                         INNER JOIN matches_statistics s
                            ON b.tournament_id = s.tournament_id
                           AND b.match_num = s.match_num
                   ) b
                 INNER JOIN matches_master mm
                    ON mm.tournament_id = b.tournament_id
                   AND mm.match_num = b.tournament_match_num
                WINDOW w AS (PARTITION BY b.player_id, b.tournament_id ORDER BY mm.match_id)
    ) b WINDOW w AS (PARTITION BY b.player_id, b.tournament_id ORDER BY b.match_id)
) data
  WHERE p.player_id = data.player_id
    AND p.tournament_id = data.tournament_id
    AND p.tournament_match_num = data.tournament_match_num;

-- season statistics.
UPDATE player_features p SET
    season_sets_lost = s.season_sets_lost,
    season_sets_won = s.season_sets_won,
    season_sets_played = s.season_sets_played,
    season_games_played = s.season_games_played,
    season_games_won = s.season_games_won,
    season_games_lost = s.season_games_lost,
    season_matches_won = s.season_matches_won,
    season_matches_lost = s.season_matches_lost,
    season_matches_played = s.season_matches_played,
    season_tournaments_won = s.season_tournaments_won,
    season_tournaments_lost = s.season_tournaments_lost,
    season_tournaments_played = s.season_tournament_played,
    season_tournaments_finals = s.season_finals_played,
    season_tournaments_semifinals = s.season_semifinals_played,
    season_surface_matches_played = CASE WHEN s.tournament_surface = 'Clay' THEN s.season_surface_clay_matches_won + s.season_surface_clay_matches_lost
                                         WHEN s.tournament_surface = 'Hard' THEN s.season_surface_hard_matches_won + s.season_surface_hard_matches_lost
                                         WHEN s.tournament_surface = 'Carpet' THEN s.season_surface_carpet_matches_won + s.season_surface_carpet_matches_lost
                                         WHEN s.tournament_surface = 'Grass' THEN s.season_surface_grass_matches_won + s.season_surface_grass_matches_lost
                                     END,
    season_surface_matches_won = CASE WHEN s.tournament_surface = 'Clay' THEN s.season_surface_clay_matches_won
                                      WHEN s.tournament_surface = 'Hard' THEN s.season_surface_hard_matches_won
                                      WHEN s.tournament_surface = 'Carpet' THEN s.season_surface_carpet_matches_won
                                      WHEN s.tournament_surface = 'Grass' THEN s.season_surface_grass_matches_won
                                  END,
    season_surface_matches_lost = CASE WHEN s.tournament_surface = 'Clay' THEN s.season_surface_clay_matches_lost
                                       WHEN s.tournament_surface = 'Hard' THEN s.season_surface_hard_matches_lost
                                       WHEN s.tournament_surface = 'Carpet' THEN s.season_surface_carpet_matches_lost
                                       WHEN s.tournament_surface = 'Grass' THEN s.season_surface_grass_matches_lost
                                   END
FROM (
    SELECT b.player_id,
           b.tournament_id,
           b.tournament_match_num,
           b.match_id,
           b.tournament_round,
           b.tournament_surface,
           -- sets.
           Sum(coalesce(b.match_sets_played, 0)) OVER w as season_sets_played,
           Sum(coalesce(b.match_sets_won, 0)) OVER w as season_sets_won,
           Sum(coalesce(b.match_sets_lost, 0)) OVER w as season_sets_lost,
           -- games
           Sum(coalesce(b.match_games_played, 0)) OVER w as season_games_played,
           Sum(coalesce(b.match_games_won, 0)) OVER w as season_games_won,
           Sum(coalesce(b.match_games_lost, 0)) OVER w as season_games_lost,
           -- minutes
           Sum(coalesce(b.minutes_played, 0)) OVER w as season_minutes_played,
           -- matches.
           Sum(coalesce(b.win_count, 0)) OVER w as season_matches_won,
           Sum(coalesce(b.lose_count, 0)) OVER w as season_matches_lost,
           Sum(coalesce(b.win_count, 0) + coalesce(b.lose_count, 0)) OVER w as season_matches_played,
           -- tournaments.
           Sum(coalesce(b.won_final_flag, 0)) OVER w as season_tournaments_won,
           Sum(coalesce(b.tournament_played_flag, 0) - coalesce(b.won_final_flag, 0)) OVER w as season_tournaments_lost,
           Sum(coalesce(b.tournament_played_flag, 0)) OVER w as season_tournament_played,
           -- tournament stages.
           Sum(coalesce(b.final_flag, 0)) OVER w as season_finals_played,
           Sum(coalesce(b.semfinal_flag, 0)) OVER w as season_semifinals_played,
           -- surface
           Sum(CASE WHEN b.tournament_surface = 'Clay' THEN coalesce(b.win_count, 0) ELSE 0 END) OVER w as season_surface_clay_matches_won,
           Sum(CASE WHEN b.tournament_surface = 'Clay' THEN coalesce(b.lose_count, 0) ELSE 0 END) OVER w as season_surface_clay_matches_lost,
           Sum(CASE WHEN b.tournament_surface = 'Carpet' THEN coalesce(b.win_count, 0) ELSE 0 END) OVER w as season_surface_carpet_matches_won,
           Sum(CASE WHEN b.tournament_surface = 'Carpet' THEN coalesce(b.lose_count, 0) ELSE 0 END) OVER w as season_surface_carpet_matches_lost,
           Sum(CASE WHEN b.tournament_surface = 'Hard' THEN coalesce(b.win_count, 0) ELSE 0 END) OVER w as season_surface_hard_matches_won,
           Sum(CASE WHEN b.tournament_surface = 'Hard' THEN coalesce(b.lose_count, 0) ELSE 0 END) OVER w as season_surface_hard_matches_lost,
           Sum(CASE WHEN b.tournament_surface = 'Grass' THEN coalesce(b.win_count, 0) ELSE 0 END) OVER w as season_surface_grass_matches_won,
           Sum(CASE WHEN b.tournament_surface = 'Grass' THEN coalesce(b.lose_count, 0) ELSE 0 END) OVER w as season_surface_grass_matches_lost
           FROM (
               -- inner lagged window calls.
                SELECT b.player_id,
                       b.tournament_id,
                       b.tournament_match_num,
                       mm.match_id as match_id,
                       b.tournament_round,
                       tm.season as season,
                       b.tournament_surface,
                       -- games.
                       lag(b.match_games_played, 1) OVER w as match_games_played,
                       lag(b.match_games_won, 1) OVER w as match_games_won,
                       lag(b.match_games_lost, 1) OVER w as match_games_lost,
                       -- sets.
                       lag(b.match_sets_won, 1) OVER w as match_sets_won,
                       lag(b.match_sets_lost, 1) OVER w as match_sets_lost,
                       lag(b.match_sets_played, 1) OVER w as match_sets_played,
                       lag(coalesce(b.match_minutes_played, 0), 1) OVER w as minutes_played,
                       lag(win_count, 1) over w as win_count,
                       lag(lose_count, 1) over w as lose_count,
                       lag(won_final_flag, 1) over w as won_final_flag,
                       lag(final_flag, 1) over w as final_flag,
                       lag(semfinal_flag, 1) over w as semfinal_flag,
                       lag(tournament_played_flag, 1) over w as tournament_played_flag
                  FROM (
                        SELECT b.winner_id as player_id,
                               b.tournament_id as tournament_id,
                               b.match_num as tournament_match_num,
                               b.round as tournament_round,
                               b.surface as tournament_surface,
                               coalesce(minutes, 0) as match_minutes_played,
                               1 as win_count,
                               0 as lose_count,
                               CASE WHEN b.round = 'F' THEN 1 ELSE 0 END as won_final_flag,
                               CASE WHEN b.round = 'F' THEN 1 ELSE 0 END as final_flag,
                               CASE WHEN b.round = 'SF' THEN 1 ELSE 0 END as semfinal_flag,
                               CASE WHEN b.round = 'F' THEN 1 ELSE 0 END tournament_played_flag,
                               coalesce(s.winner_sets_won, 0) as match_sets_won,
                               coalesce(s.winner_sets_lost, 0) as match_sets_lost,
                               coalesce(s.winner_sets_won, 0) + coalesce(s.winner_sets_lost, 0) as match_sets_played,
                               coalesce(s.winner_games_won, 0) + coalesce(winner_games_lost, 0) as match_games_played,
                               coalesce(s.winner_games_won, 0) as match_games_won,
                               coalesce(s.winner_games_lost, 0) as match_games_lost
                          FROM dataset_b AS b
                          LEFT JOIN matches_statistics s
                            ON b.tournament_id = s.tournament_id
                           AND b.match_num = s.match_num
                         UNION ALL
                        SELECT b.loser_id as player_id,
                               b.tournament_id as tournament_id,
                               b.match_num as tournament_match_num,
                               b.round as tournament_round,
                               b.surface as tournament_surface,
                               coalesce(minutes, 0) as match_minutes_played,
                               0 as win_count,
                               1 as lose_count,
                               0 as won_final_flag,
                               CASE WHEN b.round = 'F'  THEN 1 ELSE 0 END as final_flag,
                               CASE WHEN b.round = 'SF' THEN 1 ELSE 0 END as semfinal_flag,
                               1 as tournament_played_flag,
                               coalesce(s.loser_sets_won, 0) as match_sets_won,
                               coalesce(s.loser_sets_lost, 0) as match_sets_lost,
                               coalesce(s.loser_sets_lost, 0) + coalesce(s.loser_sets_won, 0) as match_sets_played,
                               coalesce(s.loser_games_won, 0) + coalesce(loser_games_lost, 0) as match_games_played,
                               coalesce(s.loser_games_won, 0) as match_games_won,
                               coalesce(s.loser_games_lost, 0) as match_games_lost
                          FROM dataset_b AS b
                          LEFT JOIN matches_statistics s
                            ON b.tournament_id = s.tournament_id
                           AND b.match_num = s.match_num
                  ) as b
                 INNER JOIN matches_master mm
                    ON mm.tournament_id = b.tournament_id
                   AND mm.match_num = b.tournament_match_num
                 INNER JOIN tournaments_master tm
                    ON tm.tournament_id = b.tournament_id
                WINDOW w AS (PARTITION BY tm.season, b.player_id ORDER BY mm.match_id)
    ) b
        WINDOW w AS (PARTITION BY b.season, b.player_id ORDER BY b.match_id)
         ORDER BY player_id, match_id
) s
  WHERE p.player_id = s.player_id
    AND p.tournament_id = s.tournament_id
    AND p.tournament_match_num = s.tournament_match_num;

-- career statistics.
UPDATE player_features p SET
    career_matches_lost = c.career_matches_lost,
    career_matches_won = c.career_matches_won,
    career_matches_played = c.career_matches_played,
    career_tournaments_won = c.career_tournaments_won,
    career_tournaments_lost = c.career_tournaments_lost,
    career_tournaments_played = c.career_tournament_played,
    career_left_handed_won = c.career_left_hand_matches_won,
    career_left_handed_lost = c.career_left_hand_matches_lost,
    career_right_handed_won = c.career_right_hand_matches_won,
    career_right_handed_lost = c.career_right_hand_matches_lost,
    -- matches surface.
    career_surface_matches_won = CASE WHEN c.tournament_surface = 'Clay' THEN c.career_surface_clay_matches_won
                                      WHEN c.tournament_surface = 'Hard' THEN c.career_surface_hard_matches_won
                                      WHEN c.tournament_surface = 'Carpet' THEN c.career_surface_carpet_matches_won
                                      WHEN c.tournament_surface = 'Grass' THEN c.career_surface_grass_matches_won
                                  END,
    career_surface_matches_lost = CASE WHEN c.tournament_surface = 'Clay' THEN c.career_surface_clay_matches_lost
                                       WHEN c.tournament_surface = 'Hard' THEN c.career_surface_hard_matches_lost
                                       WHEN c.tournament_surface = 'Carpet' THEN c.career_surface_carpet_matches_lost
                                       WHEN c.tournament_surface = 'Grass' THEN c.career_surface_grass_matches_lost
                                  END,
    career_surface_matches_played = CASE WHEN c.tournament_surface = 'Clay' THEN c.career_surface_clay_matches_lost + c.career_surface_clay_matches_won
                                       WHEN c.tournament_surface = 'Hard' THEN c.career_surface_hard_matches_lost + c.career_surface_hard_matches_won
                                       WHEN c.tournament_surface = 'Carpet' THEN c.career_surface_carpet_matches_lost + c.career_surface_carpet_matches_won
                                       WHEN c.tournament_surface = 'Grass' THEN c.career_surface_grass_matches_lost + c.career_surface_grass_matches_won
                                  END,
    career_surface_tournaments_won = CASE
                                       WHEN c.tournament_surface = 'Clay' THEN c.career_surface_clay_tournaments_won
                                       WHEN c.tournament_surface = 'Hard' THEN c.career_surface_carpet_tournaments_won
                                       WHEN c.tournament_surface = 'Carpet' THEN c.career_surface_hard_tournaments_won
                                       WHEN c.tournament_surface = 'Grass' THEN c.career_surface_grass_tournaments_won
                                    END,
    career_surface_tournaments_lost = CASE
                                       WHEN c.tournament_surface = 'Clay' THEN c.career_surface_clay_tournaments_lost
                                       WHEN c.tournament_surface = 'Hard' THEN c.career_surface_carpet_tournaments_lost
                                       WHEN c.tournament_surface = 'Carpet' THEN c.career_surface_hard_tournaments_lost
                                       WHEN c.tournament_surface = 'Grass' THEN c.career_surface_grass_tournaments_lost
                                    END,
    career_surface_tournaments_played = CASE
                                       WHEN c.tournament_surface = 'Clay' THEN c.career_surface_clay_tournaments_played
                                       WHEN c.tournament_surface = 'Hard' THEN c.career_surface_carpet_tournaments_played
                                       WHEN c.tournament_surface = 'Carpet' THEN c.career_surface_hard_tournaments_played
                                       WHEN c.tournament_surface = 'Grass' THEN c.career_surface_grass_tournaments_played
                                    END
FROM (
    SELECT b.player_id,
           b.tournament_id,
           b.tournament_match_num,
           b.match_id,
           b.tournament_round,
           b.tournament_surface,
           b.hand,
           -- matches.
           Sum(coalesce(b.win_count, 0)) OVER w as career_matches_won,
           Sum(coalesce(b.lose_count, 0)) OVER w as career_matches_lost,
           Sum(coalesce(b.win_count, 0) + coalesce(b.lose_count, 0)) OVER w as career_matches_played,
           -- tournaments.
           Sum(coalesce(b.won_final_flag, 0)) OVER w as career_tournaments_won,
           Sum(coalesce(b.tournaments_played, 0) - coalesce(b.won_final_flag, 0)) OVER w as career_tournaments_lost,
           Sum(coalesce(b.tournaments_played, 0)) OVER w as career_tournament_played,
           -- finals and semifinals.
           Sum(coalesce(b.final_flag, 0)) OVER w as career_finals_played,
           Sum(coalesce(b.semfinal_flag, 0)) OVER w as career_semifinals_played,
           -- surface career matches.
           Sum(CASE WHEN b.tournament_surface = 'Clay' THEN coalesce(b.win_count, 0) ELSE 0 END) OVER w as career_surface_clay_matches_won,
           Sum(CASE WHEN b.tournament_surface = 'Clay' THEN coalesce(b.lose_count, 0) ELSE 0 END) OVER w as career_surface_clay_matches_lost,

           Sum(CASE WHEN b.tournament_surface = 'Carpet' THEN coalesce(b.win_count, 0) ELSE 0 END) OVER w as career_surface_carpet_matches_won,
           Sum(CASE WHEN b.tournament_surface = 'Carpet' THEN coalesce(b.lose_count, 0) ELSE 0 END) OVER w as career_surface_carpet_matches_lost,

           Sum(CASE WHEN b.tournament_surface = 'Hard' THEN coalesce(b.win_count, 0) ELSE 0 END) OVER w as career_surface_hard_matches_won,
           Sum(CASE WHEN b.tournament_surface = 'Hard' THEN coalesce(b.lose_count, 0) ELSE 0 END) OVER w as career_surface_hard_matches_lost,

           Sum(CASE WHEN b.tournament_surface = 'Grass' THEN coalesce(b.win_count, 0) ELSE 0 END) OVER w as career_surface_grass_matches_won,
           Sum(CASE WHEN b.tournament_surface = 'Grass' THEN coalesce(b.lose_count, 0) ELSE 0 END) OVER w as career_surface_grass_matches_lost,
           -- surface career tournaments.
           Sum(CASE WHEN b.tournament_surface = 'Clay' THEN coalesce(b.won_final_flag, 0) ELSE 0 END) OVER w as career_surface_clay_tournaments_won,
           Sum(CASE WHEN b.tournament_surface = 'Clay' THEN coalesce(b.tournaments_played, 0) - coalesce(b.won_final_flag, 0) ELSE 0 END) OVER w as career_surface_clay_tournaments_lost,
           Sum(CASE WHEN b.tournament_surface = 'Clay' THEN coalesce(b.tournaments_played, 0) ELSE 0 END) OVER w as career_surface_clay_tournaments_played,

           Sum(CASE WHEN b.tournament_surface = 'Carpet' THEN coalesce(b.won_final_flag, 0) ELSE 0 END) OVER w as career_surface_carpet_tournaments_won,
           Sum(CASE WHEN b.tournament_surface = 'Carpet' THEN coalesce(b.tournaments_played, 0) - coalesce(b.won_final_flag, 0) ELSE 0 END) OVER w as career_surface_carpet_tournaments_lost,
           Sum(CASE WHEN b.tournament_surface = 'Carpet' THEN coalesce(b.tournaments_played, 0) ELSE 0 END) OVER w as career_surface_carpet_tournaments_played,

           Sum(CASE WHEN b.tournament_surface = 'Hard' THEN coalesce(b.won_final_flag, 0) ELSE 0 END) OVER w as career_surface_hard_tournaments_won,
           Sum(CASE WHEN b.tournament_surface = 'Hard' THEN coalesce(b.tournaments_played, 0) - coalesce(b.won_final_flag, 0) ELSE 0 END) OVER w as career_surface_hard_tournaments_lost,
           Sum(CASE WHEN b.tournament_surface = 'Hard' THEN coalesce(b.tournaments_played, 0) ELSE 0 END) OVER w as career_surface_hard_tournaments_played,

           Sum(CASE WHEN b.tournament_surface = 'Grass' THEN coalesce(b.won_final_flag, 0) ELSE 0 END) OVER w as career_surface_grass_tournaments_won,
           Sum(CASE WHEN b.tournament_surface = 'Grass' THEN coalesce(b.tournaments_played, 0) - coalesce(b.won_final_flag, 0) ELSE 0 END) OVER w as career_surface_grass_tournaments_lost,
           Sum(CASE WHEN b.tournament_surface = 'Grass' THEN coalesce(b.tournaments_played, 0) ELSE 0 END) OVER w as career_surface_grass_tournaments_played,
           -- career hand matches.
           Sum(CASE WHEN b.hand = 'R' THEN coalesce(b.win_count, 0) ELSE 0 END) OVER w as career_right_hand_matches_won,
           Sum(CASE WHEN b.hand = 'R' THEN coalesce(b.lose_count, 0) ELSE 0 END) OVER w as career_right_hand_matches_lost,
           Sum(CASE WHEN b.hand = 'L' THEN coalesce(b.win_count, 0) ELSE 0 END) OVER w as career_left_hand_matches_won,
           Sum(CASE WHEN b.hand = 'L' THEN coalesce(b.lose_count, 0) ELSE 0 END) OVER w as career_left_hand_matches_lost
           FROM (
               -- inner lagged window calls.
                SELECT b.player_id,
                       b.tournament_id,
                       b.tournament_match_num,
                       mm.match_id as match_id,
                       b.tournament_round,
                       b.tournament_surface,
                       b.hand,
                       lag(win_count, 1) over w as win_count,
                       lag(lose_count, 1) over w as lose_count,
                       lag(won_final_flag, 1) over w as won_final_flag,
                       lag(final_flag, 1) over w as final_flag,
                       lag(semfinal_flag, 1) over w as semfinal_flag,
                       lag(tournament_played_flag, 1) over w as tournaments_played
                  FROM (
                        SELECT b.winner_id as player_id,
                               b.tournament_id as tournament_id,
                               b.match_num as tournament_match_num,
                               b.round as tournament_round,
                               b.surface as tournament_surface,
                               b.loser_hand as hand, -- what matters is the hand of the rival
                               1 as win_count,
                               0 as lose_count,
                               CASE WHEN b.round = 'F' THEN 1 ELSE 0 END as won_final_flag,
                               CASE WHEN b.round = 'F' THEN 1 ELSE 0 END as final_flag,
                               CASE WHEN b.round = 'SF' THEN 1 ELSE 0 END as semfinal_flag,
                               CASE WHEN b.round = 'F' THEN 1 ELSE 0 END tournament_played_flag
                          FROM dataset_b AS b
                         UNION ALL
                        SELECT b.loser_id as player_id,
                               b.tournament_id as tournament_id,
                               b.match_num as tournament_match_num,
                               b.round as tournament_round,
                               b.surface as tournament_surface,
                               b.winner_hand as hand, -- what matters is the hand of the rival
                               0 as win_count,
                               1 as lose_count,
                               0 as won_final_flag,
                               CASE WHEN b.round = 'F'  THEN 1 ELSE 0 END as final_flag,
                               CASE WHEN b.round = 'SF' THEN 1 ELSE 0 END as semfinal_flag,
                               1 as tournament_played_flag
                          FROM dataset_b AS b
                  ) as b
                 INNER JOIN matches_master mm
                    ON mm.tournament_id = b.tournament_id
                   AND mm.match_num = b.tournament_match_num
                 INNER JOIN tournaments_master tm
                    ON tm.tournament_id = b.tournament_id
                WINDOW w AS (PARTITION BY b.player_id ORDER BY mm.match_id)
    ) b
        WINDOW w AS (PARTITION BY b.player_id ORDER BY b.match_id)
         ORDER BY player_id, match_id
) c
  WHERE p.player_id = c.player_id
    AND p.tournament_id = c.tournament_id
    AND p.tournament_match_num = c.tournament_match_num;
