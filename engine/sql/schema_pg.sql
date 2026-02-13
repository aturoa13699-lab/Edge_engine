CREATE SCHEMA IF NOT EXISTS nrl;


CREATE SCHEMA IF NOT EXISTS nrl_clean;

CREATE TABLE IF NOT EXISTS nrl_clean.matches_raw (
  match_id text PRIMARY KEY,
  season integer NOT NULL,
  round_num integer NOT NULL,
  match_date date,
  venue text,
  home_team text NOT NULL,
  away_team text NOT NULL,
  home_score integer,
  away_score integer,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS nrl_clean.odds (
  match_id text NOT NULL,
  team text NOT NULL,
  opening_price numeric(7,3),
  close_price numeric(7,3),
  last_price numeric(7,3),
  steam_factor numeric(7,4),
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (match_id, team)
);

CREATE TABLE IF NOT EXISTS nrl_clean.ingestion_provenance (
  id bigserial PRIMARY KEY,
  season integer NOT NULL,
  match_id text NOT NULL,
  source_name text NOT NULL,
  source_url_or_id text NOT NULL,
  fetched_at timestamptz NOT NULL,
  checksum text NOT NULL,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_nrl_clean_prov_season_match
  ON nrl_clean.ingestion_provenance(season, match_id);


-- Views: rest days per team per match (clean namespace)
CREATE OR REPLACE VIEW nrl_clean.team_rest_v AS
WITH team_matches AS (
  SELECT match_id, season, match_date, home_team AS team FROM nrl_clean.matches_raw
  UNION ALL
  SELECT match_id, season, match_date, away_team AS team FROM nrl_clean.matches_raw
),
lagged AS (
  SELECT
    match_id, season, team, match_date,
    LAG(match_date) OVER (PARTITION BY season, team ORDER BY match_date) AS prev_date
  FROM team_matches
)
SELECT
  match_id, season, team,
  CASE
    WHEN prev_date IS NULL OR match_date IS NULL THEN 7
    ELSE GREATEST(1, (match_date - prev_date))
  END AS rest_days
FROM lagged;

-- Views: last5 form per team per match (clean namespace)
CREATE OR REPLACE VIEW nrl_clean.team_form_v AS
WITH team_results AS (
  SELECT season, match_date, match_id, home_team AS team,
         CASE WHEN home_score IS NULL OR away_score IS NULL THEN NULL
              WHEN home_score > away_score THEN 1 ELSE 0 END AS win
  FROM nrl_clean.matches_raw
  UNION ALL
  SELECT season, match_date, match_id, away_team AS team,
         CASE WHEN home_score IS NULL OR away_score IS NULL THEN NULL
              WHEN away_score > home_score THEN 1 ELSE 0 END AS win
  FROM nrl_clean.matches_raw
),
win_hist AS (
  SELECT
    season, match_id, team, match_date,
    AVG(win) OVER (PARTITION BY season, team ORDER BY match_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS win_pct_last5
  FROM team_results
)
SELECT match_id, season, team, COALESCE(win_pct_last5, 0.5) AS win_pct_last5
FROM win_hist;

-- Core fixtures/results
CREATE TABLE IF NOT EXISTS nrl.matches_raw (
  match_id text PRIMARY KEY,
  season integer NOT NULL,
  round_num integer NOT NULL,
  match_date date,
  venue text,
  home_team text NOT NULL,
  away_team text NOT NULL,
  home_score integer,
  away_score integer,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Odds (per team per match)
CREATE TABLE IF NOT EXISTS nrl.odds (
  match_id text NOT NULL,
  team text NOT NULL,
  opening_price numeric(7,3),
  close_price numeric(7,3),
  last_price numeric(7,3),
  steam_factor numeric(7,4),
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (match_id, team)
);

-- Daily weather by venue/date
CREATE TABLE IF NOT EXISTS nrl.weather_daily (
  match_date date NOT NULL,
  venue text NOT NULL,
  conditions text,
  is_wet integer DEFAULT 0,
  temp_c numeric(5,1),
  wind_speed_kmh numeric(5,1),
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (match_date, venue)
);

-- Coach profile (lightweight)
CREATE TABLE IF NOT EXISTS nrl.coach_profile (
  season integer NOT NULL,
  team text NOT NULL,
  style_score numeric(7,3) DEFAULT 0,
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (season, team)
);

-- Team ratings (Elo-like) used by heuristic baseline and as a feature for ML
CREATE TABLE IF NOT EXISTS nrl.team_ratings (
  season integer NOT NULL,
  team text NOT NULL,
  rating numeric(10,2) DEFAULT 1500,
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (season, team)
);

-- Injuries (simplified)
CREATE TABLE IF NOT EXISTS nrl.injuries_current (
  season integer NOT NULL,
  team text NOT NULL,
  injury_count integer DEFAULT 0,
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (season, team)
);

-- Player ratings (v1.1)
CREATE TABLE IF NOT EXISTS nrl.player_ratings (
  season integer NOT NULL,
  player_name text NOT NULL,
  team text NOT NULL,
  rating numeric(6,2) NOT NULL,
  avg_score numeric(5,2),
  key_stat text,
  is_speed_player boolean DEFAULT false,
  note text,
  last_updated timestamptz DEFAULT now(),
  PRIMARY KEY (season, player_name, team)
);

-- Referee tendencies (best-effort)
CREATE TABLE IF NOT EXISTS nrl.referee_tendencies (
  season integer NOT NULL,
  referee text NOT NULL,
  notes text,
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (season, referee)
);

-- Predictions (audit-ready)
CREATE TABLE IF NOT EXISTS nrl.model_prediction (
  id bigserial PRIMARY KEY,
  season integer NOT NULL,
  round_num integer NOT NULL,
  match_id text NOT NULL,
  home_team text NOT NULL,
  away_team text NOT NULL,
  p_fair numeric(7,5),
  calibrated_p numeric(7,5),
  model_version text DEFAULT 'v2026-02-poisson-v1',
  clv_diff numeric(8,4),
  outcome_known boolean DEFAULT false,
  outcome_home_win boolean,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_model_prediction_season_round ON nrl.model_prediction(season, round_num);
CREATE INDEX IF NOT EXISTS ix_model_prediction_version ON nrl.model_prediction(model_version);

-- Slip archive
CREATE TABLE IF NOT EXISTS nrl.slips (
  portfolio_id text PRIMARY KEY,
  season integer NOT NULL,
  round_num integer NOT NULL,
  slip_json jsonb NOT NULL,
  status text DEFAULT 'pending',
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_slips_season_round_status ON nrl.slips(season, round_num, status);

-- Calibration versioning
CREATE TABLE IF NOT EXISTS nrl.calibration_params (
  season integer PRIMARY KEY,
  params jsonb NOT NULL,
  fitted_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_calibration_fitted_at ON nrl.calibration_params(fitted_at DESC);

-- Model registry (CML)
CREATE TABLE IF NOT EXISTS nrl.model_registry (
  model_key text NOT NULL,
  version text NOT NULL,
  artifact_path text NOT NULL,
  metrics jsonb NOT NULL,
  is_champion boolean DEFAULT false,
  created_at timestamptz DEFAULT now(),
  PRIMARY KEY (model_key, version)
);

CREATE INDEX IF NOT EXISTS ix_model_registry_champion ON nrl.model_registry(model_key, is_champion);

-- Data quality gate report history
CREATE TABLE IF NOT EXISTS nrl.data_quality_reports (
  id bigserial PRIMARY KEY,
  checked_at timestamptz NOT NULL,
  seasons text NOT NULL,
  ok boolean NOT NULL,
  report_json jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_data_quality_reports_checked_at
  ON nrl.data_quality_reports(checked_at DESC);

-- Views: rest days per team per match
CREATE OR REPLACE VIEW nrl.team_rest_v AS
WITH team_matches AS (
  SELECT match_id, season, match_date, home_team AS team FROM nrl.matches_raw
  UNION ALL
  SELECT match_id, season, match_date, away_team AS team FROM nrl.matches_raw
),
lagged AS (
  SELECT
    match_id, season, team, match_date,
    LAG(match_date) OVER (PARTITION BY season, team ORDER BY match_date) AS prev_date
  FROM team_matches
)
SELECT
  match_id, season, team,
  CASE
    WHEN prev_date IS NULL OR match_date IS NULL THEN 7
    ELSE GREATEST(1, (match_date - prev_date))
  END AS rest_days
FROM lagged;

-- Views: last5 form per team per match
CREATE OR REPLACE VIEW nrl.team_form_v AS
WITH team_results AS (
  SELECT season, match_date, match_id, home_team AS team,
         CASE WHEN home_score IS NULL OR away_score IS NULL THEN NULL
              WHEN home_score > away_score THEN 1 ELSE 0 END AS win
  FROM nrl.matches_raw
  UNION ALL
  SELECT season, match_date, match_id, away_team AS team,
         CASE WHEN home_score IS NULL OR away_score IS NULL THEN NULL
              WHEN away_score > home_score THEN 1 ELSE 0 END AS win
  FROM nrl.matches_raw
),
win_hist AS (
  SELECT
    season, match_id, team, match_date,
    AVG(win) OVER (PARTITION BY season, team ORDER BY match_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS win_pct_last5
  FROM team_results
)
SELECT match_id, season, team, COALESCE(win_pct_last5, 0.5) AS win_pct_last5
FROM win_hist;
