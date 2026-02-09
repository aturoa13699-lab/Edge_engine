CREATE SCHEMA IF NOT EXISTS nrl;

-- Core matches table (minimal)
CREATE TABLE IF NOT EXISTS nrl.matches (
    match_id text PRIMARY KEY,
    season integer NOT NULL,
    round_num integer NOT NULL,
    home_team text NOT NULL,
    away_team text NOT NULL,
    home_score integer,
    away_score integer,
    f_diff numeric(8,3),
    created_at timestamptz DEFAULT now()
);

-- Odds snapshots
CREATE TABLE IF NOT EXISTS nrl.odds (
    id bigserial PRIMARY KEY,
    match_id text NOT NULL,
    team text NOT NULL,
    close_price numeric(6,2),
    opening_price numeric(6,2),
    steam_factor numeric(6,3),
    captured_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_odds_match_team ON nrl.odds(match_id, team);

-- Slips / portfolio decisions
CREATE TABLE IF NOT EXISTS nrl.slips (
    portfolio_id text PRIMARY KEY,
    season integer NOT NULL,
    round_num integer NOT NULL,
    match_id text NOT NULL,
    market text NOT NULL,
    slip_json jsonb,
    stake_units numeric(10,4),
    status text DEFAULT 'pending',
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

-- Prediction log
CREATE TABLE IF NOT EXISTS nrl.model_prediction (
    season integer NOT NULL,
    round_num integer NOT NULL,
    match_id text NOT NULL,
    home_team text NOT NULL,
    away_team text NOT NULL,
    p_fair numeric(6,5),
    calibrated_p numeric(6,5),
    model_version text DEFAULT 'v2026-02-poisson-v1',
    clv_diff numeric(8,4),
    outcome_known boolean DEFAULT false,
    outcome_home_win boolean,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),
    PRIMARY KEY (season, round_num, match_id)
);

CREATE INDEX IF NOT EXISTS ix_model_prediction_version ON nrl.model_prediction(model_version);

-- Calibration versioning
CREATE TABLE IF NOT EXISTS nrl.calibration_params (
    season integer PRIMARY KEY,
    params jsonb NOT NULL,
    fitted_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_calibration_fitted_at ON nrl.calibration_params(fitted_at DESC);

-- Model registry (CML)
CREATE TABLE IF NOT EXISTS nrl.model_registry (
    model_id text PRIMARY KEY,
    metrics jsonb NOT NULL,
    artifact_path text NOT NULL,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

-- Player ratings (Squad Value + Speed)
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

-- Weather (stub placeholder table used by future scrapers)
CREATE TABLE IF NOT EXISTS nrl.weather_daily (
    venue text NOT NULL,
    day date NOT NULL,
    rainfall_mm numeric(8,2),
    is_wet integer DEFAULT 0,
    temp_c numeric(5,1),
    wind_speed_kmh numeric(5,1),
    PRIMARY KEY (venue, day)
);

-- Injuries (current list)
CREATE TABLE IF NOT EXISTS nrl.injuries_current (
    id bigserial PRIMARY KEY,
    team text NOT NULL,
    player text NOT NULL,
    status text,
    updated_at timestamptz DEFAULT now()
);

-- Coach profile (stub)
CREATE TABLE IF NOT EXISTS nrl.coach_profile (
    season integer NOT NULL,
    team text NOT NULL,
    style_score numeric(8,3),
    PRIMARY KEY (season, team)
);

-- Views (stubs for trainer compatibility)
CREATE OR REPLACE VIEW nrl.team_rest_v AS
SELECT m.match_id, m.home_team AS team, 7 AS rest_days
FROM nrl.matches m;

CREATE OR REPLACE VIEW nrl.team_form_v AS
SELECT m.match_id, m.home_team AS team, 0.5 AS win_pct_last5
FROM nrl.matches m;
