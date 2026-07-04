-- ============================================================
--  AutoSignal Pro — PostgreSQL 18 Schema  (FIXED & CLEAN)
--  Run:
--    createdb autosignal_db
--    psql -U postgres -d autosignal_db -f schema_fixed.sql
-- ============================================================

-- ── Extensions ────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- ============================================================
--  SECTION 0: DROP OLD OBJECTS (safe re-run)
-- ============================================================
DO $$
DECLARE r RECORD;
BEGIN
  -- Drop all triggers named trg_updated_at across all user tables
  FOR r IN
    SELECT trigger_name, event_object_table
    FROM information_schema.triggers
    WHERE trigger_schema = 'public'
      AND trigger_name IN ('trg_updated_at','trg_rrr_stock')
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I',
                   r.trigger_name, r.event_object_table);
  END LOOP;
END $$;

-- Drop functions if they exist (handles renamed function bug)
DROP FUNCTION IF EXISTS update_updated_at()    CASCADE;
DROP FUNCTION IF EXISTS fn_set_updated_at()    CASCADE;
DROP FUNCTION IF EXISTS fn_compute_rrr()       CASCADE;

-- Drop views
DROP VIEW IF EXISTS v_active_signals       CASCADE;
DROP VIEW IF EXISTS v_instrument_performance CASCADE;

-- Drop tables in dependency order
DROP TABLE IF EXISTS hedge_executions        CASCADE;
DROP TABLE IF EXISTS daily_performance       CASCADE;
DROP TABLE IF EXISTS system_events           CASCADE;
DROP TABLE IF EXISTS market_sentiment_log    CASCADE;
DROP TABLE IF EXISTS sensex_options_calls    CASCADE;
DROP TABLE IF EXISTS sensex_futures_calls    CASCADE;
DROP TABLE IF EXISTS nifty_options_calls     CASCADE;
DROP TABLE IF EXISTS nifty_futures_calls     CASCADE;
DROP TABLE IF EXISTS stock_futures_calls     CASCADE;
DROP TABLE IF EXISTS stock_options_calls     CASCADE;
DROP TABLE IF EXISTS stock_calls             CASCADE;
DROP TABLE IF EXISTS option_chain_strikes    CASCADE;
DROP TABLE IF EXISTS option_chain_snapshots  CASCADE;
DROP TABLE IF EXISTS order_book_snapshots    CASCADE;
DROP TABLE IF EXISTS fii_dii_activity        CASCADE;
DROP TABLE IF EXISTS india_vix               CASCADE;
DROP TABLE IF EXISTS market_sessions         CASCADE;
DROP TABLE IF EXISTS instruments             CASCADE;

-- Drop ENUMs
DROP TYPE IF EXISTS call_direction     CASCADE;
DROP TYPE IF EXISTS call_status        CASCADE;
DROP TYPE IF EXISTS market_regime      CASCADE;
DROP TYPE IF EXISTS sentiment_label    CASCADE;
DROP TYPE IF EXISTS signal_confidence  CASCADE;
DROP TYPE IF EXISTS option_type_enum   CASCADE;
DROP TYPE IF EXISTS hedge_strategy     CASCADE;
DROP TYPE IF EXISTS instrument_segment CASCADE;
DROP TYPE IF EXISTS order_type_enum    CASCADE;

-- ============================================================
--  SECTION 1: ENUMERATIONS
-- ============================================================

CREATE TYPE call_direction    AS ENUM ('LONG','SHORT','NEUTRAL');
CREATE TYPE call_status       AS ENUM (
    'ACTIVE','TARGET1_HIT','TARGET2_HIT','SL_HIT',
    'TRAILING_SL_HIT','EXPIRED','MANUALLY_CLOSED','HEDGED_CLOSED','ERROR'
);
CREATE TYPE market_regime     AS ENUM ('STRONG_BULL','BULL','SIDEWAYS','BEAR','STRONG_BEAR','HIGH_VOL');
CREATE TYPE sentiment_label   AS ENUM ('VERY_BULLISH','BULLISH','NEUTRAL','BEARISH','VERY_BEARISH');
CREATE TYPE signal_confidence AS ENUM ('A_PLUS','A','B_PLUS','B','C');
CREATE TYPE option_type_enum  AS ENUM ('CE','PE');
CREATE TYPE hedge_strategy    AS ENUM (
    'BEAR_CALL_SPREAD','BULL_PUT_SPREAD','IRON_CONDOR',
    'PROTECTIVE_PUT','COVERED_CALL','COLLAR',
    'STRADDLE','STRANGLE','SYNTHETIC_LONG','SYNTHETIC_SHORT',
    'CALENDAR_SPREAD','RATIO_SPREAD','NONE'
);
CREATE TYPE instrument_segment AS ENUM ('NSE_EQ','BSE_EQ','NFO','BFO','MCX');
CREATE TYPE order_type_enum   AS ENUM ('MARKET','LIMIT','SL','SL_M');

-- ============================================================
--  SECTION 2: REFERENCE TABLES
-- ============================================================

CREATE TABLE instruments (
    id                  BIGSERIAL PRIMARY KEY,
    symbol              VARCHAR(30)        NOT NULL,
    name                VARCHAR(120)       NOT NULL,
    segment             instrument_segment NOT NULL,
    exchange            VARCHAR(10)        NOT NULL,
    isin                CHAR(12),
    lot_size            INTEGER            NOT NULL DEFAULT 1,
    tick_size           NUMERIC(10,4)      NOT NULL DEFAULT 0.05,
    instrument_token    BIGINT,
    trading_symbol      VARCHAR(50)        NOT NULL,
    expiry_date         DATE,
    strike_price        NUMERIC(12,2),
    option_type         option_type_enum,
    underlying_symbol   VARCHAR(30),
    is_active           BOOLEAN            NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_instrument UNIQUE (trading_symbol)
);
CREATE INDEX idx_instruments_symbol ON instruments(symbol);
CREATE INDEX idx_instruments_token  ON instruments(instrument_token) WHERE instrument_token IS NOT NULL;
CREATE INDEX idx_instruments_expiry ON instruments(expiry_date)      WHERE expiry_date IS NOT NULL;

CREATE TABLE market_sessions (
    id              BIGSERIAL PRIMARY KEY,
    session_date    DATE        UNIQUE NOT NULL,
    is_holiday      BOOLEAN     NOT NULL DEFAULT FALSE,
    holiday_name    VARCHAR(80),
    nifty_open      NUMERIC(10,2),
    nifty_close     NUMERIC(10,2),
    sensex_open     NUMERIC(10,2),
    sensex_close    NUMERIC(10,2),
    vix_open        NUMERIC(8,4),
    vix_close       NUMERIC(8,4),
    advance_count   INTEGER,
    decline_count   INTEGER,
    market_regime   market_regime,
    sentiment       sentiment_label,
    total_fii_net   NUMERIC(15,2),
    total_dii_net   NUMERIC(15,2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
--  SECTION 3: MARKET DATA
-- ============================================================

CREATE TABLE india_vix (
    id          BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL,
    vix         NUMERIC(8,4) NOT NULL,
    vix_change  NUMERIC(8,4),
    vix_pct_chg NUMERIC(8,4),
    open        NUMERIC(8,4),
    high        NUMERIC(8,4),
    low         NUMERIC(8,4),
    close       NUMERIC(8,4),
    CONSTRAINT uq_vix_time UNIQUE (recorded_at)
);
CREATE INDEX idx_vix_time ON india_vix(recorded_at DESC);

CREATE TABLE fii_dii_activity (
    id              BIGSERIAL PRIMARY KEY,
    activity_date   DATE        NOT NULL,
    segment         VARCHAR(20) NOT NULL,
    entity_type     VARCHAR(10) NOT NULL,
    gross_buy       NUMERIC(15,2),
    gross_sell      NUMERIC(15,2),
    net_activity    NUMERIC(15,2) GENERATED ALWAYS AS (gross_buy - gross_sell) STORED,
    cumulative_net  NUMERIC(15,2),
    CONSTRAINT uq_fii_dii UNIQUE (activity_date, segment, entity_type)
);
CREATE INDEX idx_fii_date ON fii_dii_activity(activity_date DESC, entity_type);

CREATE TABLE option_chain_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    underlying      VARCHAR(20) NOT NULL,
    snapshot_time   TIMESTAMPTZ NOT NULL,
    expiry_date     DATE        NOT NULL,
    spot_price      NUMERIC(10,2) NOT NULL,
    atm_strike      NUMERIC(10,2) NOT NULL,
    total_call_oi   BIGINT,
    total_put_oi    BIGINT,
    pcr_oi          NUMERIC(8,4),
    pcr_volume      NUMERIC(8,4),
    max_pain_strike NUMERIC(10,2),
    iv_rank         NUMERIC(6,2),
    iv_percentile   NUMERIC(6,2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_oc_underlying_time ON option_chain_snapshots(underlying, snapshot_time DESC);
CREATE INDEX idx_oc_expiry          ON option_chain_snapshots(expiry_date, underlying);

CREATE TABLE option_chain_strikes (
    id              BIGSERIAL PRIMARY KEY,
    snapshot_id     BIGINT      NOT NULL REFERENCES option_chain_snapshots(id) ON DELETE CASCADE,
    strike_price    NUMERIC(10,2) NOT NULL,
    option_type     option_type_enum NOT NULL,
    ltp             NUMERIC(10,4),
    bid             NUMERIC(10,4),
    ask             NUMERIC(10,4),
    iv              NUMERIC(8,4),
    delta           NUMERIC(8,6),
    gamma           NUMERIC(8,6),
    theta           NUMERIC(8,6),
    vega            NUMERIC(8,6),
    rho             NUMERIC(8,6),
    oi              BIGINT,
    oi_change       BIGINT,
    volume          BIGINT,
    bid_qty         INTEGER,
    ask_qty         INTEGER,
    CONSTRAINT uq_strike UNIQUE (snapshot_id, strike_price, option_type)
);
CREATE INDEX idx_oc_strikes_snap ON option_chain_strikes(snapshot_id, strike_price);

CREATE TABLE order_book_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    instrument_id   BIGINT      NOT NULL REFERENCES instruments(id),
    snapshot_time   TIMESTAMPTZ NOT NULL,
    best_bid        NUMERIC(12,4),
    best_ask        NUMERIC(12,4),
    spread          NUMERIC(12,4) GENERATED ALWAYS AS (best_ask - best_bid) STORED,
    total_bid_qty   BIGINT,
    total_ask_qty   BIGINT,
    imbalance_ratio NUMERIC(8,4),
    depth_json      JSONB
);
CREATE INDEX idx_ob_instrument_time ON order_book_snapshots(instrument_id, snapshot_time DESC);

-- ============================================================
--  SECTION 4: SENTIMENT
-- ============================================================

CREATE TABLE market_sentiment_log (
    id                  BIGSERIAL PRIMARY KEY,
    computed_at         TIMESTAMPTZ     NOT NULL,
    regime              market_regime   NOT NULL,
    sentiment           sentiment_label NOT NULL,
    composite_score     NUMERIC(6,3)    NOT NULL,
    vix_score           NUMERIC(6,3),
    pcr_score           NUMERIC(6,3),
    fii_score           NUMERIC(6,3),
    news_nlp_score      NUMERIC(6,3),
    breadth_score       NUMERIC(6,3),
    momentum_score      NUMERIC(6,3),
    india_vix           NUMERIC(8,4),
    nifty_pcr           NUMERIC(8,4),
    fii_net_crores      NUMERIC(12,2),
    advance_decline     NUMERIC(8,4),
    allow_long          BOOLEAN         NOT NULL DEFAULT TRUE,
    allow_short         BOOLEAN         NOT NULL DEFAULT TRUE,
    allow_options_buy   BOOLEAN         NOT NULL DEFAULT TRUE,
    allow_options_sell  BOOLEAN         NOT NULL DEFAULT FALSE,
    raw_inputs          JSONB,
    CONSTRAINT uq_sentiment_time UNIQUE (computed_at)
);
CREATE INDEX idx_sentiment_time   ON market_sentiment_log(computed_at DESC);
CREATE INDEX idx_sentiment_regime ON market_sentiment_log(regime, computed_at DESC);

-- ============================================================
--  SECTION 5: CALL TABLES (one per instrument class)
-- ============================================================

-- 5.1  STOCK CALLS
CREATE TABLE stock_calls (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol              VARCHAR(30)        NOT NULL,
    instrument_id       BIGINT             REFERENCES instruments(id),
    exchange            VARCHAR(10)        NOT NULL DEFAULT 'NSE',
    call_type           VARCHAR(20)        NOT NULL DEFAULT 'INTRADAY',
    direction           call_direction     NOT NULL,
    entry_price_low     NUMERIC(12,2)      NOT NULL,
    entry_price_high    NUMERIC(12,2)      NOT NULL,
    entry_trigger       NUMERIC(12,2),
    entry_order_type    order_type_enum    NOT NULL DEFAULT 'LIMIT',
    target_1            NUMERIC(12,2)      NOT NULL,
    target_2            NUMERIC(12,2),
    target_3            NUMERIC(12,2),
    stop_loss           NUMERIC(12,2)      NOT NULL,
    trailing_sl_rule    TEXT,
    fill_price          NUMERIC(12,2),
    exit_price          NUMERIC(12,2),
    risk_reward_ratio   NUMERIC(8,3),
    suggested_quantity  INTEGER,
    position_size_inr   NUMERIC(15,2),
    actual_pnl          NUMERIC(12,2),
    max_drawdown_pct    NUMERIC(8,4),
    status              call_status        NOT NULL DEFAULT 'ACTIVE',
    confidence          signal_confidence  NOT NULL,
    regime_at_signal    market_regime,
    sentiment_at_signal sentiment_label,
    sentiment_score     NUMERIC(6,3),
    signal_score        SMALLINT,
    vix_at_signal       NUMERIC(8,4),
    rsi_14              NUMERIC(6,2),
    macd_hist           NUMERIC(10,4),
    ema_9               NUMERIC(12,4),
    ema_21              NUMERIC(12,4),
    atr_14              NUMERIC(12,4),
    adx_14              NUMERIC(6,2),
    volume_ratio        NUMERIC(8,4),
    vwap                NUMERIC(12,4),
    supertrend_dir      SMALLINT,
    rationale           TEXT,
    tags                TEXT[],
    generated_by        VARCHAR(50)        NOT NULL DEFAULT 'SentimentAdaptiveEngine',
    signal_time         TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    entry_time          TIMESTAMPTZ,
    exit_time           TIMESTAMPTZ,
    expiry_time         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_sc_status_time ON stock_calls(status, signal_time DESC);
CREATE INDEX idx_sc_symbol_time ON stock_calls(symbol,  signal_time DESC);
CREATE INDEX idx_sc_direction   ON stock_calls(direction, status);
CREATE INDEX idx_sc_confidence  ON stock_calls(confidence, signal_time DESC);
CREATE INDEX idx_sc_regime      ON stock_calls(regime_at_signal, signal_time DESC);

-- 5.2  STOCK OPTIONS CALLS
CREATE TABLE stock_options_calls (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    underlying_symbol   VARCHAR(30)        NOT NULL,
    instrument_id       BIGINT             REFERENCES instruments(id),
    contract_symbol     VARCHAR(50)        NOT NULL,
    option_type         option_type_enum   NOT NULL,
    strike_price        NUMERIC(12,2)      NOT NULL,
    expiry_date         DATE               NOT NULL,
    lot_size            INTEGER            NOT NULL,
    lots                SMALLINT           NOT NULL DEFAULT 1,
    direction           call_direction     NOT NULL,
    entry_premium_low   NUMERIC(10,4)      NOT NULL,
    entry_premium_high  NUMERIC(10,4)      NOT NULL,
    entry_order_type    order_type_enum    NOT NULL DEFAULT 'LIMIT',
    target_premium_1    NUMERIC(10,4)      NOT NULL,
    target_premium_2    NUMERIC(10,4),
    stop_loss_premium   NUMERIC(10,4)      NOT NULL,
    trailing_sl_rule    TEXT,
    time_stop_rule      TEXT,
    hedge_strategy      hedge_strategy     NOT NULL,
    hedge_leg_symbol    VARCHAR(50),
    hedge_leg_strike    NUMERIC(12,2),
    hedge_leg_type      option_type_enum,
    hedge_leg_lots      SMALLINT,
    hedge_credit_debit  NUMERIC(10,4),
    max_loss_defined    BOOLEAN            NOT NULL DEFAULT FALSE,
    max_loss_inr        NUMERIC(12,2),
    underlying_spot     NUMERIC(12,2),
    delta               NUMERIC(8,6),
    gamma               NUMERIC(8,6),
    theta               NUMERIC(8,6),
    vega                NUMERIC(8,6),
    iv                  NUMERIC(8,4),
    iv_rank             NUMERIC(6,2),
    days_to_expiry      SMALLINT,
    moneyness           VARCHAR(5),
    fill_premium        NUMERIC(10,4),
    exit_premium        NUMERIC(10,4),
    actual_pnl          NUMERIC(12,2),
    status              call_status        NOT NULL DEFAULT 'ACTIVE',
    confidence          signal_confidence  NOT NULL,
    regime_at_signal    market_regime,
    sentiment_score     NUMERIC(6,3),
    pcr_at_signal       NUMERIC(8,4),
    vix_at_signal       NUMERIC(8,4),
    rationale           TEXT,
    tags                TEXT[],
    generated_by        VARCHAR(50)        NOT NULL DEFAULT 'SentimentAdaptiveEngine',
    signal_time         TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    entry_time          TIMESTAMPTZ,
    exit_time           TIMESTAMPTZ,
    created_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_soc_underlying_time ON stock_options_calls(underlying_symbol, signal_time DESC);
CREATE INDEX idx_soc_expiry          ON stock_options_calls(expiry_date, underlying_symbol);
CREATE INDEX idx_soc_status          ON stock_options_calls(status, signal_time DESC);
CREATE INDEX idx_soc_strike_type     ON stock_options_calls(strike_price, option_type, expiry_date);

-- 5.3  STOCK FUTURES CALLS
CREATE TABLE stock_futures_calls (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    underlying_symbol   VARCHAR(30)        NOT NULL,
    instrument_id       BIGINT             REFERENCES instruments(id),
    contract_symbol     VARCHAR(50)        NOT NULL,
    expiry_date         DATE               NOT NULL,
    lot_size            INTEGER            NOT NULL,
    lots                SMALLINT           NOT NULL DEFAULT 1,
    direction           call_direction     NOT NULL,
    entry_price_low     NUMERIC(12,2)      NOT NULL,
    entry_price_high    NUMERIC(12,2)      NOT NULL,
    entry_order_type    order_type_enum    NOT NULL DEFAULT 'LIMIT',
    target_1            NUMERIC(12,2)      NOT NULL,
    target_2            NUMERIC(12,2),
    stop_loss           NUMERIC(12,2)      NOT NULL,
    trailing_sl_rule    TEXT,
    hedge_strategy      hedge_strategy     NOT NULL,
    hedge_description   TEXT,
    hedge_cost_per_lot  NUMERIC(10,2),
    basis_at_signal     NUMERIC(10,4),
    required_margin     NUMERIC(15,2),
    span_margin         NUMERIC(15,2),
    exposure_margin     NUMERIC(15,2),
    fill_price          NUMERIC(12,2),
    exit_price          NUMERIC(12,2),
    actual_pnl          NUMERIC(12,2),
    status              call_status        NOT NULL DEFAULT 'ACTIVE',
    confidence          signal_confidence  NOT NULL,
    regime_at_signal    market_regime,
    sentiment_score     NUMERIC(6,3),
    vix_at_signal       NUMERIC(8,4),
    rollover_pct        NUMERIC(8,4),
    rationale           TEXT,
    tags                TEXT[],
    generated_by        VARCHAR(50)        NOT NULL DEFAULT 'SentimentAdaptiveEngine',
    signal_time         TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    entry_time          TIMESTAMPTZ,
    exit_time           TIMESTAMPTZ,
    created_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_sfc_underlying_time ON stock_futures_calls(underlying_symbol, signal_time DESC);
CREATE INDEX idx_sfc_status          ON stock_futures_calls(status, signal_time DESC);
CREATE INDEX idx_sfc_expiry          ON stock_futures_calls(expiry_date);

-- 5.4  NIFTY FUTURES CALLS
CREATE TABLE nifty_futures_calls (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_symbol     VARCHAR(50)        NOT NULL,
    expiry_date         DATE               NOT NULL,
    lot_size            INTEGER            NOT NULL DEFAULT 75,
    lots                SMALLINT           NOT NULL DEFAULT 1,
    direction           call_direction     NOT NULL,
    entry_price_low     NUMERIC(10,2)      NOT NULL,
    entry_price_high    NUMERIC(10,2)      NOT NULL,
    entry_order_type    order_type_enum    NOT NULL DEFAULT 'MARKET',
    target_1            NUMERIC(10,2)      NOT NULL,
    target_2            NUMERIC(10,2),
    stop_loss           NUMERIC(10,2)      NOT NULL,
    sl_points           NUMERIC(8,2)       GENERATED ALWAYS AS
                            (ABS(entry_price_high - stop_loss)) STORED,
    trailing_sl_rule    TEXT,
    hedge_strategy      hedge_strategy     NOT NULL,
    hedge_strike        NUMERIC(10,2),
    hedge_option_type   option_type_enum,
    hedge_lots          SMALLINT,
    hedge_premium       NUMERIC(10,4),
    hedge_description   TEXT,
    is_overnight        BOOLEAN            NOT NULL DEFAULT FALSE,
    nifty_spot          NUMERIC(10,2),
    nifty_futures_basis NUMERIC(8,4),
    pcr_at_signal       NUMERIC(8,4),
    vix_at_signal       NUMERIC(8,4),
    max_pain            NUMERIC(10,2),
    required_margin     NUMERIC(15,2),
    fill_price          NUMERIC(10,2),
    exit_price          NUMERIC(10,2),
    actual_pnl          NUMERIC(12,2),
    actual_pnl_per_lot  NUMERIC(10,2)  GENERATED ALWAYS AS (
        CASE WHEN exit_price IS NOT NULL AND fill_price IS NOT NULL
             THEN (exit_price - fill_price) * 75 ELSE NULL END
    ) STORED,
    status              call_status        NOT NULL DEFAULT 'ACTIVE',
    confidence          signal_confidence  NOT NULL,
    regime_at_signal    market_regime,
    sentiment_score     NUMERIC(6,3),
    signal_score        SMALLINT,
    rationale           TEXT,
    tags                TEXT[],
    generated_by        VARCHAR(50)        NOT NULL DEFAULT 'SentimentAdaptiveEngine',
    signal_time         TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    entry_time          TIMESTAMPTZ,
    exit_time           TIMESTAMPTZ,
    created_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_nfc_status_time ON nifty_futures_calls(status, signal_time DESC);
CREATE INDEX idx_nfc_expiry      ON nifty_futures_calls(expiry_date);
CREATE INDEX idx_nfc_direction   ON nifty_futures_calls(direction, status);
CREATE INDEX idx_nfc_regime      ON nifty_futures_calls(regime_at_signal, signal_time DESC);

-- 5.5  NIFTY OPTIONS CALLS
CREATE TABLE nifty_options_calls (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_symbol     VARCHAR(50)        NOT NULL,
    option_type         option_type_enum   NOT NULL,
    strike_price        NUMERIC(10,2)      NOT NULL,
    expiry_date         DATE               NOT NULL,
    lot_size            INTEGER            NOT NULL DEFAULT 75,
    lots                SMALLINT           NOT NULL DEFAULT 1,
    direction           call_direction     NOT NULL,
    entry_premium_low   NUMERIC(10,4)      NOT NULL,
    entry_premium_high  NUMERIC(10,4)      NOT NULL,
    entry_order_type    order_type_enum    NOT NULL DEFAULT 'LIMIT',
    target_premium_1    NUMERIC(10,4)      NOT NULL,
    target_premium_2    NUMERIC(10,4),
    stop_loss_premium   NUMERIC(10,4)      NOT NULL,
    sl_pct_of_premium   NUMERIC(6,2),
    trailing_sl_rule    TEXT,
    time_stop_rule      TEXT,
    hedge_strategy      hedge_strategy     NOT NULL,
    hedge_strike        NUMERIC(10,2),
    hedge_option_type   option_type_enum,
    hedge_lots          SMALLINT,
    hedge_premium       NUMERIC(10,4),
    spread_type         VARCHAR(30),
    max_loss_inr        NUMERIC(12,2),
    max_profit_inr      NUMERIC(12,2),
    breakeven_1         NUMERIC(10,2),
    breakeven_2         NUMERIC(10,2),
    is_spread           BOOLEAN            NOT NULL DEFAULT FALSE,
    strategy_name       VARCHAR(80),
    nifty_spot          NUMERIC(10,2),
    delta               NUMERIC(8,6),
    gamma               NUMERIC(8,6),
    theta               NUMERIC(8,6),
    vega                NUMERIC(8,6),
    iv                  NUMERIC(8,4),
    iv_rank             NUMERIC(6,2),
    iv_percentile       NUMERIC(6,2),
    days_to_expiry      SMALLINT,
    moneyness           VARCHAR(5),
    fill_premium        NUMERIC(10,4),
    exit_premium        NUMERIC(10,4),
    actual_pnl          NUMERIC(12,2),
    actual_pnl_per_lot  NUMERIC(10,2)  GENERATED ALWAYS AS (
        CASE WHEN exit_premium IS NOT NULL AND fill_premium IS NOT NULL
             THEN (exit_premium - fill_premium) * 75 ELSE NULL END
    ) STORED,
    status              call_status        NOT NULL DEFAULT 'ACTIVE',
    confidence          signal_confidence  NOT NULL,
    regime_at_signal    market_regime,
    sentiment_score     NUMERIC(6,3),
    pcr_at_signal       NUMERIC(8,4),
    vix_at_signal       NUMERIC(8,4),
    max_pain_at_signal  NUMERIC(10,2),
    signal_score        SMALLINT,
    rationale           TEXT,
    tags                TEXT[],
    generated_by        VARCHAR(50)        NOT NULL DEFAULT 'SentimentAdaptiveEngine',
    signal_time         TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    entry_time          TIMESTAMPTZ,
    exit_time           TIMESTAMPTZ,
    created_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_noc_status_time ON nifty_options_calls(status, signal_time DESC);
CREATE INDEX idx_noc_strike_type ON nifty_options_calls(strike_price, option_type, expiry_date);
CREATE INDEX idx_noc_expiry      ON nifty_options_calls(expiry_date, strike_price);
CREATE INDEX idx_noc_direction   ON nifty_options_calls(direction, option_type, status);
CREATE INDEX idx_noc_confidence  ON nifty_options_calls(confidence, signal_time DESC);

-- 5.6  SENSEX FUTURES CALLS
CREATE TABLE sensex_futures_calls (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_symbol     VARCHAR(50)        NOT NULL,
    expiry_date         DATE               NOT NULL,
    lot_size            INTEGER            NOT NULL DEFAULT 10,
    lots                SMALLINT           NOT NULL DEFAULT 1,
    direction           call_direction     NOT NULL,
    entry_price_low     NUMERIC(10,2)      NOT NULL,
    entry_price_high    NUMERIC(10,2)      NOT NULL,
    entry_order_type    order_type_enum    NOT NULL DEFAULT 'MARKET',
    target_1            NUMERIC(10,2)      NOT NULL,
    target_2            NUMERIC(10,2),
    stop_loss           NUMERIC(10,2)      NOT NULL,
    trailing_sl_rule    TEXT,
    hedge_strategy      hedge_strategy     NOT NULL,
    hedge_strike        NUMERIC(10,2),
    hedge_option_type   option_type_enum,
    hedge_lots          SMALLINT,
    hedge_premium       NUMERIC(10,4),
    hedge_description   TEXT,
    is_overnight        BOOLEAN            NOT NULL DEFAULT FALSE,
    sensex_spot         NUMERIC(10,2),
    basis_at_signal     NUMERIC(10,4),
    vix_at_signal       NUMERIC(8,4),
    required_margin     NUMERIC(15,2),
    fill_price          NUMERIC(10,2),
    exit_price          NUMERIC(10,2),
    actual_pnl          NUMERIC(12,2),
    actual_pnl_per_lot  NUMERIC(10,2)  GENERATED ALWAYS AS (
        CASE WHEN exit_price IS NOT NULL AND fill_price IS NOT NULL
             THEN (exit_price - fill_price) * 10 ELSE NULL END
    ) STORED,
    status              call_status        NOT NULL DEFAULT 'ACTIVE',
    confidence          signal_confidence  NOT NULL,
    regime_at_signal    market_regime,
    sentiment_score     NUMERIC(6,3),
    signal_score        SMALLINT,
    rationale           TEXT,
    tags                TEXT[],
    generated_by        VARCHAR(50)        NOT NULL DEFAULT 'SentimentAdaptiveEngine',
    signal_time         TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    entry_time          TIMESTAMPTZ,
    exit_time           TIMESTAMPTZ,
    created_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_sxfc_status_time ON sensex_futures_calls(status, signal_time DESC);
CREATE INDEX idx_sxfc_expiry      ON sensex_futures_calls(expiry_date);

-- 5.7  SENSEX OPTIONS CALLS
CREATE TABLE sensex_options_calls (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_symbol     VARCHAR(50)        NOT NULL,
    option_type         option_type_enum   NOT NULL,
    strike_price        NUMERIC(10,2)      NOT NULL,
    expiry_date         DATE               NOT NULL,
    lot_size            INTEGER            NOT NULL DEFAULT 10,
    lots                SMALLINT           NOT NULL DEFAULT 1,
    direction           call_direction     NOT NULL,
    entry_premium_low   NUMERIC(10,4)      NOT NULL,
    entry_premium_high  NUMERIC(10,4)      NOT NULL,
    entry_order_type    order_type_enum    NOT NULL DEFAULT 'LIMIT',
    target_premium_1    NUMERIC(10,4)      NOT NULL,
    target_premium_2    NUMERIC(10,4),
    stop_loss_premium   NUMERIC(10,4)      NOT NULL,
    trailing_sl_rule    TEXT,
    time_stop_rule      TEXT,
    hedge_strategy      hedge_strategy     NOT NULL,
    hedge_strike        NUMERIC(10,2),
    hedge_option_type   option_type_enum,
    hedge_lots          SMALLINT,
    hedge_premium       NUMERIC(10,4),
    spread_type         VARCHAR(30),
    max_loss_inr        NUMERIC(12,2),
    max_profit_inr      NUMERIC(12,2),
    breakeven_1         NUMERIC(10,2),
    breakeven_2         NUMERIC(10,2),
    is_spread           BOOLEAN            NOT NULL DEFAULT FALSE,
    strategy_name       VARCHAR(80),
    sensex_spot         NUMERIC(10,2),
    delta               NUMERIC(8,6),
    theta               NUMERIC(8,6),
    vega                NUMERIC(8,6),
    iv                  NUMERIC(8,4),
    iv_rank             NUMERIC(6,2),
    days_to_expiry      SMALLINT,
    moneyness           VARCHAR(5),
    fill_premium        NUMERIC(10,4),
    exit_premium        NUMERIC(10,4),
    actual_pnl          NUMERIC(12,2),
    status              call_status        NOT NULL DEFAULT 'ACTIVE',
    confidence          signal_confidence  NOT NULL,
    regime_at_signal    market_regime,
    sentiment_score     NUMERIC(6,3),
    pcr_at_signal       NUMERIC(8,4),
    vix_at_signal       NUMERIC(8,4),
    signal_score        SMALLINT,
    rationale           TEXT,
    tags                TEXT[],
    generated_by        VARCHAR(50)        NOT NULL DEFAULT 'SentimentAdaptiveEngine',
    signal_time         TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    entry_time          TIMESTAMPTZ,
    exit_time           TIMESTAMPTZ,
    created_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ        NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_sxoc_status_time   ON sensex_options_calls(status, signal_time DESC);
CREATE INDEX idx_sxoc_strike_expiry ON sensex_options_calls(strike_price, option_type, expiry_date);

-- ============================================================
--  SECTION 6: AUDIT & PERFORMANCE TABLES
-- ============================================================

CREATE TABLE hedge_executions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_call_id  UUID         NOT NULL,
    parent_table    VARCHAR(50)  NOT NULL,
    hedge_strategy  hedge_strategy NOT NULL,
    leg_number      SMALLINT     NOT NULL DEFAULT 1,
    contract_symbol VARCHAR(50)  NOT NULL,
    direction       call_direction NOT NULL,
    quantity        INTEGER      NOT NULL,
    entry_price     NUMERIC(12,4) NOT NULL,
    exit_price      NUMERIC(12,4),
    hedge_pnl       NUMERIC(12,2),
    executed_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    notes           TEXT
);
CREATE INDEX idx_hedge_parent ON hedge_executions(parent_call_id, parent_table);

CREATE TABLE daily_performance (
    id                  BIGSERIAL PRIMARY KEY,
    perf_date           DATE        UNIQUE NOT NULL,
    opening_balance     NUMERIC(15,2) NOT NULL,
    closing_balance     NUMERIC(15,2),
    gross_pnl           NUMERIC(12,2) NOT NULL DEFAULT 0,
    total_brokerage     NUMERIC(10,2) NOT NULL DEFAULT 0,
    net_pnl             NUMERIC(12,2) NOT NULL DEFAULT 0,
    total_signals       INTEGER       NOT NULL DEFAULT 0,
    active_signals      INTEGER       NOT NULL DEFAULT 0,
    closed_signals      INTEGER       NOT NULL DEFAULT 0,
    winning_trades      INTEGER       NOT NULL DEFAULT 0,
    losing_trades       INTEGER       NOT NULL DEFAULT 0,
    sl_hits             INTEGER       NOT NULL DEFAULT 0,
    target_hits         INTEGER       NOT NULL DEFAULT 0,
    win_rate            NUMERIC(6,4),
    profit_factor       NUMERIC(8,4),
    avg_win             NUMERIC(12,2),
    avg_loss            NUMERIC(12,2),
    largest_win         NUMERIC(12,2),
    largest_loss        NUMERIC(12,2),
    max_drawdown        NUMERIC(12,2),
    sharpe_ratio        NUMERIC(8,4),
    expectancy          NUMERIC(12,4),
    equity_pnl          NUMERIC(12,2) DEFAULT 0,
    stock_opt_pnl       NUMERIC(12,2) DEFAULT 0,
    stock_fut_pnl       NUMERIC(12,2) DEFAULT 0,
    nifty_fut_pnl       NUMERIC(12,2) DEFAULT 0,
    nifty_opt_pnl       NUMERIC(12,2) DEFAULT 0,
    sensex_fut_pnl      NUMERIC(12,2) DEFAULT 0,
    sensex_opt_pnl      NUMERIC(12,2) DEFAULT 0,
    hedge_pnl           NUMERIC(12,2) DEFAULT 0,
    india_vix_avg       NUMERIC(8,4),
    regime              market_regime,
    notes               TEXT,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_dp_date ON daily_performance(perf_date DESC);

CREATE TABLE system_events (
    id          BIGSERIAL PRIMARY KEY,
    event_type  VARCHAR(40)  NOT NULL,
    severity    VARCHAR(10)  NOT NULL DEFAULT 'INFO',
    source      VARCHAR(50),
    message     TEXT,
    metadata    JSONB,
    resolved    BOOLEAN      NOT NULL DEFAULT FALSE,
    resolved_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_se_severity_time ON system_events(severity,    created_at DESC);
CREATE INDEX idx_se_type_time     ON system_events(event_type,  created_at DESC);

-- ============================================================
--  SECTION 7: TRIGGER FUNCTION  (defined ONCE, named clearly)
-- ============================================================

-- FIX: single canonical function name used everywhere
CREATE OR REPLACE FUNCTION trg_fn_set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

-- Apply the trigger to every table that has updated_at
DO $$
DECLARE
    tbl TEXT;
BEGIN
    FOREACH tbl IN ARRAY ARRAY[
        'instruments',
        'stock_calls',
        'stock_options_calls',
        'stock_futures_calls',
        'nifty_futures_calls',
        'nifty_options_calls',
        'sensex_futures_calls',
        'sensex_options_calls',
        'daily_performance'
    ]
    LOOP
        -- Drop old trigger first (safe re-run)
        EXECUTE format(
            'DROP TRIGGER IF EXISTS trg_updated_at ON %I',
            tbl
        );
        -- Create trigger calling the ONE canonical function
        EXECUTE format(
            'CREATE TRIGGER trg_updated_at
             BEFORE UPDATE ON %I
             FOR EACH ROW
             EXECUTE FUNCTION trg_fn_set_updated_at()',
            tbl
        );
    END LOOP;
END;
$$;

-- ============================================================
--  SECTION 8: RISK/REWARD RATIO AUTO-COMPUTE (stock_calls)
-- ============================================================

CREATE OR REPLACE FUNCTION trg_fn_compute_rrr()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
    entry_mid NUMERIC;
    reward    NUMERIC;
    risk      NUMERIC;
BEGIN
    entry_mid := (NEW.entry_price_low + NEW.entry_price_high) / 2.0;
    reward    := ABS(NEW.target_1 - entry_mid);
    risk      := ABS(entry_mid - NEW.stop_loss);
    IF risk > 0 THEN
        NEW.risk_reward_ratio := ROUND(reward / risk, 3);
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_rrr_stock ON stock_calls;
CREATE TRIGGER trg_rrr_stock
    BEFORE INSERT OR UPDATE ON stock_calls
    FOR EACH ROW
    EXECUTE FUNCTION trg_fn_compute_rrr();

-- ============================================================
--  SECTION 9: VIEWS
-- ============================================================

-- 9.1  Unified active signals across all 7 tables
CREATE OR REPLACE VIEW v_active_signals AS
    SELECT id, 'STOCK'      AS instrument_type, symbol          AS name,
           direction::TEXT, entry_price_low, entry_price_high,
           target_1, stop_loss, confidence::TEXT,
           regime_at_signal::TEXT, sentiment_score,
           vix_at_signal, signal_score, signal_time, status::TEXT,
           risk_reward_ratio AS rrr, rationale, tags
    FROM stock_calls WHERE status = 'ACTIVE'
  UNION ALL
    SELECT id, 'STOCK_OPT'  AS instrument_type, contract_symbol AS name,
           direction::TEXT, entry_premium_low, entry_premium_high,
           target_premium_1, stop_loss_premium, confidence::TEXT,
           regime_at_signal::TEXT, sentiment_score,
           vix_at_signal, NULL, signal_time, status::TEXT,
           NULL, rationale, tags
    FROM stock_options_calls WHERE status = 'ACTIVE'
  UNION ALL
    SELECT id, 'STOCK_FUT'  AS instrument_type, contract_symbol AS name,
           direction::TEXT, entry_price_low, entry_price_high,
           target_1, stop_loss, confidence::TEXT,
           regime_at_signal::TEXT, sentiment_score,
           vix_at_signal, NULL, signal_time, status::TEXT,
           NULL, rationale, tags
    FROM stock_futures_calls WHERE status = 'ACTIVE'
  UNION ALL
    SELECT id, 'NIFTY_FUT'  AS instrument_type, contract_symbol AS name,
           direction::TEXT, entry_price_low, entry_price_high,
           target_1, stop_loss, confidence::TEXT,
           regime_at_signal::TEXT, sentiment_score,
           vix_at_signal, signal_score, signal_time, status::TEXT,
           NULL, rationale, tags
    FROM nifty_futures_calls WHERE status = 'ACTIVE'
  UNION ALL
    SELECT id, 'NIFTY_OPT'  AS instrument_type, contract_symbol AS name,
           direction::TEXT, entry_premium_low, entry_premium_high,
           target_premium_1, stop_loss_premium, confidence::TEXT,
           regime_at_signal::TEXT, sentiment_score,
           vix_at_signal, signal_score, signal_time, status::TEXT,
           NULL, rationale, tags
    FROM nifty_options_calls WHERE status = 'ACTIVE'
  UNION ALL
    SELECT id, 'SENSEX_FUT' AS instrument_type, contract_symbol AS name,
           direction::TEXT, entry_price_low, entry_price_high,
           target_1, stop_loss, confidence::TEXT,
           regime_at_signal::TEXT, sentiment_score,
           vix_at_signal, signal_score, signal_time, status::TEXT,
           NULL, rationale, tags
    FROM sensex_futures_calls WHERE status = 'ACTIVE'
  UNION ALL
    SELECT id, 'SENSEX_OPT' AS instrument_type, contract_symbol AS name,
           direction::TEXT, entry_premium_low, entry_premium_high,
           target_premium_1, stop_loss_premium, confidence::TEXT,
           regime_at_signal::TEXT, sentiment_score,
           vix_at_signal, signal_score, signal_time, status::TEXT,
           NULL, rationale, tags
    FROM sensex_options_calls WHERE status = 'ACTIVE';

-- 9.2  Per-instrument performance stats
CREATE OR REPLACE VIEW v_instrument_performance AS
    SELECT 'STOCK' AS instrument,
        COUNT(*) FILTER (WHERE status <> 'ACTIVE')                            AS total,
        COUNT(*) FILTER (WHERE actual_pnl > 0)                                AS winners,
        COUNT(*) FILTER (WHERE actual_pnl < 0)                                 AS losers,
        ROUND(100.0 * COUNT(*) FILTER (WHERE actual_pnl > 0)
            / NULLIF(COUNT(*) FILTER (WHERE status <> 'ACTIVE'), 0), 2)        AS win_pct,
        COALESCE(SUM(actual_pnl), 0)                                           AS total_pnl,
        COALESCE(AVG(actual_pnl) FILTER (WHERE actual_pnl > 0), 0)            AS avg_win,
        COALESCE(AVG(actual_pnl) FILTER (WHERE actual_pnl < 0), 0)             AS avg_loss,
        COALESCE(SUM(actual_pnl) FILTER (WHERE actual_pnl > 0), 0)
            / NULLIF(ABS(SUM(actual_pnl) FILTER (WHERE actual_pnl < 0)), 0)   AS profit_factor
    FROM stock_calls
  UNION ALL
    SELECT 'STOCK_OPT',
        COUNT(*) FILTER (WHERE status <> 'ACTIVE'),
        COUNT(*) FILTER (WHERE actual_pnl > 0),
        COUNT(*) FILTER (WHERE actual_pnl < 0),
        ROUND(100.0 * COUNT(*) FILTER (WHERE actual_pnl > 0)
            / NULLIF(COUNT(*) FILTER (WHERE status <> 'ACTIVE'), 0), 2),
        COALESCE(SUM(actual_pnl), 0),
        COALESCE(AVG(actual_pnl) FILTER (WHERE actual_pnl > 0), 0),
        COALESCE(AVG(actual_pnl) FILTER (WHERE actual_pnl < 0), 0),
        COALESCE(SUM(actual_pnl) FILTER (WHERE actual_pnl > 0), 0)
            / NULLIF(ABS(SUM(actual_pnl) FILTER (WHERE actual_pnl < 0)), 0)
    FROM stock_options_calls
  UNION ALL
    SELECT 'NIFTY_FUT',
        COUNT(*) FILTER (WHERE status <> 'ACTIVE'),
        COUNT(*) FILTER (WHERE actual_pnl > 0),
        COUNT(*) FILTER (WHERE actual_pnl < 0),
        ROUND(100.0 * COUNT(*) FILTER (WHERE actual_pnl > 0)
            / NULLIF(COUNT(*) FILTER (WHERE status <> 'ACTIVE'), 0), 2),
        COALESCE(SUM(actual_pnl), 0),
        COALESCE(AVG(actual_pnl) FILTER (WHERE actual_pnl > 0), 0),
        COALESCE(AVG(actual_pnl) FILTER (WHERE actual_pnl < 0), 0),
        COALESCE(SUM(actual_pnl) FILTER (WHERE actual_pnl > 0), 0)
            / NULLIF(ABS(SUM(actual_pnl) FILTER (WHERE actual_pnl < 0)), 0)
    FROM nifty_futures_calls
  UNION ALL
    SELECT 'NIFTY_OPT',
        COUNT(*) FILTER (WHERE status <> 'ACTIVE'),
        COUNT(*) FILTER (WHERE actual_pnl > 0),
        COUNT(*) FILTER (WHERE actual_pnl < 0),
        ROUND(100.0 * COUNT(*) FILTER (WHERE actual_pnl > 0)
            / NULLIF(COUNT(*) FILTER (WHERE status <> 'ACTIVE'), 0), 2),
        COALESCE(SUM(actual_pnl), 0),
        COALESCE(AVG(actual_pnl) FILTER (WHERE actual_pnl > 0), 0),
        COALESCE(AVG(actual_pnl) FILTER (WHERE actual_pnl < 0), 0),
        COALESCE(SUM(actual_pnl) FILTER (WHERE actual_pnl > 0), 0)
            / NULLIF(ABS(SUM(actual_pnl) FILTER (WHERE actual_pnl < 0)), 0)
    FROM nifty_options_calls
  UNION ALL
    SELECT 'SENSEX_FUT',
        COUNT(*) FILTER (WHERE status <> 'ACTIVE'),
        COUNT(*) FILTER (WHERE actual_pnl > 0),
        COUNT(*) FILTER (WHERE actual_pnl < 0),
        ROUND(100.0 * COUNT(*) FILTER (WHERE actual_pnl > 0)
            / NULLIF(COUNT(*) FILTER (WHERE status <> 'ACTIVE'), 0), 2),
        COALESCE(SUM(actual_pnl), 0),
        COALESCE(AVG(actual_pnl) FILTER (WHERE actual_pnl > 0), 0),
        COALESCE(AVG(actual_pnl) FILTER (WHERE actual_pnl < 0), 0),
        COALESCE(SUM(actual_pnl) FILTER (WHERE actual_pnl > 0), 0)
            / NULLIF(ABS(SUM(actual_pnl) FILTER (WHERE actual_pnl < 0)), 0)
    FROM sensex_futures_calls
  UNION ALL
    SELECT 'SENSEX_OPT',
        COUNT(*) FILTER (WHERE status <> 'ACTIVE'),
        COUNT(*) FILTER (WHERE actual_pnl > 0),
        COUNT(*) FILTER (WHERE actual_pnl < 0),
        ROUND(100.0 * COUNT(*) FILTER (WHERE actual_pnl > 0)
            / NULLIF(COUNT(*) FILTER (WHERE status <> 'ACTIVE'), 0), 2),
        COALESCE(SUM(actual_pnl), 0),
        COALESCE(AVG(actual_pnl) FILTER (WHERE actual_pnl > 0), 0),
        COALESCE(AVG(actual_pnl) FILTER (WHERE actual_pnl < 0), 0),
        COALESCE(SUM(actual_pnl) FILTER (WHERE actual_pnl > 0), 0)
            / NULLIF(ABS(SUM(actual_pnl) FILTER (WHERE actual_pnl < 0)), 0)
    FROM sensex_options_calls;

-- ============================================================
--  SECTION 10: VERIFY (run to confirm all objects created)
-- ============================================================
DO $$
DECLARE
    tbl_count  INT;
    trig_count INT;
    view_count INT;
BEGIN
    SELECT COUNT(*) INTO tbl_count
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE';

    SELECT COUNT(*) INTO trig_count
    FROM information_schema.triggers
    WHERE trigger_schema = 'public' AND trigger_name = 'trg_updated_at';

    SELECT COUNT(*) INTO view_count
    FROM information_schema.views
    WHERE table_schema = 'public';

    RAISE NOTICE '==============================================';
    RAISE NOTICE ' AutoSignal Pro Schema — Setup Complete';
    RAISE NOTICE ' Tables  : %', tbl_count;
    RAISE NOTICE ' Triggers: % (trg_updated_at)', trig_count;
    RAISE NOTICE ' Views   : %', view_count;
    RAISE NOTICE '==============================================';
END;
$$;
