-- Market Data Schema
-- Market instruments table
CREATE TABLE instruments (
    instrument_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    instrument_type VARCHAR(10) NOT NULL,
    tick_size NUMERIC(15, 8) NOT NULL,
    lot_size INTEGER NOT NULL,
    trading_hours JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(symbol, exchange)
);

-- Raw tick data, partitioned by date
CREATE TABLE tick_data (
    tick_id BIGSERIAL,
    instrument_id INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    price NUMERIC(15, 8) NOT NULL,
    volume INTEGER NOT NULL,
    bid_price NUMERIC(15, 8),
    ask_price NUMERIC(15, 8),
    bid_size INTEGER,
    ask_size INTEGER,
    trade_id VARCHAR(50),
    trade_condition VARCHAR(20)[],
    source VARCHAR(20) NOT NULL,
    PRIMARY KEY (tick_id, timestamp),
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id)
) PARTITION BY RANGE (timestamp);

-- Bar data tables (aggregated)
CREATE TABLE bars (
    bar_id BIGSERIAL,
    instrument_id INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    timeframe VARCHAR(10) NOT NULL, -- Changed from VARCHAR(5) to VARCHAR(10)
    open NUMERIC(15, 8) NOT NULL,
    high NUMERIC(15, 8) NOT NULL,
    low NUMERIC(15, 8) NOT NULL,
    close NUMERIC(15, 8) NOT NULL,
    volume BIGINT NOT NULL,
    vwap NUMERIC(15, 8),
    trades INTEGER,
    PRIMARY KEY (bar_id, timestamp, timeframe),
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id)
) PARTITION BY LIST (timeframe);

-- Create partitions for different timeframes
CREATE TABLE bars_5m PARTITION OF bars FOR VALUES IN ('5m');
CREATE TABLE bars_15m PARTITION OF bars FOR VALUES IN ('15m');
CREATE TABLE bars_30m PARTITION OF bars FOR VALUES IN ('30m');
CREATE TABLE bars_60m PARTITION OF bars FOR VALUES IN ('60m');
CREATE TABLE bars_daily PARTITION OF bars FOR VALUES IN ('daily');
CREATE TABLE bars_weekly PARTITION OF bars FOR VALUES IN ('weekly');

-- Create indexes on partitions
CREATE INDEX idx_bars_5m_instrument_time ON bars_5m (instrument_id, timestamp);
CREATE INDEX idx_bars_15m_instrument_time ON bars_15m (instrument_id, timestamp);
CREATE INDEX idx_bars_30m_instrument_time ON bars_30m (instrument_id, timestamp);
CREATE INDEX idx_bars_60m_instrument_time ON bars_60m (instrument_id, timestamp);
CREATE INDEX idx_bars_daily_instrument_time ON bars_daily (instrument_id, timestamp);
CREATE INDEX idx_bars_weekly_instrument_time ON bars_weekly (instrument_id, timestamp);

-- Trading strategies
CREATE TABLE strategies (
    strategy_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    version VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_modified TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    author VARCHAR(100),
    status VARCHAR(20) DEFAULT 'draft',
    UNIQUE(name, version)
);

-- Strategy parameters
CREATE TABLE strategy_parameters (
    parameter_id SERIAL PRIMARY KEY,
    strategy_id INTEGER NOT NULL,
    name VARCHAR(100) NOT NULL,
    data_type VARCHAR(20) NOT NULL,
    default_value TEXT,
    min_value TEXT,
    max_value TEXT,
    description TEXT,
    FOREIGN KEY (strategy_id) REFERENCES strategies(strategy_id),
    UNIQUE(strategy_id, name)
);

-- Parameter sets for backtesting
CREATE TABLE parameter_sets (
    set_id SERIAL PRIMARY KEY,
    strategy_id INTEGER NOT NULL,
    name VARCHAR(100) NOT NULL,
    parameters JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    FOREIGN KEY (strategy_id) REFERENCES strategies(strategy_id)
);

-- Backtest sessions
CREATE TABLE backtest_sessions (
    session_id SERIAL PRIMARY KEY,
    strategy_id INTEGER NOT NULL,
    parameter_set_id INTEGER NOT NULL,
    instrument_id INTEGER NOT NULL,
    start_date TIMESTAMP WITH TIME ZONE NOT NULL,
    end_date TIMESTAMP WITH TIME ZONE NOT NULL,
    timeframe VARCHAR(10) NOT NULL, -- Changed from VARCHAR(5) to VARCHAR(10)
    initial_capital NUMERIC(15, 2) NOT NULL,
    commission_model JSONB,
    slippage_model JSONB,
    status VARCHAR(20) DEFAULT 'running',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY (strategy_id) REFERENCES strategies(strategy_id),
    FOREIGN KEY (parameter_set_id) REFERENCES parameter_sets(set_id),
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id)
);

-- Simulated orders
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    instrument_id INTEGER NOT NULL,
    order_type VARCHAR(20) NOT NULL,
    side VARCHAR(5) NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(15, 8),
    status VARCHAR(20) DEFAULT 'pending',
    time_in_force VARCHAR(10),
    stop_price NUMERIC(15, 8),
    limit_price NUMERIC(15, 8),
    filled_quantity INTEGER DEFAULT 0,
    average_fill_price NUMERIC(15, 8),
    submit_time TIMESTAMP WITH TIME ZONE,
    execution_time TIMESTAMP WITH TIME ZONE,
    cancellation_time TIMESTAMP WITH TIME ZONE,
    reason VARCHAR(100),
    FOREIGN KEY (session_id) REFERENCES backtest_sessions(session_id),
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id)
);

-- Simulated trades/executions
CREATE TABLE trades (
    trade_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    price NUMERIC(15, 8) NOT NULL,
    quantity INTEGER NOT NULL,
    commission NUMERIC(15, 8) NOT NULL,
    slippage NUMERIC(15, 8) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Portfolio state snapshots
CREATE TABLE portfolio_snapshots (
    snapshot_id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    cash NUMERIC(15, 2) NOT NULL,
    equity NUMERIC(15, 2) NOT NULL,
    margin_used NUMERIC(15, 2) DEFAULT 0,
    margin_available NUMERIC(15, 2),
    position_value NUMERIC(15, 2) NOT NULL,
    open_pnl NUMERIC(15, 2) NOT NULL,
    closed_pnl NUMERIC(15, 2) NOT NULL,
    FOREIGN KEY (session_id) REFERENCES backtest_sessions(session_id)
);

-- Positions
CREATE TABLE positions (
    position_id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL,
    instrument_id INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    quantity INTEGER NOT NULL,
    average_price NUMERIC(15, 8) NOT NULL,
    current_price NUMERIC(15, 8) NOT NULL,
    unrealized_pnl NUMERIC(15, 8) NOT NULL,
    realized_pnl NUMERIC(15, 8) NOT NULL,
    FOREIGN KEY (session_id) REFERENCES backtest_sessions(session_id),
    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id)
);

-- Performance metrics
CREATE TABLE performance_metrics (
    metric_id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL,
    total_return NUMERIC(15, 8),
    annualized_return NUMERIC(15, 8),
    sharpe_ratio NUMERIC(15, 8),
    sortino_ratio NUMERIC(15, 8),
    max_drawdown NUMERIC(15, 8),
    max_drawdown_duration INTEGER,
    volatility NUMERIC(15, 8),
    win_rate NUMERIC(15, 8),
    profit_factor NUMERIC(15, 8),
    avg_profit_loss_ratio NUMERIC(15, 8),
    avg_holding_period NUMERIC(15, 8),
    num_trades INTEGER,
    profitable_trades INTEGER,
    losing_trades INTEGER,
    metrics_json JSONB,
    FOREIGN KEY (session_id) REFERENCES backtest_sessions(session_id)
);

-- System logs
CREATE TABLE system_logs (
    log_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    log_level VARCHAR(10) NOT NULL,
    component VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    details JSONB
);

-- Audit trails
CREATE TABLE audit_trails (
    audit_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    user_id VARCHAR(100),
    action VARCHAR(50) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_id INTEGER NOT NULL,
    old_values JSONB,
    new_values JSONB
);

-- Create indexes for fast time range queries
CREATE INDEX idx_tick_data_instrument_time ON tick_data(instrument_id, timestamp); 