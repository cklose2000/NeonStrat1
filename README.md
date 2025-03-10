# PostgreSQL-Based Trading Strategy Backtesting System

A comprehensive system for backtesting trading strategies using PostgreSQL as the central data store. The system supports multiple strategies, detailed performance analysis, and efficient data management.

## Features

- **Data Management**
  - Efficient market data ingestion and storage
  - Support for tick data and OHLCV bars
  - Automatic data aggregation
  - Time-series optimized storage

- **Strategy Framework**
  - Flexible strategy implementation
  - Built-in strategies:
    - Moving Average Crossover
    - RSI (Relative Strength Index)
    - Bollinger Bands
  - Easy to add new strategies

- **Backtesting Engine**
  - Realistic simulation with slippage and commission
  - Detailed trade logging
  - Portfolio tracking
  - Performance metrics calculation

- **Database-Centric Architecture**
  - Efficient data storage and retrieval
  - Transaction safety
  - Complex query support
  - Data integrity enforcement

## Prerequisites

- Python 3.8+
- PostgreSQL 13+
- NeonDB account with database credentials

## Installation

1. Clone the repository:
```bash
git clone https://github.com/cklose2000/NeonStrat1.git
cd NeonStrat1
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file with your NeonDB credentials:
```env
# NeonDB Credentials
NEON_API_KEY=your_neon_api_key_here

# Database Connection
DATABASE_URL=your_database_url
PGHOST=your_host
PGDATABASE=your_database
PGUSER=your_user
PGPASSWORD=your_password
PGPORT=5432

# SSL Configuration
PGSSLMODE=require

# Additional NeonDB Options
NEON_PROJECT_ID=your_project_id
```

5. Initialize the database:
```bash
python scripts/init_db.py
```

## Usage

### Basic Usage

1. Run a simple backtest:
```bash
python scripts/main.py
```

This will:
- Ingest sample market data
- Run backtests for all built-in strategies
- Store results in the database

### Custom Strategies

1. Create a new strategy by subclassing the `Strategy` class:
```python
from src.strategy import Strategy

class MyStrategy(Strategy):
    def initialize(self, parameters):
        # Initialize strategy parameters
        self.param1 = parameters.get('param1', default_value)
    
    def on_bar(self, bar_data):
        # Implement your strategy logic
        # Return DataFrame with trading signals
        return signals
```

2. Use the strategy in backtesting:
```python
from src.backtest import BacktestSimulator

simulator = BacktestSimulator(db_config)
session_id = simulator.run_backtest(
    strategy_class=MyStrategy,
    strategy_id=1,
    parameter_set_id=1,
    instrument_id=1,
    start_date='2023-01-01',
    end_date='2023-12-31',
    parameters={'param1': value}
)
```

## Project Structure

```
NeonStrat1/
├── data/                  # Market data files
├── logs/                  # Log files (excluded from version control)
├── schema/                # Database schema files
│   └── schema.sql
├── scripts/               # Utility scripts
│   ├── init_db.py         # Database initialization
│   ├── load_market_data.py # Load market data into the database
│   ├── test_connection.py # Test database connection
│   └── main.py            # Example usage
├── src/                   # Source code
│   ├── __init__.py
│   ├── data_loader.py     # Data loader module
│   ├── data_ingestion.py  # Data ingestion module
│   ├── backtest.py        # Backtesting engine
│   ├── strategy.py        # Strategy implementations
│   └── db_connection.py   # Database connection management
├── .env.example           # Environment variable template
├── .gitignore             # Git ignore file
├── README.md              # Project documentation
└── requirements.txt       # Project dependencies
```

## Database Schema

The system uses a comprehensive database schema:

- **Market Data Tables**
  - `instruments`: Tradable instruments
  - `tick_data`: Raw market data
  - `bars`: Aggregated OHLCV bars

- **Strategy Tables**
  - `strategies`: Trading strategy definitions
  - `strategy_parameters`: Strategy parameter definitions
  - `parameter_sets`: Parameter combinations for testing

- **Backtest Tables**
  - `backtest_sessions`: Backtest execution records
  - `orders`: Simulated orders
  - `trades`: Executed trades
  - `portfolio_snapshots`: Portfolio state tracking
  - `performance_metrics`: Backtest results

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Built with [NeonDB](https://neon.tech/) for PostgreSQL hosting
- Inspired by various open-source backtesting frameworks
