# NeonStrat1

A Python-based project for managing and interacting with a NeonDB PostgreSQL database.

## Setup

1. Clone the repository:
```bash
git clone https://github.com/cklose2000/NeonStrat1.git
cd NeonStrat1
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the root directory with your NeonDB credentials:
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

## Usage

The project provides several utility functions for database operations:

- `test_connection()`: Test the database connection
- `list_tables()`: List all tables in the database
- `drop_all_tables_public_schema()`: Drop all tables in the public schema (use with caution)

To run the basic connection test:
```bash
python db_connection.py
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 