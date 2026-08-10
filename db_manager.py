from sqlalchemy import create_engine, Table, Column, String, Float, DateTime, MetaData, Index, Integer, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime
from config import DatabaseConfig


class DatabaseManager:
    """Manages database connections and operations"""

    # Table schemas
    TABLE_SCHEMAS = {
        'liquidity_ratios': {
            'symbol': String(10),
            'date': DateTime,
            'current_ratio': Float,
            'cash_ratio': Float,
            'days_of_sales_outstanding': Float,
            'operating_cash_flow_ratio': Float,
            'debt_to_assets_ratio': Float,
            'debt_to_equity_ratio': Float,
            'interest_coverage': Float,
            'debt_service_coverage_ratio': Float,
            'created_at': DateTime
        },
        'earnings_ratios': {
            'symbol': String(10),
            'date': DateTime,
            'price_earnings_ratio': Float,
            'price_to_earnings_ratio': Float,
            'price_to_sales_ratio': Float,
            'enterprise_value_multiple': Float,
            'ev_to_sales': Float,
            'created_at': DateTime
        },
        'financial_growth': {
            'symbol': String(10),
            'date': DateTime,
            'revenue_growth': Float,
            'ebit_growth': Float,
            'ebitda_growth': Float,
            'operating_income_growth': Float,
            'net_income_growth': Float,
            'eps_growth': Float,
            'operating_cash_flow_growth': Float,
            'free_cash_flow_growth': Float,
            'created_at': DateTime
        },
        'profit_margins': {
            'symbol': String(10),
            'date': DateTime,
            'operating_profit_margin': Float,
            'net_profit_margin': Float,
            'created_at': DateTime
        },
        'stocks_daily': {
            'symbol': String(10),
            'adj_open': Float,
            'adj_close': Float,
            'high': Float,
            'low': Float,
            'change_percent': Float,
            'date': DateTime,
            'exchange': String(10),
            'sector': String(50),
            'industry': String(100),
            'volume': Integer,
            'change_pct': Float,
            'vwap': Float,
            'market_cap': Float,
            'shares_outstanding': Integer,
            'created_at': DateTime

            }
    }

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = create_engine(
            config.get_sqlalchemy_url(),
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20
        )
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)

    def create_tables(self):
        """Create all tables if they don't exist"""
        for table_name, schema in self.TABLE_SCHEMAS.items():
            # Create table with auto-incrementing integer primary key
            table = Table(
                table_name,
                self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                *[Column(col_name, col_type) for col_name, col_type in schema.items()],
                extend_existing=True
            )

            # Create indexes for performance
            Index(f'idx_{table_name}_symbol', table.c.symbol)
            Index(f'idx_{table_name}_date', table.c.date)
            Index(f'idx_{table_name}_symbol_date', table.c.symbol, table.c.date, unique=True)

        # Create all tables
        self.metadata.create_all(self.engine)
        print("Database tables created successfully")

    def drop_tables(self):
        """Drop all tables - USE WITH CAUTION"""
        self.metadata.drop_all(self.engine)
        print("All tables dropped")

    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame column names to match database schema"""
        # Convert camelCase to snake_case
        df = df.copy()
        df.columns = df.columns.str.replace(r'([A-Z])', r'_\1', regex=True).str.lower()
        df.columns = df.columns.str.replace('__', '_').str.strip('_')

        # Handle special cases
        column_mapping = {
            'e_v/_sales': 'ev_to_sales',
            'open': 'adj_open',
            'close': 'adj_close'
        }
        df.rename(columns=column_mapping, inplace=True)

        return df

    def insert_data(
            self,
            table_name: str,
            df: pd.DataFrame,
            method: str = 'upsert'
    ) -> int:
        """
        Insert data into database table

        Args:
            table_name: Name of the table
            df: DataFrame to insert
            method: 'upsert' (update if exists), 'replace', or 'append'

        Returns:
            Number of rows inserted/updated
        """
        if df.empty:
            print(f"No data to insert into {table_name}")
            return 0

        # Normalize column names
        df = self._normalize_column_names(df)

        # Remove 'id' column if it exists (will be auto-generated)
        if 'id' in df.columns:
            df = df.drop(columns=['id'])

        df['created_at'] = datetime.now()

        # Convert DataFrame to dict records
        records = df.to_dict('records')

        if method == 'upsert':
            return self._upsert_data(table_name, records)
        elif method == 'replace':
            return self._replace_data(table_name, df)
        elif method == 'append':
            return self._append_data(table_name, df)
        else:
            raise ValueError(f"Unknown method: {method}")

    def _upsert_data(self, table_name: str, records: List[Dict]) -> int:
        """Insert or update data using PostgreSQL's ON CONFLICT"""
        table = self.metadata.tables[table_name]

        with self.engine.begin() as conn:
            # Use PostgreSQL's INSERT ... ON CONFLICT DO UPDATE
            stmt = insert(table).values(records)

            # Update all columns except id on conflict (symbol, date)
            update_dict = {
                col.name: stmt.excluded[col.name]
                for col in table.columns
                if col.name not in ['id']
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=['symbol', 'date'],
                set_=update_dict
            )

            result = conn.execute(stmt)
            return result.rowcount

    def _replace_data(self, table_name: str, df: pd.DataFrame) -> int:
        """Replace all data in table"""
        df.to_sql(
            table_name,
            self.engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        return len(df)

    def _append_data(self, table_name: str, df: pd.DataFrame) -> int:
        """Append data to table"""
        df.to_sql(
            table_name,
            self.engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        return len(df)

    def load_table(
            self,
            table_name: str,
            symbols: Optional[List[str]] = None,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Load data from database table

        Args:
            table_name: Name of the table
            symbols: Filter by symbols (optional)
            start_date: Filter by start date (optional)
            end_date: Filter by end date (optional)

        Returns:
            DataFrame with loaded data
        """
        query = f"SELECT * FROM {table_name} WHERE 1=1"
        params = []

        if symbols:
            placeholders = ','.join(['%s'] * len(symbols))
            query += f" AND symbol IN ({placeholders})"
            params.extend(symbols)

        if start_date:
            query += " AND date >= %s"
            params.append(start_date)

        if end_date:
            query += " AND date <= %s"
            params.append(end_date)

        query += " ORDER BY symbol, date DESC"

        df = pd.read_sql(query, self.engine, params=params)
        return df

    def get_latest_dates(self, table_name: str) -> Dict[str, datetime]:
        """Get the latest date for each symbol in a table"""
        query = f"""
            SELECT symbol, MAX(date) as latest_date
            FROM {table_name}
            GROUP BY symbol
        """
        df = pd.read_sql(query, self.engine)
        return dict(zip(df['symbol'], df['latest_date']))

    def get_table_stats(self, table_name: str) -> Dict:
        """Get statistics about a table"""
        query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT symbol) as unique_symbols,
                MIN(date) as earliest_date,
                MAX(date) as latest_date
            FROM {table_name}
        """
        df = pd.read_sql(query, self.engine)
        return df.iloc[0].to_dict()

    def delete_symbol(self, table_name: str, symbol: str) -> int:
        """Delete all records for a specific symbol"""
        query = text(f"DELETE FROM {table_name} WHERE symbol = :symbol")

        with self.engine.begin() as conn:
            result = conn.execute(query, {'symbol': symbol})
            return result.rowcount

    def close(self):
        """Close database connection"""
        self.engine.dispose()


class DatabasePipeline:
    """High-level pipeline for database operations"""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def initialize_database(self):
        """Initialize database with tables"""
        self.db.create_tables()

    def save_all_tables(
            self,
            tables: Dict[str, pd.DataFrame],
            method: str = 'upsert'
    ) -> Dict[str, int]:
        """
        Save all tables to database

        Returns:
            Dict with table names and row counts
        """
        results = {}

        print("\n=== SAVING TO DATABASE ===")
        for table_name, df in tables.items():
            if not df.empty:
                count = self.db.insert_data(table_name, df, method=method)
                results[table_name] = count
                print(f"{table_name}: {count} rows saved")
            else:
                results[table_name] = 0
                print(f"{table_name}: no data to save")

        return results

    def load_all_tables(
            self,
            symbols: Optional[List[str]] = None
    ) -> Dict[str, pd.DataFrame]:
        """Load all tables from database"""
        tables = {}

        print("\n=== LOADING FROM DATABASE ===")
        for table_name in self.db.TABLE_SCHEMAS.keys():
            df = self.db.load_table(table_name, symbols=symbols)
            tables[table_name] = df
            print(f"{table_name}: {len(df)} rows loaded")

        return tables

    def get_database_summary(self) -> pd.DataFrame:
        """Get summary statistics for all tables"""
        summary_data = []

        for table_name in self.db.TABLE_SCHEMAS.keys():
            try:
                stats = self.db.get_table_stats(table_name)
                stats['table'] = table_name
                summary_data.append(stats)
            except Exception as e:
                print(f"Error getting stats for {table_name}: {e}")

        return pd.DataFrame(summary_data)

