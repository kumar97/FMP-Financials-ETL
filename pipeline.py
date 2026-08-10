"""Main pipeline orchestrating data fetching and processing"""

import time
import os
from typing import List, Dict, Any
import pandas as pd
from config import FMPConfig
from client import AsyncFMPClient
from processor import FinancialDataProcessor


class FinancialDataPipeline:
    """Main pipeline for async data fetching and processing"""

    # Define standard endpoints
    ENDPOINTS = [
        'key-metrics',
        'ratios',
        'financial-growth',
        'income-statement-growth',
        'historical-price-eod',
        'historical-market-capitalization',
        'profile',
        'shares-float'
    ]

    def __init__(self, api_key: str, max_concurrent: int = 20):
        config = FMPConfig(
            api_key=api_key,
            max_concurrent_requests=max_concurrent
        )
        self.client = AsyncFMPClient(config)
        self.processor = FinancialDataProcessor()
        self.raw_data: Dict[str, Dict[str, Any]] = {}

    def fetch_data(
            self,
            tickers: List[str],
            show_progress: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch data for multiple tickers

        Returns:
            Raw data organized by ticker and endpoint
        """
        print(f"Fetching data for {len(tickers)} tickers...")
        start_time = time.time()

        def progress_callback(completed, total):
            if show_progress:
                pct = (completed / total) * 100
                print(f"Progress: {completed}/{total} ({pct:.1f}%)", end='\r')

        self.raw_data = self.client.fetch_all(
            tickers,
            self.ENDPOINTS,
            progress_callback=progress_callback if show_progress else None
        )

        elapsed = time.time() - start_time

        if show_progress:
            print(f"\nCompleted in {elapsed:.2f} seconds")

            # Show stats
            stats = self.client.get_stats()
            success = stats['success_rate']
            print(f"Success rate: {success:.1f}%")
            print(f"Average fetch time: {stats['avg_fetch_time']:.3f}s")

            if stats['failed_requests']:
                print(f"\nFailed requests: {len(stats['failed_requests'])}")
                for failed in stats['failed_requests'][:5]:  # Show first 5
                    print(f"  - {failed['ticker']} / {failed['endpoint']}: {failed['error']}")

        return self.raw_data

    def process_ticker(self, ticker: str) -> Dict[str, pd.DataFrame]:
        """Process data for a single ticker into DataFrames"""
        if ticker not in self.raw_data:
            print(f"No data found for {ticker}")
            return {}

        ticker_data = self.raw_data[ticker]

        return dict(liquidity_ratios=self.processor.process_liquidity_ratios(
            ticker_data.get('key-metrics'),
            ticker_data.get('ratios')
        ), earnings_ratios=self.processor.process_earnings_ratios(
            ticker_data.get('ratios'),
            ticker_data.get('key-metrics')
        ), financial_growth=self.processor.process_financial_growth(
            ticker_data.get('financial-growth'),
            ticker_data.get('income-statement-growth')
        ), profit_margins=self.processor.process_profit_margins(
            ticker_data.get('ratios')
        ), stocks_daily=self.processor.process_stocks_daily(ticker_data.get('historical-price-eod'),
                                                            ticker_data.get('historical-market-capitalization'),
                                                            ticker_data.get('profile'),
                                                            ticker_data.get('shares-float'))
        )

    def process_all(self) -> Dict[str, pd.DataFrame]:
        """Process all fetched data and combine into tables"""
        all_tables = {
            'liquidity_ratios': [],
            'earnings_ratios': [],
            'financial_growth': [],
            'profit_margins': [],
            'stocks_daily': []
        }

        print(f"\nProcessing {len(self.raw_data)} tickers...")

        for ticker in self.raw_data.keys():
            processed = self.process_ticker(ticker)

            for table_name, df in processed.items():
                if not df.empty:
                    all_tables[table_name].append(df)

        # Combine all tables
        combined = {}
        for table_name, dfs in all_tables.items():
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)
                combined_df.sort_values(
                    ['symbol', 'date'],
                    ascending=[True, False],
                    inplace=True
                )
                combined[table_name] = combined_df
                print(f"{table_name}: {len(combined_df)} rows")
            else:
                combined[table_name] = pd.DataFrame()
                print(f"{table_name}: 0 rows")

        return combined

    def export_to_csv(self, tables: Dict[str, pd.DataFrame], output_dir: str = "."):
        """Export tables to CSV files"""
        os.makedirs(output_dir, exist_ok=True)

        for table_name, df in tables.items():
            if not df.empty:
                filepath = os.path.join(output_dir, f"{table_name}.csv")
                df.to_csv(filepath, index=False)
                print(f"Exported {table_name} to {filepath}")

    def get_raw_data(self) -> Dict[str, Dict[str, Any]]:
        """Access raw API data for custom processing"""
        return self.raw_data
