import pandas as pd
from typing import List, Optional, Any


class FinancialDataProcessor:
    """Process raw API data into clean DataFrames"""

    @staticmethod
    def safe_to_dataframe(data: Optional[Any]) -> pd.DataFrame:
        """Safely convert API data to DataFrame"""
        if data is None or not data:
            return pd.DataFrame()

        try:
            df = pd.DataFrame(data)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
            return df
        except Exception as e:
            print(f"Error creating DataFrame: {e}")
            return pd.DataFrame()

    @staticmethod
    def filter_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Filter DataFrame to specified columns"""
        if df.empty:
            return pd.DataFrame(columns=columns)
        available = [col for col in columns if col in df.columns]
        return df[available].copy()

    def process_liquidity_ratios(
            self,
            key_metrics_data: Optional[Any],
            ratios_data: Optional[Any]
    ) -> pd.DataFrame:
        """Process liquidity ratios from key-metrics endpoint"""
        df_key_met = self.safe_to_dataframe(key_metrics_data)
        df_fratios = self.safe_to_dataframe(ratios_data)

        key_met_columns = [
            'symbol', 'date', 'currentRatio', 'cashRatio', 'daysOfSalesOutstanding'
        ]
        fratios_columns = ['symbol', 'date', 'operatingCashFlowRatio', 'debtToAssetsRatio', 'debtToEquityRatio',
                           'interestCoverage', 'debtServiceCoverageRatio']

        df_key_met_filt = self.filter_columns(df_key_met, key_met_columns)
        df_fratios_filt = self.filter_columns(df_fratios, fratios_columns)

        liquidity_df = df_key_met_filt.merge(df_fratios_filt, on=['symbol', 'date'], how='left')

        return liquidity_df

    def process_earnings_ratios(
            self,
            ratios_data: Optional[Any],
            key_metrics_data: Optional[Any]
    ) -> pd.DataFrame:
        """Process earnings and valuation ratios"""
        ratios_df = self.safe_to_dataframe(ratios_data)
        metrics_df = self.safe_to_dataframe(key_metrics_data)

        # Extract earnings ratios
        earnings_cols = [
            'symbol', 'date', 'priceToEarningsRatio',
            'priceEarningsToGrowthRatio', 'priceToSalesRatio',
            'enterpriseValueMultiple'
        ]
        earnings_df = self.filter_columns(ratios_df, earnings_cols)

        # Add EV/Sales from key metrics
        ev_cols = ['symbol', 'date', 'evToSales']
        ev_df = self.filter_columns(metrics_df, ev_cols)

        if not earnings_df.empty and not ev_df.empty:
            earnings_df = earnings_df.merge(
                ev_df,
                on=['symbol', 'date'],
                how='left'
            )
            earnings_df.rename(columns={'evToSales': 'EV/Sales'}, inplace=True)

        return earnings_df

    def process_financial_growth(
            self,
            financial_growth_data: Optional[Any],
            income_growth_data: Optional[Any]
    ) -> pd.DataFrame:
        """Process financial growth metrics"""
        fg_df = self.safe_to_dataframe(financial_growth_data)
        ig_df = self.safe_to_dataframe(income_growth_data)

        # Extract growth metrics
        growth_cols = [
            'symbol', 'date', 'revenueGrowth', 'ebitGrowth',
            'operatingIncomeGrowth', 'netIncomeGrowth', 'epsGrowth',
            'operatingCashFlowGrowth', 'freeCashFlowGrowth'
        ]
        growth_df = self.filter_columns(fg_df, growth_cols)

        # Add EBITDA growth
        ebitda_cols = ['symbol', 'date', 'growthEBITDA']
        ebitda_df = self.filter_columns(ig_df, ebitda_cols)

        if not growth_df.empty and not ebitda_df.empty:
            growth_df = growth_df.merge(
                ebitda_df,
                on=['symbol', 'date'],
                how='left'
            )

            # Rename and reorder
            growth_df.rename(columns={'growthEBITDA': 'ebitdaGrowth'}, inplace=True)

            # Move ebitdaGrowth after ebitgrowth
            cols = list(growth_df.columns)
            if 'ebitdaGrowth' in cols and 'ebitgrowth' in cols:
                cols.remove('ebitdaGrowth')
                idx = cols.index('ebitgrowth') + 1
                cols.insert(idx, 'ebitdaGrowth')
                growth_df = growth_df[cols]

        # Convert to percentages
        growth_rate_cols = [
            col for col in growth_df.columns
            if 'Growth' in col or 'growth' in col
        ]
        growth_df[growth_rate_cols] = growth_df[growth_rate_cols] * 100

        return growth_df

    def process_profit_margins(
            self,
            ratios_data: Optional[Any]
    ) -> pd.DataFrame:
        """Process profit margin metrics"""
        df = self.safe_to_dataframe(ratios_data)

        columns = ['symbol', 'date', 'operatingProfitMargin', 'netProfitMargin']

        return self.filter_columns(df, columns)

    def process_stocks_daily(self, eod_data: Optional[Any], market_cap: Optional[Any],
                             sector: Optional[Any], shares_float: Optional[Any]) -> pd.DataFrame:

        df_eod = self.safe_to_dataframe(eod_data)
        df_market_cap = self.safe_to_dataframe(market_cap)
        df_sector = self.safe_to_dataframe(sector)
        df_shares_float = self.safe_to_dataframe(shares_float)

        eod_columns = ['symbol', 'date', 'open', 'close', 'high', 'low', 'volume', 'vwap', 'changePercent']
        marketcap_columns = ['symbol', 'date', 'marketCap']
        x_columns = ['symbol', 'date', 'outstandingShares']
        sector_columns = ['symbol', 'sector', 'industry', 'exchange']

        eod_df = self.filter_columns(df_eod, eod_columns)
        market_cap_df = self.filter_columns(df_market_cap, marketcap_columns)
        sector_df = self.filter_columns(df_sector, sector_columns)
        shares_float_df = self.filter_columns(df_shares_float, x_columns)
        shares_float_df["date"] = pd.to_datetime(shares_float_df["date"]).dt.normalize()

        eod_df = eod_df.merge(
            market_cap_df,
            on=['symbol', 'date'],
            how='left'
        )
        # eod_df = eod_df.merge(
        #     shares_float_df,
        #     on=['symbol', 'date'],
        #     how='left'
        # )
        eod_df = eod_df.merge(
            sector_df,
            on='symbol',  # Only merge on symbol, not date
            how='left'
        )

        return eod_df
