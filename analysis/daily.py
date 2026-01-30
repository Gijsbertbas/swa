from datetime import date, datetime
from matplotlib.figure import Figure
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import awswrangler as wr

from analysis.enum import Colors
from analysis.households import Households

HOUSE_TYPE_MAPPING = {
    'Detached house': 'detached', 
    'Corner house': 'corner', 
    'Semi-detached house': 'semi-detached', 
    'rowhouse': 'terraced',
    'Terraced house': 'terraced',
    'Apartment': 'apartment'
}


class DailyUsage:
    """Helper class to collect and query the Dialy Usage dataset"""
    df: pd.DataFrame
    client_id: str
    _from: date
    _to: date

    QUERY: str = """SELECT du.household_id, du.date, du.type, du.usage, hh.resident_count, hh.house_type
FROM usage.vw_daily_usage du 
JOIN usage.vw_households hh ON hh.id = du.household_id 
WHERE hh.account_status = 'Active' 
AND hh.client_id = '{client_id}'
AND du.date >= '{_from}' AND du.date < '{_to}'"""

    def __init__(self, client_id: str, from_: date, to_: date) -> None:
        self.client_id = client_id
        self._from = from_
        self._to = to_

    def collect_data(self, session):
        df = wr.athena.read_sql_query(
            sql=self.QUERY.format(
                client_id=self.client_id,
                _from=self._from.strftime('%Y-%m-%d'),
                _to=self._to.strftime('%Y-%m-%d'),
            ),
            database="usage",
            s3_output="s3://slimwonen-athena-queries/",
            workgroup="primary",
            boto3_session=session,
        )
        df['date'] = pd.to_datetime(df['date'])
        self.df = df
        self._pivoted = df.groupby(['household_id', 'type'])['usage'].sum().unstack(fill_value=0)
    
    @property
    def total_backfeed(self) -> float:
        """Calculate the total backfeed for this dataset"""
        return self.df[self.df['type'] == 'backfeed']['usage'].sum()

    def electricity_consumption_on_date(self, day: date, debug: bool = False) -> pd.DataFrame:
        """Return a dataset with electricity consumption on the given day
        Only consider households with > 1kWh to avoid diluting the average
        Only consider households with > 1 residents
        """
        df_pivoted = self.df[self.df['date'] == datetime(day.year, day.month, day.day)].groupby(['household_id', 'type']).agg({
            'usage': 'sum',
            'resident_count': 'min',
        }).unstack(fill_value=0)

        # Flatten the MultiIndex columns and rename
        df_pivoted.columns = ['_'.join(col).strip() for col in df_pivoted.columns.values]
        df_pivoted.reset_index(inplace=True)
        df_pivoted.drop(columns=['resident_count_gas', 'resident_count_backfeed'], inplace=True)
        df_pivoted.rename(columns={'resident_count_electricity': 'resident_count'}, inplace=True)
        df_pivoted.columns = [c.removeprefix('usage_') for c in df_pivoted.columns]

        # Add house type
        df_pivoted = df_pivoted.merge(self.df[['household_id', 'house_type']].drop_duplicates(), on='household_id', how='left')
        df_pivoted['house_type'] = df_pivoted['house_type'].replace(HOUSE_TYPE_MAPPING)

        df_pivoted['has_solar'] = df_pivoted['backfeed'] > 0
        df_pivoted['elec_usage'] = df_pivoted['electricity'] + df_pivoted['backfeed'] * .3 / .7

        if debug:
            print(f'{len(df_pivoted)} records with data')
        df_pivoted = df_pivoted[df_pivoted['elec_usage'] > 1]
        if debug:
            print(f'{len(df_pivoted)} remaining with more than 1 kWh usage')

        df_pivoted = df_pivoted[df_pivoted['house_type'].isin(('detached', 'semi-detached', 'corner'))]
        if debug:
            print(f'{len(df_pivoted)} remaining with house type (semi-)detached')
        return df_pivoted.sort_values(by='elec_usage').copy()

    def average_electricity_consumption_on_date(self, day: date) -> float:
        """Calculate the average consumption on the given day
        Only consider households with > 1kWh to avoid diluting the average
        Only consider households with > 1 residents
        """
        day_usage = self.df[(self.df['date'] == datetime(day.year, day.month, day.day)) & (self.df['type'] == 'electricity')]
        day_usage = day_usage[day_usage['usage'] > 1]
        day_usage = day_usage[day_usage['resident_count'] > 1]
        print(f'Calculating average for {len(day_usage)} records')
        return day_usage['usage'].mean()


class DailyUsagePlots:
    """Helper class to generate plots from the DialyUsage dataset"""

    du: DailyUsage
    G2KWH_FACTOR: float = 9.77

    def __init__(self, du: DailyUsage):
        self.du = du

    def plot_daily_records(self):
        """
        Plot daily gas and electricity counts to identify gaps or changes in the data
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame with columns: 'date', 'type', 'usage'
            Types should include 'gas', 'electricity', and optionally 'backfeed'
        
        Returns:
        --------
        fig, axes : matplotlib figure and axes objects
        """

        # Calculate daily totals for gas and electricity
        daily_gas = self.du.df[self.du.df['type'] == 'gas'].groupby('date').size().sort_index()
        daily_elec = self.du.df[self.du.df['type'] == 'electricity'].groupby('date').size().sort_index()
        
        fig, ax = plt.subplots(1, 1, figsize=(14, 8), sharex=True)
        
        ax.plot(daily_gas.index, daily_gas.values, 
                color=Colors.GAS, linewidth=2, label='Gas records', alpha=0.8)
        ax.plot(daily_elec.index, daily_elec.values, 
                color=Colors.ELECTRICITY, linewidth=2, label='Electricity records', alpha=0.8)
        
        ax.set_ylabel('Aantal metingen', fontsize=11, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.set_title('Dagelijks aantal metingen', fontsize=13, fontweight='bold')
        ax.legend(loc='upper left')
        ax.set_ylim([0, 1.1 * max(daily_gas.max(), daily_elec.max())])
        
        plt.tight_layout()
        return fig, ax

    def daily_usage_plot(self, title: str, extrapolate_for_households: Households | None = None) -> Figure:
        """
        Plot daily gas and electricity usage in kWh.
        
        Parameters:
        -----------
        title : str
            Plot title
        extrapolate_for_households : Households, optional
            If provided the values will be extrapolated to entire council 
        
        Returns:
        --------
        fig : matplotlib figure object
        """
        # Ensure date column is datetime
        df = self.du.df.copy()
        df['date'] = pd.to_datetime(df['date'])
        
        # Calculate daily totals for gas and electricity
        daily_gas = df[df['type'] == 'gas'].groupby('date')['usage'].sum().sort_index()
        daily_elec = df[df['type'] == 'electricity'].groupby('date')['usage'].sum().sort_index()

        if extrapolate_for_households:
            daily_gas = pd.Series({idx: val * extrapolate_for_households.num_households / extrapolate_for_households.num_active_households(idx) for idx, val in daily_gas.items()})
            daily_elec = pd.Series({idx: val * extrapolate_for_households.num_households / extrapolate_for_households.num_active_households(idx) for idx, val in daily_elec.items()})
        
        # Convert gas to kWh
        daily_gas_kwh = daily_gas * self.G2KWH_FACTOR
        
        # Create figure with subplots
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Plot gas usage (converted to kWh)
        ax.plot(daily_gas_kwh.index, daily_gas_kwh.values, 
                color=Colors.GAS, linewidth=2, label='Gas', alpha=0.8)
        
        # Plot electricity usage
        ax.plot(daily_elec.index, daily_elec.values, 
                color=Colors.ELECTRICITY, linewidth=2, label='Elektriciteit', alpha=0.8)
        
        ax.set_ylabel('Energieverbruik (kWh)', fontsize=11, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.set_title(title, fontsize=13, fontweight='bold')
        ax.legend(loc='upper right')
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.set_ylim([0, 1.1 * daily_gas_kwh.max()])
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        plt.tight_layout()
        return fig

    def daily_backfeed_plot(self, title: str) -> Figure:
        """
        Plot daily electricity backfeed figures in kWh.
        
        Parameters:
        -----------
        title : str
            Plot title
        
        Returns:
        --------
        fig : matplotlib figure object
        """
        # Ensure date column is datetime
        df = self.du.df.copy()
        df['date'] = pd.to_datetime(df['date'])
        
        # Calculate daily totals
        backfeed = df[df['type'] == 'backfeed'].groupby('date')['usage'].sum().sort_index()

        # Estimate production totals
        production = backfeed / .7

        # Create figure with subplots
        fig, ax = plt.subplots(figsize=(12, 6))
        
        ax.plot(backfeed.index, backfeed.values,
            color=Colors.SUN, linewidth=2, label='Teruglevering', alpha=0.8)
        
        ax.plot(production.index, production.values, 
            color=Colors.DARKBLUE, linewidth=2, label='Opwek', alpha=0.8)
        
        ax.set_ylabel('Teruglevering en opwek (kWh)', fontsize=11, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.set_title(title, fontsize=13, fontweight='bold')
        ax.legend(loc='upper right')
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.set_ylim([0, 1.1 * production.max()])
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        plt.tight_layout()
        return fig

    def daily_usage_backfeed_plot(self, title: str, total: bool = False) -> Figure:
        """
        Plot daily electricity/energy usage and backfeed figures in kWh.
        
        Parameters:
        -----------
        title : str
            Plot title
        total : bool
            Plot total energy (electricity + gas converted) or electricity only
        
        Returns:
        --------
        fig : matplotlib figure object
        """
        df = self.du.df.copy()
        df['date'] = pd.to_datetime(df['date'])
        
        pivoted = df.groupby(['date', 'type'])['usage'].sum().unstack(fill_value=0)
        pivoted['energy'] = pivoted['electricity'] + pivoted['gas'] * self.G2KWH_FACTOR
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        ax.plot(pivoted.index, pivoted.backfeed,
            color=Colors.SUN, linewidth=2, label='Teruglevering', alpha=0.8)
        
        usage = pivoted.energy if total else pivoted.electricity
        label = 'Energieverbruik' if total else 'Elektriciteitsverbruik'
        ax.plot(pivoted.index, usage, 
            color=Colors.ELECTRICITY, linewidth=2, label=label, alpha=0.8)
        
        ax.set_ylabel('Teruglevering en verbruik (kWh)', fontsize=11, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.set_title(title, fontsize=13, fontweight='bold')
        ax.legend(loc='upper right')
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.set_ylim([0, 1.1 * max(usage.max(), pivoted.backfeed.max())])
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        plt.tight_layout()
        return fig

    def daily_avg_app_usage_plot(self, title: str, households: Households) -> Figure:
        """
        Plot daily average energy consumption, plot for 4 levels of app users
        
        Parameters:
        -----------
        title : str
            Plot title
        household : Households
            household object with app usage information
        
        Returns:
        --------
        fig : matplotlib figure object
        """

        if 'logins' not in households.df.columns:
            print('No login info, enrich houshold data first')
            return
    
        df = self.du.df.copy()
        df['date'] = pd.to_datetime(df['date'])

        with_login = pd.merge(df, households.df[['id', 'logins']], left_on='household_id', right_on='id', how='left')

        use_none = with_login[with_login['logins'] == 0]
        mean_none = use_none.groupby(['date', 'type'])['usage'].mean().unstack(fill_value=0)
        mean_none['energy'] = mean_none['electricity'] + mean_none['gas'] * self.G2KWH_FACTOR

        use_monthly = with_login[with_login['logins'] > 9]
        mean_monthly = use_monthly.groupby(['date', 'type'])['usage'].mean().unstack(fill_value=0)
        mean_monthly['energy'] = mean_monthly['electricity'] + mean_monthly['gas'] * self.G2KWH_FACTOR

        fig, ax = plt.subplots(figsize=(12, 6))
        
        ax.plot(mean_none.index, mean_none.energy,
            color=Colors.GAS, linewidth=2, label='Geen App Gebruik', alpha=0.8)
        
        ax.plot(mean_monthly.index, mean_monthly.energy, 
            color=Colors.GREEN, linewidth=2, label='Maandelijks App Gebruik', alpha=0.8)
        
        ax.set_ylabel('Energyverbruik (kWh)', fontsize=11, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.set_title(title, fontsize=13, fontweight='bold')
        ax.legend(loc='upper right')
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.set_ylim([0, 1.1 * mean_none.energy.max()])
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        plt.tight_layout()

        # Calculate saving potential
        merged = mean_none.merge(mean_monthly.reset_index()[['date','energy']], on='date')
        print(f'Active app users use {(merged['energy_x'] - merged['energy_y']).mean():.02f} kWh per day less')
        return fig
