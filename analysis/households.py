from datetime import date, datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import awswrangler as wr
import json

import folium
from branca.colormap import linear


class Households:
    """Helper class to collect and query a subset of households households"""
    df: pd.DataFrame
    client_id: str
    _from: date
    _to: date
    solar: pd.DataFrame | None = None
    posthog: pd.DataFrame | None = None

    QUERY: str = "SELECT * FROM usage.vw_households where client_id = '{client_id}'"

    SOLAR_QUERY: str = """SELECT h.client_id, h.id, sum(du.usage) > 0 AS has_solar FROM households h 
JOIN daily_usage du ON du.household_id = h.id 
WHERE du.date LIKE '{period}%' AND du.type = 'backfeed' 
GROUP BY (h.client_id, h.id);"""

    POSTHOG_QUERY: str = """
SELECT up.timestamp, up.properties 
FROM usage.posthog up 
WHERE up.timestamp > DATE '{from_}' 
  AND up.timestamp < DATE '{to_}' 
  AND up.event = 'dashboard_viewed';
"""

    def __init__(self, client_id: str, from_: date, to_: date) -> None:
        self.client_id = client_id
        self._from = from_
        self._to = to_

    def collect_data(self, session):
        households = wr.athena.read_sql_query(
            sql=self.QUERY.format(client_id=self.client_id),
            database="usage",
            s3_output="s3://slimwonen-athena-queries/",
            workgroup="primary",
            boto3_session=session,
        )
        households['date_of_activation'] = pd.to_datetime(households['date_of_activation'])
        self.df = households

    def collect_solar(self, session):
        """Collect a dataframe with client_id, household_id, has_solar
        Based on backfeed data of the most recent summer"""
        year = date.today().year if date.today().month > 9 else date.today().year -1
        period = date(year=year, month=8, day=1).strftime('%Y-%m')

        print(f'Collecting solar data for {period}')

        self.solar = wr.athena.read_sql_query(
            sql=self.SOLAR_QUERY.format(period=period),
            database="usage",
            s3_output="s3://slimwonen-athena-queries/",
            workgroup="primary",
            boto3_session=session,
        )

    def collect_posthog(self, session):
        """Collect posthog rows, to be used to enrich the houshold dataframe"""

        self.posthog = wr.athena.read_sql_query(
            sql=self.POSTHOG_QUERY.format(
                from_=self._from.strftime('%Y-%m-%d'),
                to_=self._to.strftime('%Y-%m-%d')
            ),
            database="usage",
            s3_output="s3://slimwonen-athena-queries/",
            workgroup="primary",
            boto3_session=session,
        )

    def enrich(self, feature: str):
        """Enrich the household table with additional columns
        Meant to be extended with various types of enrichments"""
        match feature:
            case 'has_solar':
                if self.solar is None:
                    print('No solar data available, please collect solar first')
                    return
                self.df = self.df[[c for c in self.df.columns if not c.startswith(feature)]]
                self.df = pd.merge(self.df, self.solar[['id', 'has_solar']], on='id', how='left')
                print(f'{sum(pd.isna(self.df[self.df['account_status'] == 'Active']['has_solar']))} active households without solar info')
            case 'logins':
                if self.posthog is None:
                    print('No posthog data available, please collect posthog first')
                    return
                def add_household(row: pd.Series) -> pd.Series:
                    props = json.loads(row['properties'])
                    if 'household_id' in props:
                        row['household_id'] = props['household_id']
                    return row
                self.posthog = self.posthog.apply(add_household, axis=1)
                logins = self.posthog.groupby('household_id').count().reset_index()
                logins = logins[['household_id', 'timestamp']]
                logins.rename(columns={'household_id': 'id', 'timestamp': 'logins'}, inplace=True)
                
                self.df = pd.merge(self.df, logins, on='id', how='left')
                self.df['logins'] = self.df['logins'].fillna(0)
            case _:
                print(f'Feature {feature} not supported yet')
    
    @property
    def num_households(self) -> int:
        return len(self.df)
    
    def num_active_households(self, day: str | date | datetime) -> int:
        """Count the number of active households on the given date"""
        day = pd.to_datetime(day)
        return (self.df['date_of_activation'] <= day).sum()
    

class HouseholdPlots:
    """Plots to analyse a group of households"""
    hh: Households
    ZOOM: float = 11

    def __init__(self, hh: Households) -> None:
        self.hh = hh

    def plot_active_accounts(self, start_date: str, end_date: str):
        """
        Plot the cumulative number of active accounts over time.
        
        For each date, counts all rows where the activation date is on or before that date.
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame containing the data
        date_column : str
            Name of the column containing activation dates
        start_date : str or pd.Timestamp
            Start date for the plot (inclusive)
        end_date : str or pd.Timestamp
            End date for the plot (inclusive)
        
        Returns:
        --------
        fig, ax : matplotlib figure and axis objects
        """
        # Convert start and end dates to datetime
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # Create a complete date range
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # For each date, count how many accounts were activated on or before that date
        cumulative_counts = []
        for date in date_range:
            count = self.hh.num_active_households(date)
            cumulative_counts.append(count)
        
        # Create the plot
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(date_range, cumulative_counts, marker='o', linestyle='-', linewidth=1.5, markersize=3)
        
        ax.set_ylabel('Number of Active Accounts')
        ax.set_title(f'Active Accounts from {start_date.date()} to {end_date.date()}')
        ax.grid(True, alpha=0.3)
        
        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        return fig, ax

    def solar_per_neighbourhood_map(self) -> folium.Map:
        """Map labeled circles at neighbours ('wijk') illustrating the 
        relative number of solar panel holders, % illustrated by color"""

        if 'has_solar' not in self.hh.df.columns:
            print('No solar info, enrich houshold data first')
            return
        
        df = self.hh.df[(self.hh.df['account_status'] == 'Active') & (~pd.isna(self.hh.df['has_solar']))].copy()

        mapping = {}
        for wijk, group in df.groupby('wijk'):
            if group.empty:
                continue
            mapping[wijk] = (group['lat'].mean(), group['lon'].mean(), sum(group['has_solar']) / len(group) * 100)

        MEAN_LAT = sum((k[0] for k in mapping.values())) / len(mapping)
        MEAN_LON = sum((k[1] for k in mapping.values())) / len(mapping)
        m = folium.Map(location=[MEAN_LAT, MEAN_LON], zoom_start=self.ZOOM)

        RATIO_MIN = min((k[2] for k in mapping.values()))
        RATIO_MAX = max((k[2] for k in mapping.values()))

        colormap = linear.viridis.scale(np.floor(RATIO_MIN/10) * 10, np.ceil(RATIO_MAX/10) * 10)
        # colormap.caption = 'Zonnepanelen per wijk (%)'

        for wijk, stats in mapping.items():
            popup_text = f"""
            <b>{wijk}</b><br>
            {stats[2]:.1f}%
            """
            
            # Get color based on ratio
            color = colormap(stats[2])
            
            folium.CircleMarker(
                location=[stats[0], stats[1]],
                radius=25,
                popup=popup_text,
                color=color,
                fill=True,
                fillColor=color,
                fillOpacity=0.7
            ).add_to(m)
            
            # Add a label below the marker
            folium.Marker(
                location=[stats[0], stats[1]],
                icon=folium.DivIcon(html=f'''
                    <div style="font-size: 14pt; color: black; 
                                font-weight: bold; text-align: center;
                                text-shadow: 1px 1px 2px white, -1px -1px 2px white,
                                            1px -1px 2px white, -1px 1px 2px white;
                                margin-top: 15px;">
                        {wijk}
                    </div>
                ''')
            ).add_to(m)

        colormap.add_to(m)
        return m
