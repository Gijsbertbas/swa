import os

import pandas as pd
from src.supabase import SupabaseClient
from datetime import date, timedelta


class Extractor:
    def __init__(self, client: SupabaseClient, path: str = 'extracted'):
        self.client = client
        self.path = path

    def get_daily_usage_data_since_date(self, date: date):
        """Extract daily usage data from Supabase since a specific date up until yesterday.
        Storing as multiline JSON files in the unpacked/supabase_daily_usage directory.
        """
        output_path = os.path.join(self.path, 'daily_usage_data')
        os.makedirs(output_path, exist_ok=True)
        
        while report_date < date.today():
            date_str = report_date.strftime('%Y-%m-%d')
            print(f"Fetching data for {date_str}")
            
            df = self.client.get_daily_usage_data_for_date(date_str)
            df['date'] = df['date'].astype(str)  # Ensure date is in string format
            df.to_json(f'{output_path}/supabase_{date_str}.json', orient='records', lines=True, default_handler=str)

            report_date += timedelta(days=1)

    def get_household_data(self):
        """Extract household data from Supabase and store it in a JSON file."""
        output_path = os.path.join(self.path, 'supabase_households')
        os.makedirs(output_path, exist_ok=True)

        print("Fetching household data from Supabase...")
        alle_adressen = pd.read_csv('data/alle_adressen.csv', sep=';', low_memory=False, encoding='utf-8')
        households = self.client.get_household_data()

        households.loc[households['house_number'].isna() | households['house_number'].isin([float('inf'), float('-inf')]), 'house_number'] = -1
        households['house_number'] = households['house_number'].astype('int')

        households['address'] = f"{households['zipcode']}#{households['house_number'].astype(str)}"
        alle_adressen['address'] = f"{alle_adressen['Postcode']}#{alle_adressen['Huisnummer'].astype(str)}"
        alle_adressen.drop_duplicates(subset='address', inplace=True)
        households = households.merge(alle_adressen[['address', 'lon', 'lat']], on='address', how='left').drop(columns='address')

        households['date_of_activation'] = households['date_of_activation'].dt.date.astype(str)

        households.to_json(f'{output_path}/households_for_analysis.json', orient='records', lines=True, default_handler=str)
        print(f'Household data saved to {output_path}/households_for_analysis.json')