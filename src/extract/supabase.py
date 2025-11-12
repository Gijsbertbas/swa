import os

import pandas as pd
from src.supabase import SupabaseClient
from datetime import date, timedelta

from src.utils import categorise_build_years, categorise_square_meters


class SupabaseExtractor:
    def __init__(self, client: SupabaseClient, path: str = "extracted"):
        self.client = client
        self.path = path

    def extract(self, *args, **kwargs):
        raise NotImplementedError


class DailyUsageDataExtractor(SupabaseExtractor):
    def extract(self, report_date: date):
        """Extract daily usage data from Supabase since a specific date up until yesterday.
        Storing as multiline JSON files in the self.path/daily_usage directory.
        """
        output_path = os.path.join(self.path, "daily_usage_data")
        os.makedirs(output_path, exist_ok=True)

        while report_date < date.today():
            date_str = report_date.strftime("%Y-%m-%d")
            print(f"Fetching data for {date_str}")

            df = self.client.get_daily_usage_data_for_date(date_str)
            df["date"] = df["date"].astype(str)  # Ensure date is in string format
            df.to_json(
                f"{output_path}/supabase_{date_str}.json",
                orient="records",
                lines=True,
                default_handler=str,
            )

            report_date += timedelta(days=1)


class HouseholdDataExtractor(SupabaseExtractor):
    def extract(self):
        """Extract household data from Supabase and store it in a JSON file."""
        output_path = os.path.join(self.path, "supabase_households")
        os.makedirs(output_path, exist_ok=True)

        print("Fetching household data from Supabase...")
        households = self.client.get_household_data()

        households = self._enrich(households)

        households.to_json(
            f"{output_path}/households_for_analysis.json",
            orient="records",
            lines=True,
            default_handler=str,
        )
        print(f"Household data saved to {output_path}/households_for_analysis.json")

    def _enrich(self, households: pd.DataFrame) -> pd.DataFrame:
        print("Enriching household data...")
        print("\t> Updating existing columns...")
        households = households.apply(categorise_build_years, axis=1)
        households = households.apply(categorise_square_meters, axis=1)

        households["date_of_activation"] = households[
            "date_of_activation"
        ].dt.date.astype(str)

        households.loc[
            households["house_number"].isna()
            | households["house_number"].isin([float("inf"), float("-inf")]),
            "house_number",
        ] = -1
        households["house_number"] = households["house_number"].astype("int")

        print("\t> Adding coordinates...")
        # Add longitude and latitude from alle_adressen.csv
        households["address"] = (
            households["zipcode"].astype(str)
            + "#"
            + households["house_number"].astype(str)
        )
        alle_adressen = pd.read_csv(
            "data/alle_adressen.csv", sep=";", low_memory=False, encoding="utf-8"
        )
        alle_adressen["address"] = (
            alle_adressen["Postcode"].astype(str)
            + "#"
            + alle_adressen["Huisnummer"].astype(str)
        )
        alle_adressen.drop_duplicates(subset="address", inplace=True)
        households = households.merge(
            alle_adressen[["address", "lon", "lat"]], on="address", how="left"
        ).drop(columns="address")

        print("\t> Adding energielabel and buurtnaam (Drenthe only)...")
        # Add energielabel and buurtnaam from openinfo data
        households["address"] = (
            households["zipcode"].astype(str)
            + "#"
            + households["house_number"].astype(str)
            + households["house_number_addition"].fillna("").astype(str)
        )
        drenthe = pd.read_csv(
            "data/adresgegevens-provincie-drenthe-2024.csv", sep=",", low_memory=False, encoding="utf-8"
        )
        drenthe["address"] = (
            drenthe["Postcode"].astype(str)
            + "#"
            + drenthe["Huisnummer"].astype(str)
            + drenthe["Huisletter"].fillna("").astype(str)
        )
        drenthe.drop_duplicates(subset="address", inplace=True)
        households = households.merge(
            drenthe[["address", "Pand energielabel"]],
            on="address", how="left"
        ).drop(columns="address")  # Energylabel must be as exact as possible
        drenthe.drop_duplicates(subset="Postcode", inplace=True)
        households = households.merge(
            drenthe[["Postcode", "Buurtnaam", "Wijknaam"]],
            left_on="zipcode", right_on="Postcode", how="left"
        ).drop(columns="Postcode")  # Buurtnaam and Wijknaam can be on postcode level
        households.rename(columns={"Pand energielabel": "energy_label", "Buurtnaam": "buurt", "Wijknaam": "wijk"}, inplace=True)

        return households
