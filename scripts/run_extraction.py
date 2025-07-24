from datetime import date
from src.extract import Extractor
from settings import SupabaseSettings
from src.supabase import SupabaseClient

if __name__ == "__main__":
    settings = SupabaseSettings()
    client = SupabaseClient(settings)
    extractor = Extractor(client, path='extracted')

    # Extract daily usage data since a specific date
    # extractor.get_daily_usage_data_since_date(date(2025, 6, 26))

    # Extract household data
    extractor.get_household_data()
