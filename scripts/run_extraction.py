from datetime import date
from src.extract import HouseholdDataExtractor, DailyUsageDataExtractor
from settings import SupabaseSettings
from src.supabase import SupabaseClient
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data extraction.")
    parser.add_argument(
        "extractor",
        choices=["household", "daily_usage"],
        help="Select which extractor to run: 'household' or 'daily_usage'",
    )
    parser.add_argument(
        "--since-date",
        type=lambda s: date.fromisoformat(s),
        default=None,
        help="Date (YYYY-MM-DD) to start extracting daily usage data (only for daily_usage)",
    )
    args = parser.parse_args()

    settings = SupabaseSettings()
    client = SupabaseClient(settings)

    if args.extractor == "household":
        extractor = HouseholdDataExtractor(client, path="extracted")
        extractor.extract()
    elif args.extractor == "daily_usage":
        extractor = DailyUsageDataExtractor(client, path="extracted")
        since_date = args.since_date or date.today()
        extractor.get_daily_usage_data_since_date(since_date)
