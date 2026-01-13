import pandas as pd
from sqlalchemy import create_engine

from settings import SupabaseSettings


class SupabaseClient:
    def __init__(self, settings: SupabaseSettings):
        self.url = f"postgresql://postgres:{settings.pwd}@{settings.url}:5432/postgres"
        self.engine = create_engine(self.url)

    def get_daily_usage_data_for_date(self, date: str) -> pd.DataFrame:
        """
        Fetch daily usage data for a specific date.
        :param date: Date in 'YYYY-MM-DD' format.
        :return: List of daily usage records.
        """
        sql = f"SELECT household_id, household_activation_code as activation_code, date, type, usage FROM public.daily_usage_data WHERE date = '{date}'"
        return pd.read_sql(sql, self.engine)

    def get_household_data(self) -> pd.DataFrame:
        """
        Fetch household data.
        :return: DataFrame containing household records.
        """
        sql = """
        SELECT 
            ph.id, ph.activation_code, ph.account_status, ph.client_id, c.title as client_title, cg.name as client_group, 
            ph.date_of_activation, ph.gas_ean, ph.electricity_ean, 
            phd.build_year, phd.square_meters, phd.house_type, phd.heating_type, phd.gas_connection, 
            phd.gas_or_induction, phd.water_heating_type, phd.resident_count,
            hd.zipcode, hd.house_number, hd.house_number_addition, hd.housing_corporation
        FROM public.households ph 
        LEFT JOIN public.clients c ON c.id = ph.client_id
        LEFT JOIN public.household_house_details phd ON phd.household_id = ph.id 
        LEFT JOIN public.household_details hd ON hd.household_id = ph.id 
        LEFT JOIN public.client_groups cg ON cg.id = ph.client_group_id
        """
        return pd.read_sql(sql, self.engine)
