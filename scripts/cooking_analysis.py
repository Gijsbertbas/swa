import boto3
import json
from pprint import pprint
import awswrangler as wr
import pandas as pd
from datetime import datetime, time
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm



def get_household_data():
    households = pd.read_csv('../data/spa_households.csv', sep=';')
    households.rename(columns={'activation_code': 'activationkey'}, inplace=True)
    households['date_of_activation'] = pd.to_datetime(households['date_of_activation'], format='ISO8601', utc=False).dt.tz_localize(None)

    households = households[~pd.isna(households['gas_or_induction'])]
    households['gas_or_induction'] = households['gas_or_induction'].replace({'Inductie': 'Induction'})
    return households


def get_hour_data_for_keys(keys: tuple, session):
    # Running this query made the kernel crash on the 250M+ rows that were returned, it left a temporary table which can be queried

    query = f"""SELECT 
    p4a.activationkey, p4h.datetime, p4h.electricitymeasurement, p4h.gasmeasurement
    FROM swa.gcs_p4_hour_data_2024 p4h
    LEFT JOIN swa.p4aggregation p4a ON p4h.houseid = p4a.houseid 
    WHERE p4a.activationkey in {keys}
    AND p4h.datetime BETWEEN '2024-06-01' AND '2024-07-01'
    """

    df = wr.athena.read_sql_query(
        sql=query,
        database="swa",
        s3_output="s3://gbstraathof-athena-queries/",
        workgroup="primary",
        boto3_session=session,
    )
    df['datetime'] = pd.to_datetime(df['datetime'], format='ISO8601', utc=False).dt.tz_localize(None)
    df = df[df['datetime'].dt.time.between(time(17), time(19), inclusive='both')]
    return df


def process_hour_data(df):
    def read_meter(row: pd.Series) -> pd.Series:
        if not pd.isna(row['gasmeasurement']):
            row['gas'] = row['gasmeasurement']['meter']
        if not pd.isna(row['electricitymeasurement']):
            row['elec'] = row['electricitymeasurement']['meter']
        return row

    df = df.progress_apply(read_meter, axis=1)
    df = df[['activationkey', 'datetime', 'gas', 'elec']]
    return df
