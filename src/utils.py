import pandas as pd


def categorise_build_years(row: pd.Series) -> pd.Series:
    bouwjaar = None
    if row["build_year"] < 1950:
        bouwjaar = "tot 1950"
    elif 1950 <= row["build_year"] < 1975:
        bouwjaar = "1950-1975"
    elif 1975 <= row["build_year"] < 1992:
        bouwjaar = "1975-1992"
    elif 1992 <= row["build_year"] < 2006:
        bouwjaar = "1992-2006"
    elif 2006 <= row["build_year"] <= 2015:
        bouwjaar = "2006-2015"
    elif 2015 < row["build_year"]:
        bouwjaar = "na 2015"

    row["build_year_cat"] = bouwjaar
    return row


square_meters_categories = {
    "< 100m2": lambda x: x < 100,
    "100-120 m2": lambda x: 100 <= x < 120,
    "120-150 m2": lambda x: 120 <= x < 150,
    "150-200 m2": lambda x: 150 <= x < 200,
    "> 200m2": lambda x: x >= 200,
}


def categorise_square_meters(row: pd.Series) -> pd.Series:
    if pd.isna(row["square_meters"]):
        row["square_meters_cat"] = None
        return row
    opp = int(row["square_meters"])
    for o, f in square_meters_categories.items():
        if f(opp):
            row["square_meters_cat"] = o
            return row
