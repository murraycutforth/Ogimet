from pathlib import Path
import logging
from datetime import datetime

from tqdm import tqdm
import pandas as pd
from ogimet import Downloader

logger = logging.getLogger(__name__)


def get_station_df():

    df = pd.read_csv("uk-station-data.csv", header=0)
    
    def convert_to_decimal(lat_str):
        parts = list(map(int, lat_str[:-1].split('-')))
        degrees = parts[0]
        minutes = parts[1]
        direction = 1 if lat_str[-1] == 'N' else -1  # Positive for 'N', negative for 'S'
        return (degrees + minutes / 60) * direction
    
    # Apply the conversion function to the 'Latitude' column
    df['DecimalLatitude'] = df['Latitude'].apply(convert_to_decimal)
    
    
    # Everything in the highlands is further N of 55.7 lat (roughly in Glasgow)
    
    lat_min = 55.7
    
    df = df[df["DecimalLatitude"] > lat_min]
    
    
    # Convert Established and Closed cols to datetime
    
    df["Established"] = pd.to_datetime(df["Established"], errors="coerce")
    df["Closed"] = pd.to_datetime(df["Closed"], errors="coerce")
    
    
    # Throw away stations with missing WIGOS IDs
    df = df[df["WIGOS ID"] != "0-0-0-MISSING"]
    
    return df


def download_all(df):
    def iterate_monthly_intervals(start_date, end_date):
        current_date = start_date

        while current_date <= end_date:
            yield current_date.year, current_date.month

            # Increment current_date by one month
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)

    start_date = datetime(2000, 1, 1)
    end_date = datetime(2022, 12, 31)

    for wmo in tqdm(df["WMO INDEX"]):
        for year, month in iterate_monthly_intervals(start_date, end_date):
            D = Downloader()

            try:
                D.running_all(int(year), int(month), int(year), int(month), wmo)
            except FileExistsError:
                logger.info(f"Skipping {wmo}, {year}-{month} as file already exists")
            


def main():
    logging.basicConfig(level=logging.DEBUG)
    df = get_station_df()
    download_all(df)


main()
