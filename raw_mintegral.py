import time
import requests
import pandas as pd
from hashlib import md5
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from io import StringIO
import re

from airflow.models import Variable


class MintegralSitesFetcher:
    def __init__(self):
        # берем ключи из Airflow Variables
        self.api_key_1    = Variable.get("mintegral_api_key_1")
        self.access_key_1 = Variable.get("mintegral_access_key_secret_1")
        self.api_key_2    = Variable.get("mintegral_api_key_2")
        self.access_key_2 = Variable.get("mintegral_access_key_secret_2")
        # строка подключения к БД
        self.db_url       = Variable.get("postgresql_secret")

        # создаём SQLAlchemy-движок
        self.engine = create_engine(self.db_url)

    def gen_headers(self, access_key: str, api_key: str) -> dict:
        timestamp = int(time.time())
        encoded_timestamp = md5(str(timestamp).encode()).hexdigest()
        token = md5((api_key + encoded_timestamp).encode()).hexdigest()
        return {
            "access-key": access_key,
            "api-key":    api_key,
            "token":      token,
            "timestamp":  str(timestamp),
            "Content-Type": "application/json"
        }

    def fetch_data(self, access_key: str, api_key: str,
                   start_date: str, end_date: str) -> pd.DataFrame:
        url = 'https://ss-api.mintegral.com/api/v2/reports/data'
        headers = self.gen_headers(access_key, api_key)
        params = {
            'start_time':       start_date,
            'end_time':         end_date,
            'timezone':         '+0',
            'type':             '1',
            'dimension_option': 'Day,Offer,Package,Sub'
        }

        # инициация генерации отчёта
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Failed to initiate data generation: {response.status_code}")
            return pd.DataFrame()

        # ждём, пока отчёт не станет готов
        while True:
            time.sleep(20)
            params['type'] = '2'
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                print("An error occurred.")
                return pd.DataFrame()

            try:
                payload = response.json()
                # код 204 означает «ещё не готово»
                if payload.get('code') == 204:
                    print("Data is not ready, please wait...")
                    continue
                # если JSON с другими кодами — выходим
                print(f"Unexpected JSON response: {payload}")
                return pd.DataFrame()
            except ValueError:
                # если не JSON — пришла таблица
                text = response.text.strip()
                if not text:
                    print("Empty or invalid response.")
                    return pd.DataFrame()
                return pd.read_csv(StringIO(text), sep='\t')

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            print("No data to process.")
            return df

        df = df[[
            'Date', 'Offer Name', 'Sub Id', 'Package Name',
            'Impression', 'Click', 'Conversion', 'Spend'
        ]].copy()

        df.rename(columns={
            'Date': 'day',
            'Offer Name': 'campaign_network',
            'Sub Id': 'site_network',
            'Package Name': 'site_name_network',
            'Impression': 'impressions',
            'Click': 'clicks',
            'Conversion': 'conversions',
            'Spend': 'spend'
        }, inplace=True)

        df['client'] = 'Other'
        df.loc[df['campaign_network']
               .str.contains(r'clinet1',
                             flags=re.IGNORECASE, na=False),
               'client'] = 'liga_stavok'
        df.loc[df['campaign_network']
               .str.contains(r'clinet2', flags=re.IGNORECASE, na=False),
               'client'] = 'betboom'
        df.loc[df['campaign_network']
               .str.contains(r'clinet3', flags=re.IGNORECASE, na=False),
               'client'] = 'winline'

        df = df[df['client'] != 'Other'].copy()
        df['day'] = pd.to_datetime(df['day'],
                                   format='%Y%m%d',
                                   errors='coerce') \
                       .dt.strftime('%Y-%m-%d')

        df['key_network'] = (
            df['day']
            + df['campaign_network']
            + df['site_network']
        )
        df.drop_duplicates(subset=['key_network'], inplace=True)
        return df

    def save_to_database(self, df: pd.DataFrame):
        if df.empty:
            print("No data to save.")
            return

        with self.engine.begin() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_mint (
                    day               DATE,
                    campaign_network  TEXT,
                    site_network      TEXT,
                    site_name_network TEXT,
                    impressions       NUMERIC,
                    clicks            NUMERIC,
                    conversions       NUMERIC,
                    spend             NUMERIC,
                    client            TEXT,
                    key_network       TEXT PRIMARY KEY
                );
            """)
            conn.execute("""
                DELETE FROM raw_mint
                 WHERE day IN (
                   CURRENT_DATE - INTERVAL '1 day',
                   CURRENT_DATE - INTERVAL '2 days'
                 );
            """)
            df.to_sql(
                'raw_mint',
                conn,
                if_exists='append',
                index=False
            )
            print(f"Inserted {len(df)} rows into the database.")

    def process_and_save(self):
        start_date = (datetime.today() - timedelta(days=2)) \
                         .strftime('%Y-%m-%d')
        end_date   = (datetime.today() - timedelta(days=1)) \
                         .strftime('%Y-%m-%d')

        df1 = self.fetch_data(self.access_key_1,
                              self.api_key_1,
                              start_date, end_date)
        df2 = self.fetch_data(self.access_key_2,
                              self.api_key_2,
                              start_date, end_date)

        df1 = self.process_data(df1)
        df2 = self.process_data(df2)

        df = pd.concat([df1, df2], ignore_index=True)
        self.save_to_database(df)

        print("Data fetching and saving completed.")